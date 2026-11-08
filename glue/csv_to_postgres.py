"""Glue (PySpark) job: CSV in S3 -> typed/validated rows -> per-run staging table
-> idempotent upsert into the target table in PostgreSQL.

Duplicate protection:
  * duplicates inside the file are collapsed (latest updated_at wins)
  * PRIMARY KEY (order_id) + INSERT ... ON CONFLICT DO UPDATE in the database
  * an older row never overwrites a newer one (updated_at guard)
"""
import json
import re
import sys
from functools import reduce

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "SOURCE_S3_PATH", "RUN_ID", "REJECT_S3_PATH", "DB_SECRET_ARN", "TARGET_TABLE"],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------------ config
# Keep in sync with src/validator/expected_schema.json and sql/ddl.sql.
SCHEMA = [
    ("order_id", "string"),
    ("customer_id", "string"),
    ("customer_name", "string"),
    ("email", "string"),
    ("product_id", "string"),
    ("quantity", "int"),
    ("unit_price", "decimal(12,2)"),
    ("order_status", "string"),
    ("order_date", "date"),
    ("updated_at", "timestamp"),
]
KEY = ["order_id"]
REQUIRED = ["order_id", "customer_id", "product_id", "quantity", "unit_price", "order_date", "updated_at"]

SOURCE = args["SOURCE_S3_PATH"]
TARGET = args["TARGET_TABLE"]
if not re.fullmatch(r"[a-z_][a-z0-9_]*", TARGET):
    raise ValueError(f"Illegal target table name: {TARGET}")
STAGING = f"stg_{TARGET}_{re.sub(r'[^a-z0-9_]', '_', args['RUN_ID'].lower())}"[:63]

DDL = f"""
CREATE TABLE IF NOT EXISTS public.{TARGET} (
    order_id       VARCHAR(50)   PRIMARY KEY,
    customer_id    VARCHAR(50)   NOT NULL,
    customer_name  VARCHAR(200),
    email          VARCHAR(200),
    product_id     VARCHAR(50)   NOT NULL,
    quantity       INTEGER       NOT NULL,
    unit_price     NUMERIC(12,2) NOT NULL,
    order_status   VARCHAR(30),
    order_date     DATE          NOT NULL,
    updated_at     TIMESTAMP     NOT NULL,
    source_file    VARCHAR(500),
    loaded_at      TIMESTAMP     NOT NULL DEFAULT now()
)"""

# ------------------------------------------------------------- DB plumbing
secret = json.loads(boto3.client("secretsmanager").get_secret_value(SecretId=args["DB_SECRET_ARN"])["SecretString"])
JDBC_URL = f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}"


def run_sql(statement):
    """Execute one statement over a plain JDBC connection (autocommit)."""
    jvm = sc._gateway.jvm
    jvm.java.lang.Class.forName("org.postgresql.Driver")
    conn = jvm.java.sql.DriverManager.getConnection(JDBC_URL, secret["username"], secret["password"])
    try:
        stmt = conn.createStatement()
        try:
            return stmt.executeUpdate(statement)
        finally:
            stmt.close()
    finally:
        conn.close()


def jdbc_reader():
    return (spark.read.format("jdbc")
            .option("url", JDBC_URL)
            .option("user", secret["username"])
            .option("password", secret["password"])
            .option("driver", "org.postgresql.Driver"))


# ---------------------------------------------------------------- extract
raw = (spark.read
       .option("header", "true")
       .option("quote", '"')
       .option("escape", '"')
       .csv(SOURCE))
raw = raw.toDF(*[c.strip().lower() for c in raw.columns])

expected_cols = [c for c, _ in SCHEMA]
if set(raw.columns) != set(expected_cols):
    raise ValueError(
        f"Schema mismatch. missing={sorted(set(expected_cols) - set(raw.columns))} "
        f"unexpected={sorted(set(raw.columns) - set(expected_cols))}"
    )


# --------------------------------------------------------------- transform
def typed_expr(name, dtype):
    col = F.trim(F.col(name))
    col = F.when(col == "", F.lit(None)).otherwise(col)
    if dtype == "date":
        return F.to_date(col, "yyyy-MM-dd")
    if dtype == "timestamp":
        return F.to_timestamp(col)
    return col.cast(dtype)


staged = raw.select("*", *[typed_expr(n, t).alias(f"t_{n}") for n, t in SCHEMA]).cache()
is_valid = reduce(lambda a, b: a & b, [F.col(f"t_{c}").isNotNull() for c in REQUIRED])

good = staged.filter(is_valid).select(*[F.col(f"t_{n}").alias(n) for n, _ in SCHEMA])
bad = staged.filter(~is_valid).select(*expected_cols)

bad_count = bad.count()
if bad_count:
    print(f"DQ: {bad_count} row(s) failed required/type checks -> {args['REJECT_S3_PATH']}")
    bad.coalesce(1).write.mode("overwrite").option("header", "true").csv(args["REJECT_S3_PATH"])

latest_first = Window.partitionBy(*KEY).orderBy(F.col("updated_at").desc())
deduped = (good.withColumn("_rn", F.row_number().over(latest_first))
           .filter("_rn = 1").drop("_rn")
           .withColumn("source_file", F.lit(SOURCE))
           .cache())

good_count = good.count()
final_count = deduped.count()
print(f"Rows read={good_count + bad_count} valid={good_count} "
      f"in-file duplicates dropped={good_count - final_count} to load={final_count}")

# -------------------------------------------------------------------- load
run_sql(DDL)
run_sql(f"DROP TABLE IF EXISTS public.{STAGING}")
run_sql(f"CREATE TABLE public.{STAGING} (LIKE public.{TARGET} INCLUDING DEFAULTS)")

cols = expected_cols + ["source_file"]
col_list = ", ".join(cols)
set_list = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in KEY)
MERGE = f"""
INSERT INTO public.{TARGET} ({col_list})
SELECT {col_list} FROM public.{STAGING}
ON CONFLICT ({", ".join(KEY)}) DO UPDATE
   SET {set_list}, loaded_at = now()
 WHERE {TARGET}.updated_at <= EXCLUDED.updated_at
"""

try:
    (deduped.write.format("jdbc")
     .option("url", JDBC_URL)
     .option("dbtable", f"public.{STAGING}")
     .option("user", secret["username"])
     .option("password", secret["password"])
     .option("driver", "org.postgresql.Driver")
     .option("batchsize", 5000)
     .mode("append")
     .save())
    affected = run_sql(MERGE)
    total = jdbc_reader().option("query", f"SELECT count(*) AS c FROM public.{TARGET}").load().collect()[0]["c"]
    print(f"Upsert complete: {affected} row(s) inserted/updated; {TARGET} now holds {total} row(s)")
finally:
    run_sql(f"DROP TABLE IF EXISTS public.{STAGING}")

job.commit()
