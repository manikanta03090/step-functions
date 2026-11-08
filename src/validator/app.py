"""SQS-triggered validator.

For every S3 "object created" event it:
  1. checks the object key / size,
  2. reads only the CSV header (ranged GET) and compares it to expected_schema.json
     -> wrong file, missing column or NEW column => email alert + move to rejected/
  3. claims the file in a DynamoDB control table (idempotency, survives SQS redelivery)
  4. starts the Step Functions execution that runs Glue and loads PostgreSQL.
"""
import csv
import hashlib
import io
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from urllib.parse import unquote_plus

import boto3
from botocore.exceptions import ClientError

log = logging.getLogger()
log.setLevel(logging.INFO)

s3 = boto3.client("s3")
sns = boto3.client("sns")
sfn = boto3.client("stepfunctions")
table = boto3.resource("dynamodb").Table(os.environ.get("CONTROL_TABLE", "file-control"))

STATE_MACHINE_ARN = os.environ.get("STATE_MACHINE_ARN", "")
ALERT_TOPIC_ARN = os.environ.get("ALERT_TOPIC_ARN", "")
REJECTED_PREFIX = os.environ.get("REJECTED_PREFIX", "rejected/")
FILE_PATTERN = re.compile(os.environ.get("FILE_PATTERN", r"^incoming/orders_[A-Za-z0-9_.-]+\.csv$"))

with open(os.path.join(os.path.dirname(__file__), "expected_schema.json")) as fh:
    EXPECTED_COLUMNS = json.load(fh)["columns"]


# ----------------------------------------------------------------- checks
def check_name(key, size):
    problems = []
    if not FILE_PATTERN.match(key):
        problems.append(f"Unexpected file name '{key}'. Expected pattern: {FILE_PATTERN.pattern}")
    if size == 0:
        problems.append("File is empty (0 bytes)")
    return problems


def check_schema(header):
    """Return a list of problems; empty list means the header is acceptable.

    Column order is not enforced (Glue reads by name); missing, new,
    blank and duplicate columns are all rejected.
    """
    cleaned = [h.strip().lower() for h in header]
    if not cleaned or all(h == "" for h in cleaned):
        return ["Header row is empty or missing"]

    problems = []
    blanks = cleaned.count("")
    if blanks:
        problems.append(f"{blanks} column(s) with a blank name")
    dupes = sorted({h for h in cleaned if h and cleaned.count(h) > 1})
    if dupes:
        problems.append(f"Duplicate column name(s): {dupes}")

    expected = set(EXPECTED_COLUMNS)
    actual = set(cleaned)
    new_cols = [h for h in dict.fromkeys(cleaned) if h and h not in expected]
    missing = [c for c in EXPECTED_COLUMNS if c not in actual]
    if new_cols:
        problems.append(f"NEW column(s) added to the CSV: {new_cols}")
    if missing:
        problems.append(f"MISSING expected column(s): {missing}")
    return problems


def read_header(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key, Range="bytes=0-65535")
    text = obj["Body"].read().decode("utf-8-sig", errors="replace")
    return next(csv.reader(io.StringIO(text)), [])


# ---------------------------------------------------------------- helpers
def s3_records(body):
    payload = json.loads(body)
    if "Records" not in payload and "Message" in payload:  # SNS envelope (raw delivery off)
        payload = json.loads(payload["Message"])
    return payload.get("Records", [])  # s3:TestEvent has no Records


def move(bucket, key, new_key):
    s3.copy_object(Bucket=bucket, Key=new_key, CopySource={"Bucket": bucket, "Key": key})
    s3.delete_object(Bucket=bucket, Key=key)


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def reject(bucket, key, file_id, problems):
    filename = key.rsplit("/", 1)[-1]
    drift = any("new column" in p.lower() for p in problems)
    subject = f"Schema change detected: {filename}" if drift else f"File rejected: {filename}"
    lines = [f"File: s3://{bucket}/{key}", "", "Problems found:"]
    lines += [f"  - {p}" for p in problems]
    lines += ["", f"The file was moved to s3://{bucket}/{REJECTED_PREFIX}{filename} and was NOT loaded."]
    # alert first: if a later step fails, SQS redelivery re-sends the alert instead of losing it
    sns.publish(TopicArn=ALERT_TOPIC_ARN, Subject=subject[:100], Message="\n".join(lines))
    table.put_item(Item={"file_id": file_id, "status": "REJECTED",
                         "reason": "; ".join(problems)[:1000], "updated_at": now_iso()})
    move(bucket, key, f"{REJECTED_PREFIX}{filename}")
    log.warning("Rejected %s: %s", key, problems)


def claim(file_id, bucket, key):
    """Conditional write: True if this file may be processed now."""
    try:
        table.put_item(
            Item={"file_id": file_id, "bucket": bucket, "key": key,
                  "status": "RECEIVED", "updated_at": now_iso()},
            ConditionExpression="attribute_not_exists(file_id) OR #s IN (:f, :r)",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":f": "FAILED", ":r": "REJECTED"},
        )
        return True
    except ClientError as err:
        if err.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


def start_pipeline(bucket, key, file_id, etag):
    name = f"{hashlib.sha256(file_id.encode()).hexdigest()[:16]}-{int(time.time())}"
    payload = {"bucket": bucket, "key": key, "filename": key.rsplit("/", 1)[-1],
               "file_id": file_id, "etag": etag}
    try:
        sfn.start_execution(stateMachineArn=STATE_MACHINE_ARN, name=name, input=json.dumps(payload))
    except Exception:
        # release the claim so the SQS retry is allowed to start the execution
        table.update_item(Key={"file_id": file_id},
                          UpdateExpression="SET #s = :f",
                          ExpressionAttributeNames={"#s": "status"},
                          ExpressionAttributeValues={":f": "FAILED"})
        raise
    log.info("Started execution %s for %s", name, key)


# ------------------------------------------------------------ main flow
def process_object(rec):
    if not rec.get("eventName", "").startswith("ObjectCreated"):
        return
    bucket = rec["s3"]["bucket"]["name"]
    obj = rec["s3"]["object"]
    key = unquote_plus(obj["key"])
    size = obj.get("size", 0)
    etag = obj.get("eTag", "")
    file_id = f"{bucket}/{key}#{etag}"

    problems = check_name(key, size)
    if not problems:
        try:
            header = read_header(bucket, key)
        except ClientError as err:
            if err.response["Error"]["Code"] in ("NoSuchKey", "404"):
                log.info("%s no longer exists (already handled) - skipping", key)
                return
            raise
        problems = check_schema(header)

    if problems:
        try:
            reject(bucket, key, file_id, problems)
        except ClientError as err:
            if err.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return
            raise
        return

    if not claim(file_id, bucket, key):
        log.info("%s (etag %s) already processed or in progress - skipping", key, etag)
        return
    start_pipeline(bucket, key, file_id, etag)


def handler(event, context):
    failures = []
    for message in event["Records"]:
        try:
            for rec in s3_records(message["body"]):
                process_object(rec)
        except Exception:
            log.exception("Failed to process message %s", message["messageId"])
            failures.append({"itemIdentifier": message["messageId"]})
    return {"batchItemFailures": failures}
