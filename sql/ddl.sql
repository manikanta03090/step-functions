-- Reference copy of the table created by the Glue job (keep in sync with glue/csv_to_postgres.py)
CREATE TABLE IF NOT EXISTS public.orders (
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
);
