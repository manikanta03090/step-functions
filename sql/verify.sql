-- Run from a VPC-attached client (e.g. CloudShell VPC environment) after each test
SELECT count(*)                       AS total_rows,
       count(DISTINCT order_id)       AS distinct_orders      -- must be equal: no duplicates
FROM public.orders;

SELECT order_id, order_status, updated_at, source_file
FROM public.orders
ORDER BY order_id;
