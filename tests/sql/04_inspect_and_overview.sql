-- Test: Schema inspection
LOAD 'zig-out/lib/vizier.duckdb_extension';

-- Create sample tables
CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount DOUBLE, order_date DATE);
INSERT INTO orders SELECT i, i % 100, i * 1.5, '2026-01-01'::DATE + (i % 365)::INTEGER FROM range(5000) t(i);

CREATE TABLE customers (id INTEGER, name VARCHAR, region VARCHAR);
INSERT INTO customers SELECT i, 'cust_' || i, CASE WHEN i % 3 = 0 THEN 'US' WHEN i % 3 = 1 THEN 'EU' ELSE 'APAC' END FROM range(100) t(i);

-- Capture some queries to populate predicates
SELECT * FROM vizier_capture('SELECT * FROM orders WHERE customer_id = 42');
SELECT * FROM vizier_capture('SELECT * FROM orders WHERE customer_id = 7 AND order_date >= DATE ''2026-06-01''');
SELECT * FROM vizier_flush();

-- Inspect table: should show columns, storage info, predicate counts
SELECT column_name, column_type, approx_rows, has_nulls, compression, row_groups, predicate_count, predicate_kinds
FROM vizier.inspect_table('orders');

-- Overview: should show both tables
SELECT * FROM vizier.overview();
