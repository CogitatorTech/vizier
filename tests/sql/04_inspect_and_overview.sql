-- Test: Schema inspection
load 'zig-out/lib/vizier.duckdb_extension';

-- Create sample tables
create table orders (id integer, customer_id integer, amount double, order_date date);
insert into orders select i, i % 100, i * 1.5, '2026-01-01'::date + (i % 365)::integer from range(5000) t(i);

create table customers (id integer, name varchar, region varchar);
insert into customers select i, 'cust_' || i, case when i % 3 = 0 then 'US' when i % 3 = 1 then 'EU' else 'APAC' end from range(100) t(i);

-- Capture some queries to populate predicates
select * from vizier_capture('select * from orders where customer_id = 42');
select * from vizier_capture('select * from orders where customer_id = 7 and order_date >= date ''2026-06-01''');
select * from vizier_flush();

-- Inspect table: should show columns, storage info, predicate counts
select column_name, column_type, approx_rows, has_nulls, compression, row_groups, predicate_count, predicate_kinds
from vizier.inspect_table('orders');

-- Overview: should show both tables
select * from vizier.overview();
