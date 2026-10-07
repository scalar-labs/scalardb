-- Benchmark schema for native PostgreSQL, with the same access paths as the ScalarDB layout:
-- the primary-key index covers the partition key plus clustering keys, and the same secondary
-- indexes exist. Run with psql connected to an empty database:
--   psql -h HOST -p PORT -d bench -f schema/postgres.sql
CREATE TABLE customers (id INT PRIMARY KEY, name TEXT, region INT, tier INT, balance DOUBLE PRECISION);
CREATE INDEX customers_region_idx ON customers (region);
CREATE TABLE items (id INT PRIMARY KEY, code BIGINT, name TEXT, category TEXT, price DOUBLE PRECISION);
CREATE INDEX items_code_idx ON items (code);
CREATE INDEX items_category_idx ON items (category);
CREATE TABLE orders (customer_id INT, order_id INT, item_id INT, amount DOUBLE PRECISION, status TEXT, created BIGINT, PRIMARY KEY (customer_id, order_id));
CREATE TABLE counters (id INT PRIMARY KEY, hits INT);
CREATE TABLE kv (id INT PRIMARY KEY, v INT);
