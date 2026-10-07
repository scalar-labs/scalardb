-- Benchmark schema for ScalarDB through the PostgreSQL frontend.
-- Run with psql connected to the frontend with database "bench" (the default namespace):
--   psql -h HOST -p PORT -d bench -f schema/scalardb.sql
-- The first PRIMARY KEY column is the partition key, the rest are clustering keys.
CREATE COORDINATOR TABLES IF NOT EXISTS;
CREATE SCHEMA IF NOT EXISTS bench;
CREATE TABLE customers (id INT, name TEXT, region INT, tier INT, balance DOUBLE PRECISION, PRIMARY KEY (id));
CREATE INDEX customers_region_idx ON customers (region);
CREATE TABLE items (id INT, code BIGINT, name TEXT, category TEXT, price DOUBLE PRECISION, PRIMARY KEY (id));
CREATE INDEX items_code_idx ON items (code);
CREATE INDEX items_category_idx ON items (category);
CREATE TABLE orders (customer_id INT, order_id INT, item_id INT, amount DOUBLE PRECISION, status TEXT, created BIGINT, PRIMARY KEY (customer_id, order_id));
CREATE TABLE counters (id INT, hits INT, PRIMARY KEY (id));
CREATE TABLE kv (id INT, v INT, PRIMARY KEY (id));
