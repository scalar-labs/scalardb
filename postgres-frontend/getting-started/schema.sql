-- Schema for the getting-started guide: ScalarDB through the PostgreSQL frontend. Run it with psql connected to the
-- frontend with database "demo" (the database name is the default ScalarDB namespace):
--   psql -h localhost -p 15432 -d demo -f schema.sql
-- The first PRIMARY KEY column is the partition key; the others are clustering keys.
CREATE COORDINATOR TABLES IF NOT EXISTS;
CREATE SCHEMA IF NOT EXISTS demo;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS items;
DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
  id      INT,
  name    TEXT,
  region  TEXT,
  tier    INT,
  balance DOUBLE PRECISION,
  PRIMARY KEY (id)
);
CREATE INDEX customers_region_idx ON customers (region);

CREATE TABLE items (
  id       INT,
  name     TEXT,
  category TEXT,
  price    DOUBLE PRECISION,
  PRIMARY KEY (id)
);
CREATE INDEX items_category_idx ON items (category);

-- one partition per customer, orders ordered by order_id inside it
CREATE TABLE orders (
  customer_id INT,
  order_id    INT,
  item_id     INT,
  quantity    INT,
  amount      DOUBLE PRECISION,
  status      TEXT,
  ordered_at  TIMESTAMP,
  PRIMARY KEY (customer_id, order_id)
);
