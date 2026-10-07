-- A tour of the frontend with psql: paste the statements one at a time (or run the file
-- with psql -f tour.sql). Load schema.sql and data.sql first. Customer 6509 has seven orders.
\timing on
\pset pager off

\echo
\echo '=== 1. The catalog comes from ScalarDB metadata: psql meta-commands work'
\dt
\d orders
\di

\echo
\echo '=== 2. Point read by primary key: one ScalarDB Get'
SELECT * FROM customers WHERE id = 6509;
EXPLAIN SELECT * FROM customers WHERE id = 6509;

\echo
\echo '=== 3. One partition, ordered by the clustering key, newest first: range, order and limit pushed down'
SELECT order_id, item_id, quantity, amount, status, ordered_at FROM orders WHERE customer_id = 6509 ORDER BY order_id DESC LIMIT 3;
EXPLAIN SELECT order_id, amount FROM orders WHERE customer_id = 6509 AND order_id >= 2 ORDER BY order_id DESC LIMIT 3;

\echo
\echo '=== 4. Secondary index, extra conditions pushed to ScalarDB, sort in the frontend'
SELECT id, name, tier, balance FROM customers WHERE region = 'Kyoto' AND tier = 3 ORDER BY balance DESC LIMIT 5;
EXPLAIN SELECT id, name, tier, balance FROM customers WHERE region = 'Kyoto' AND tier = 3 ORDER BY balance DESC LIMIT 5;

\echo
\echo '=== 5. No key at all: a cross-partition scan with the conditions pushed down'
SELECT id, name, category, price FROM items WHERE price >= 199.5 AND price <= 200 ORDER BY price;
EXPLAIN SELECT id, name, category, price FROM items WHERE price >= 199.5 AND price <= 200 ORDER BY price;

\echo
\echo '=== 6. IN on the key becomes lookups; OR on one table is pushed as a disjunction'
SELECT id, name, region FROM customers WHERE id IN (1, 2, 3);
EXPLAIN SELECT id, name, region FROM customers WHERE id IN (1, 2, 3);
SELECT id, name, price FROM items WHERE category = 'books' AND (price < 1.5 OR price > 199.5) ORDER BY price;
EXPLAIN SELECT id, name, price FROM items WHERE category = 'books' AND (price < 1.5 OR price > 199.5) ORDER BY price;

\echo
\echo '=== 7. Joins: a lookup join by key, and a LEFT JOIN with an aggregate'
SELECT o.order_id, i.name, o.quantity, o.amount FROM orders o JOIN items i ON i.id = o.item_id WHERE o.customer_id = 6509 ORDER BY o.order_id;
EXPLAIN SELECT o.order_id, i.name FROM orders o JOIN items i ON i.id = o.item_id WHERE o.customer_id = 6509;
SELECT c.id, c.name, count(o.order_id) AS orders, sum(o.amount) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.id
 WHERE c.region = 'Kyoto' AND c.tier = 3 AND c.id < 400
 GROUP BY c.id, c.name ORDER BY orders DESC, c.id LIMIT 5;

\echo
\echo '=== 8. Aggregates, GROUP BY, HAVING, DISTINCT'
SELECT status, count(*) AS orders, sum(amount) AS total, avg(amount) AS average FROM orders WHERE customer_id < 1000 GROUP BY status ORDER BY orders DESC;
SELECT category, count(*) FROM items GROUP BY category HAVING count(*) > 1020 ORDER BY 2 DESC;
SELECT DISTINCT region FROM customers WHERE tier = 3 AND id < 100 ORDER BY region;

\echo
\echo '=== 9. Expressions, CASE, subqueries, CTEs, set operations'
SELECT name, CASE WHEN tier = 3 THEN 'gold' WHEN tier = 2 THEN 'silver' ELSE 'bronze' END AS level, round(balance * 1.1, 2) AS projected FROM customers WHERE id IN (1, 2, 3);
SELECT name, price FROM items WHERE id IN (SELECT item_id FROM orders WHERE customer_id = 6509) ORDER BY price;
SELECT (SELECT count(*) FROM orders WHERE customer_id = 6509) AS orders, (SELECT name FROM customers WHERE id = 6509) AS customer;
WITH recent AS (SELECT * FROM orders WHERE customer_id < 500 AND ordered_at >= '2026-09-01')
SELECT status, count(*) FROM recent GROUP BY status ORDER BY 2 DESC;
SELECT item_id FROM orders WHERE customer_id = 1 UNION SELECT item_id FROM orders WHERE customer_id = 2 ORDER BY 1;

\echo
\echo '=== 10. Writes: INSERT ... RETURNING, UPDATE by key, UPDATE by scan, UPSERT, DELETE'
INSERT INTO customers (id, name, region, tier, balance) VALUES (10001, 'Demo User', 'Tokyo', 1, 0) RETURNING id, name, balance;
INSERT INTO orders (customer_id, order_id, item_id, quantity, amount, status, ordered_at) VALUES
  (10001, 1, 7, 1, 12.5, 'paid', '2026-10-02 10:00:00'),
  (10001, 2, 8, 2, 40.0, 'paid', '2026-10-02 10:05:00'),
  (10001, 3, 9, 1, 9.9, 'new', '2026-10-02 10:10:00');
UPDATE customers SET balance = balance + 100 WHERE id = 10001 RETURNING id, balance;
UPDATE orders SET status = 'shipped' WHERE customer_id = 10001 AND status = 'paid';
SELECT order_id, status FROM orders WHERE customer_id = 10001 ORDER BY order_id;
INSERT INTO customers (id, name, region, tier, balance) VALUES (10001, 'Demo User', 'Tokyo', 2, 250)
  ON CONFLICT (id) DO UPDATE SET tier = EXCLUDED.tier, balance = EXCLUDED.balance;
SELECT id, name, tier, balance FROM customers WHERE id = 10001;
DELETE FROM orders WHERE customer_id = 10001 AND order_id = 3;
SELECT count(*) FROM orders WHERE customer_id = 10001;

\echo
\echo '=== 11. A write that would touch too many rows is refused; SET scalardb.max_rows_per_write raises the cap'
UPDATE orders SET status = 'archived';
-- Raise the cap for this session and rewrite every order (the data stays the same)
SET scalardb.max_rows_per_write = 20000;
UPDATE orders SET status = status;
RESET scalardb.max_rows_per_write;

\echo
\echo '=== 12. Transactions: BEGIN/COMMIT, ROLLBACK, the aborted state, read-only transactions'
BEGIN;
UPDATE customers SET balance = balance - 50 WHERE id = 1;
UPDATE customers SET balance = balance + 50 WHERE id = 2;
SELECT id, name, balance FROM customers WHERE id IN (1, 2);
COMMIT;
BEGIN;
UPDATE customers SET balance = 0 WHERE id = 1;
ROLLBACK;
SELECT id, balance FROM customers WHERE id = 1;
BEGIN;
SELECT * FROM no_such_table;
SELECT 'this is ignored until ROLLBACK' AS note;
ROLLBACK;
BEGIN READ ONLY;
SELECT count(*) FROM orders WHERE customer_id = 6509;
COMMIT;

\echo
\echo '=== 13. EXPLAIN ANALYZE: rows and ScalarDB reads per operator'
EXPLAIN ANALYZE SELECT o.order_id, i.name FROM orders o JOIN items i ON i.id = o.item_id WHERE o.customer_id = 6509;
EXPLAIN ANALYZE SELECT count(*) FROM orders WHERE customer_id < 100;

\echo
\echo '=== 14. Prepared statements with $n parameters (bound with psql''s bind meta-command), session keywords'
SELECT name, balance FROM customers WHERE id = $1 \bind 7 \g
SELECT current_user, current_catalog, current_schema;

\echo
\echo '=== 15. Clean up the rows the tour inserted'
DELETE FROM orders WHERE customer_id = 10001;
DELETE FROM customers WHERE id = 10001;
