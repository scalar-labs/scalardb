\set id random(2, :ncust)
\set tier random(0, 4)
\set oid random(1000000, 2000000000)
\set item random(1, :nitems)
BEGIN;
SELECT name, tier FROM customers WHERE id = :id;
UPDATE customers SET tier = :tier WHERE id = :id;
INSERT INTO orders (customer_id, order_id, item_id, amount, status, created) VALUES (:id, :oid, :item, 10, 'new', 1700000000);
COMMIT;
