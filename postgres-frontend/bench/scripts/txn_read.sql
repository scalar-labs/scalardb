\set id random(1, :ncust)
BEGIN;
SELECT name, tier FROM customers WHERE id = :id;
SELECT order_id, amount FROM orders WHERE customer_id = :id;
COMMIT;
