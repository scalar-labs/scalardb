\set cid random(1, :ncust)
SELECT order_id, amount FROM orders WHERE customer_id = :cid ORDER BY amount DESC LIMIT 3;
