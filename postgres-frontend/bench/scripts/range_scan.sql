\set cid random(1, :ncust)
\set from random(1, :norders)
SELECT order_id, amount FROM orders WHERE customer_id = :cid AND order_id >= :from ORDER BY order_id LIMIT 5;
