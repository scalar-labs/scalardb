\set cid random(1, :ncust)
WITH recent AS (SELECT order_id, amount FROM orders WHERE customer_id = :cid AND order_id > 5) SELECT COUNT(*) AS n, SUM(amount) AS total FROM recent;
