\set cid random(1, :ncust)
SELECT status, COUNT(*) AS n, SUM(amount) AS total FROM orders WHERE customer_id = :cid GROUP BY status;
