\set id random(1, :ncust)
\set amt random(1, 400)
SELECT name FROM customers c WHERE c.id = :id AND EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.amount > :amt);
