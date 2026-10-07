\set a random(1, :ncust)
\set b random(1, :ncust)
SELECT item_id FROM orders WHERE customer_id = :a UNION SELECT item_id FROM orders WHERE customer_id = :b;
