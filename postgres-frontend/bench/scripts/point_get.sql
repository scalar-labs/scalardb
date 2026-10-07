\set id random(1, :ncust)
SELECT name, region, tier FROM customers WHERE id = :id;
