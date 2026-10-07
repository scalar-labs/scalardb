\set region random(0, 999)
SELECT id, name FROM customers WHERE region = :region;
