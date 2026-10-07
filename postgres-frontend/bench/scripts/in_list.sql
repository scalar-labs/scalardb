\set a random(1, :ncust)
\set b random(1, :ncust)
\set c random(1, :ncust)
\set d random(1, :ncust)
\set e random(1, :ncust)
SELECT id, name FROM customers WHERE id IN (:a, :b, :c, :d, :e);
