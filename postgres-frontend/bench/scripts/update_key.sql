\set id random(1, :ncust)
\set tier random(0, 4)
UPDATE customers SET tier = :tier WHERE id = :id;
