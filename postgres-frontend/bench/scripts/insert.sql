\set cid random(2, :ncust)
\set oid random(1000000, 2000000000)
\set item random(1, :nitems)
\set amt random(1, 500)
INSERT INTO orders (customer_id, order_id, item_id, amount, status, created) VALUES (:cid, :oid, :item, :amt, 'new', 1700000000);
