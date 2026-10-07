\set cid random(1, :ncust)
\set oid random(1, :norders)
SELECT o.order_id, o.amount, c.name, c.tier FROM orders o JOIN customers c ON c.id = o.customer_id WHERE o.customer_id = :cid AND o.order_id = :oid;
