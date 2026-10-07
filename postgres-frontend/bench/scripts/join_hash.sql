\set cid random(1, :ncust)
SELECT o.order_id, i.name FROM orders o JOIN (SELECT id, name FROM items WHERE category = 'cat3') i ON i.id = o.item_id WHERE o.customer_id = :cid;
