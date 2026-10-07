\set cid random(1, :ncust)
SELECT o.order_id, i.name, o.amount FROM orders o JOIN items i ON i.id = o.item_id WHERE o.customer_id = :cid;
