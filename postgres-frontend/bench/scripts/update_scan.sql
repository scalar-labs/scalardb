\set cid random(1, :ncust)
UPDATE orders SET status = CASE WHEN status = 'new' THEN 'shipped' ELSE 'new' END WHERE customer_id = :cid;
