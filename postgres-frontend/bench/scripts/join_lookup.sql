\set id random(1, :ncust)
SELECT c.name, o.order_id, o.amount FROM customers c JOIN orders o ON o.customer_id = c.id WHERE c.id = :id;
