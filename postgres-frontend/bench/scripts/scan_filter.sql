\set p random(1, 900)
SELECT COUNT(*) FROM items WHERE price > :p AND category = 'cat7';
