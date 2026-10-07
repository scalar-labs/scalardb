\set i random(1, :nitems)
\set code 1000000 + 7 * :i
SELECT id, name, price FROM items WHERE code = :code;
