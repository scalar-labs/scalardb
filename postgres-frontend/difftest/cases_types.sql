-- @types_all
SET TIME ZONE 'UTC';
SELECT * FROM types;
-- @types_time_cmp
SELECT id FROM types WHERE t >= '09:30:15';
-- @types_time_order ordered
SELECT id, t FROM types ORDER BY t NULLS LAST;
-- @types_time_funcs
SELECT id, extract(hour from t) AS h, t::text AS s FROM types;
-- @types_tz_cmp
SET TIME ZONE 'UTC';
SELECT id, tz FROM types WHERE tz > '2024-06-01 00:00:00+00';
-- @types_tz_order ordered
SET TIME ZONE 'UTC';
SELECT id FROM types ORDER BY tz NULLS LAST;
-- @types_tz_text
SET TIME ZONE 'UTC';
SELECT id, tz::text AS s, extract(year from tz) AS y FROM types;
-- @types_bytea
SELECT id, b, length(b) AS n FROM types;
-- @types_bytea_cmp
SELECT id FROM types WHERE b = '\xdeadbeef';
-- @types_real
SELECT id, r, r * 2 AS d, r::double precision AS dp FROM types;
-- @types_real_cmp
SELECT id FROM types WHERE r > 1;
-- @types_insert
BEGIN;
INSERT INTO types VALUES (10, '01:02:03', '2024-02-29 10:00:00+02', '\x0a0b', 2.5);
SET TIME ZONE 'UTC';
SELECT * FROM types WHERE id = 10;
ROLLBACK;
-- @types_update
BEGIN;
UPDATE types SET t = '05:05:05', tz = '2025-01-01 00:00:00+00', b = '\xff', r = 7.5 WHERE id = 1;
SET TIME ZONE 'UTC';
SELECT t, tz, b, r FROM types WHERE id = 1;
ROLLBACK;
-- @types_bind
SET TIME ZONE 'UTC';
SELECT id FROM types WHERE t = $1 \bind 09:30:15 \g
SELECT id FROM types WHERE tz = $1 \bind '2024-06-15 03:00:00+00' \g
SELECT id FROM types WHERE b = $1 \bind '\\xdeadbeef' \g
SELECT id FROM types WHERE r = $1 \bind 1.25 \g
