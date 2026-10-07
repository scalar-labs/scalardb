-- @bind_ts
SELECT proj_id FROM proj WHERE started < $1 \bind '2023-05-01 00:00:00' \g
-- @bind_dml_reuse
BEGIN;
UPDATE emp SET salary = $1 WHERE id = $2 \bind 111 1 \g
UPDATE emp SET salary = $1 WHERE id = $2 \bind 222 2 \g
INSERT INTO dept VALUES ($1, $2, NULL, $3, NULL) \bind 700 'a''b' 1.25 \g
INSERT INTO dept VALUES ($1, $2, NULL, $3, NULL) \bind 701 x 2 \g
COMMIT;
SELECT id, salary FROM emp WHERE id IN (1, 2);
SELECT * FROM dept WHERE id >= 700;
