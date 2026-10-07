-- @bind_point_get_reuse ordered
SELECT id, name FROM emp WHERE id = $1 \bind 7 \g
SELECT id, name FROM emp WHERE id = $1 \bind 8 \g
SELECT id, name FROM emp WHERE id = $1 \bind 9999 \g
SELECT id, name FROM emp WHERE id = $1 \bind 7 \g
-- @bind_index_reuse
SELECT id FROM emp WHERE dept_id = $1 \bind 3 \g
SELECT id FROM emp WHERE dept_id = $1 \bind 4 \g
SELECT id FROM emp WHERE dept_id = $1 \bind 3 \g
-- @bind_null_param
SELECT id FROM emp WHERE dept_id = $1 \bind \g
SELECT count(*) FROM emp WHERE $1::int IS NULL \bind \g
-- @bind_text_quote
SELECT $1::text, length($1::text) \bind "it's" \g
SELECT id FROM emp WHERE name = $1 \bind bob \g
SELECT id FROM emp WHERE name = $1 \bind "o'x" \g
-- @bind_range_reuse ordered
SELECT id FROM emp WHERE id > $1 AND id < $2 ORDER BY id \bind 10 15 \g
SELECT id FROM emp WHERE id > $1 AND id < $2 ORDER BY id \bind 70 100 \g
-- @bind_limit ordered
SELECT id FROM emp ORDER BY id LIMIT $1 \bind 3 \g
SELECT id FROM emp ORDER BY id LIMIT $1 \bind 5 \g
-- @bind_join_reuse
SELECT e.id, d.name FROM emp e JOIN dept d ON d.id = e.dept_id WHERE e.dept_id = $1 \bind 2 \g
SELECT e.id, d.name FROM emp e JOIN dept d ON d.id = e.dept_id WHERE e.dept_id = $1 \bind 5 \g
-- @bind_join_second_table_param
SELECT e.id, a.proj_id FROM emp e JOIN assign a ON a.emp_id = e.id WHERE a.hours = $1 AND e.dept_id = $2 \bind 40 3 \g
SELECT e.id, a.proj_id FROM emp e JOIN assign a ON a.emp_id = e.id WHERE a.hours = $1 AND e.dept_id = $2 \bind 10 9 \g
-- @bind_or_reuse
SELECT id FROM emp WHERE id = $1 OR dept_id = $2 \bind 1 3 \g
SELECT id FROM emp WHERE id = $1 OR dept_id = $2 \bind 2 4 \g
-- @bind_in_reuse
SELECT id FROM emp WHERE id IN ($1, $2) \bind 1 2 \g
SELECT id FROM emp WHERE id IN ($1, $2) \bind 3 3 \g
-- @bind_subquery_reuse
SELECT d.id FROM dept d WHERE EXISTS (SELECT 1 FROM emp e WHERE e.dept_id = d.id AND e.salary > $1) \bind 3000 \g
SELECT d.id FROM dept d WHERE EXISTS (SELECT 1 FROM emp e WHERE e.dept_id = d.id AND e.salary > $1) \bind 100 \g
-- @bind_agg_reuse
SELECT dept_id, count(*) FROM emp WHERE salary > $1 GROUP BY dept_id \bind 0 \g
SELECT dept_id, count(*) FROM emp WHERE salary > $1 GROUP BY dept_id \bind 2000 \g
-- @bind_date_ts
SELECT id FROM emp WHERE hired > $1 \bind 2023-06-01 \g
SELECT proj_id FROM proj WHERE started < $1 \bind "2023-05-01 00:00:00" \g
-- @bind_bool_double
SELECT id FROM emp WHERE active = $1 AND bonus > $2 \bind true 50.5 \g
SELECT id FROM emp WHERE active = $1 AND bonus > $2 \bind f -1 \g
-- @bind_dml_reuse
BEGIN;
UPDATE emp SET salary = $1 WHERE id = $2 \bind 111 1 \g
UPDATE emp SET salary = $1 WHERE id = $2 \bind 222 2 \g
INSERT INTO dept VALUES ($1, $2, NULL, $3, NULL) \bind 700 "a'b" 1.25 \g
INSERT INTO dept VALUES ($1, $2, NULL, $3, NULL) \bind 701 \N 2 \g
COMMIT;
SELECT id, salary FROM emp WHERE id IN (1, 2);
SELECT * FROM dept WHERE id >= 700;
-- @bind_dml_scan_reuse
UPDATE emp SET note = $1 WHERE dept_id = $2 \bind A 3 \g
UPDATE emp SET note = $1 WHERE dept_id = $2 \bind B 4 \g
SELECT id, note FROM emp WHERE dept_id IN (3, 4);
-- @bind_delete_reuse
DELETE FROM assign WHERE emp_id = $1 \bind 4 \g
DELETE FROM assign WHERE emp_id = $1 \bind 5 \g
SELECT count(*) FROM assign;
