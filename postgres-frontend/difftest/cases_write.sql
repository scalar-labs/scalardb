-- @update_pk_returning
BEGIN;
UPDATE emp SET salary = salary + 1 WHERE id = 7 RETURNING id, salary;
SELECT id, salary FROM emp WHERE id = 7;
ROLLBACK;
SELECT id, salary FROM emp WHERE id = 7;
-- @update_scan
BEGIN;
UPDATE emp SET bonus = coalesce(bonus, 0) * 2, note = 'raised' WHERE dept_id = 3 AND salary >= 1000;
SELECT id, bonus, note FROM emp WHERE dept_id = 3;
ROLLBACK;
-- @update_null_where
BEGIN;
UPDATE emp SET note = 'nodept' WHERE dept_id IS NULL;
SELECT id, note FROM emp WHERE note = 'nodept';
ROLLBACK;
-- @update_set_null
BEGIN;
UPDATE emp SET salary = NULL WHERE id IN (1, 2, 3) RETURNING *;
ROLLBACK;
-- @update_self_ref_swap
BEGIN;
UPDATE emp SET salary = mgr_id, mgr_id = salary WHERE id = 10;
SELECT id, salary, mgr_id FROM emp WHERE id = 10;
ROLLBACK;
-- @update_subquery
BEGIN;
UPDATE emp SET salary = (SELECT max(salary) FROM emp) WHERE id = 11;
SELECT id, salary FROM emp WHERE id = 11;
ROLLBACK;
-- @update_in_subquery
BEGIN;
UPDATE emp SET active = false WHERE dept_id IN (SELECT id FROM dept WHERE region = 'east');
SELECT count(*) FROM emp WHERE active;
ROLLBACK;
-- @update_no_match
UPDATE emp SET salary = 1 WHERE id = 9999;
-- @delete_scan
BEGIN;
DELETE FROM assign WHERE hours IS NULL RETURNING emp_id, proj_id;
SELECT count(*) FROM assign;
ROLLBACK;
-- @delete_not_exists
BEGIN;
DELETE FROM assign a WHERE NOT EXISTS (SELECT 1 FROM emp e WHERE e.id = a.emp_id);
SELECT count(*) FROM assign;
ROLLBACK;
-- @delete_pk
BEGIN;
DELETE FROM emp WHERE id = 80;
DELETE FROM emp WHERE id = 80;
SELECT count(*) FROM emp;
ROLLBACK;
-- @insert_multi_returning
BEGIN;
INSERT INTO dept VALUES (100, 'new', 'west', 1.5, NULL), (101, 'n2', NULL, NULL, 100) RETURNING id, name;
SELECT * FROM dept WHERE id >= 100;
ROLLBACK;
-- @insert_columns_default_null
BEGIN;
INSERT INTO emp (id, name) VALUES (500, 'x');
SELECT * FROM emp WHERE id = 500;
ROLLBACK;
-- @insert_duplicate
INSERT INTO dept VALUES (1, 'dup', NULL, NULL, NULL);
-- @insert_duplicate_in_tx
BEGIN;
INSERT INTO dept VALUES (200, 'a', NULL, NULL, NULL);
INSERT INTO dept VALUES (200, 'b', NULL, NULL, NULL);
COMMIT;
SELECT * FROM dept WHERE id = 200;
-- @insert_select
BEGIN;
INSERT INTO dept (id, name, budget) SELECT id + 1000, name, salary FROM emp WHERE salary > 3000;
SELECT id, name, budget FROM dept WHERE id > 1000;
ROLLBACK;
-- @insert_on_conflict_update
BEGIN;
INSERT INTO dept VALUES (1, 'up', 'x', 9.0, NULL) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, region = EXCLUDED.region, budget = EXCLUDED.budget, parent_id = EXCLUDED.parent_id;
INSERT INTO dept VALUES (300, 'ins', 'x', 9.0, NULL) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, region = EXCLUDED.region, budget = EXCLUDED.budget, parent_id = EXCLUDED.parent_id;
SELECT * FROM dept WHERE id IN (1, 300);
ROLLBACK;
-- @insert_on_conflict_expr
BEGIN;
INSERT INTO dept VALUES (2, 'zz', NULL, 1.0, NULL) ON CONFLICT (id) DO UPDATE SET budget = dept.budget + EXCLUDED.budget RETURNING id, budget;
ROLLBACK;
-- @insert_on_conflict_nothing
BEGIN;
INSERT INTO dept VALUES (3, 'zz', NULL, 1.0, NULL), (301, 'new', NULL, 1.0, NULL) ON CONFLICT (id) DO NOTHING;
SELECT id, name FROM dept WHERE id IN (3, 301);
ROLLBACK;
-- @insert_on_conflict_where
BEGIN;
INSERT INTO dept VALUES (4, 'zz', NULL, 1.0, NULL) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE dept.region = 'nowhere';
SELECT id, name FROM dept WHERE id = 4;
ROLLBACK;
-- @insert_quote
BEGIN;
INSERT INTO dept VALUES (400, 'it''s', NULL, NULL, NULL);
SELECT name, length(name) FROM dept WHERE id = 400;
SELECT id FROM dept WHERE name = 'it''s';
ROLLBACK;
-- @insert_type_mismatch
INSERT INTO dept VALUES ('abc', 'x', NULL, NULL, NULL);
-- @insert_int_out_of_range
INSERT INTO dept VALUES (3000000000, 'x', NULL, NULL, NULL);
-- @insert_null_pk
INSERT INTO dept VALUES (NULL, 'x', NULL, NULL, NULL);
-- @insert_text_into_int_column_coerce
BEGIN;
INSERT INTO dept VALUES ('501', 'x', NULL, '2.5', NULL);
SELECT * FROM dept WHERE id = 501;
ROLLBACK;
-- @insert_date_ts
BEGIN;
INSERT INTO proj VALUES (1, 900, 't', 1, '2024-02-29 13:45:00');
INSERT INTO emp (id, hired) VALUES (900, '2024-02-29');
SELECT * FROM proj WHERE proj_id = 900;
SELECT id, hired FROM emp WHERE id = 900;
ROLLBACK;
-- @insert_bad_date
INSERT INTO emp (id, hired) VALUES (901, '2023-02-30');
-- @commit_visible
BEGIN;
INSERT INTO dept VALUES (600, 'c', NULL, NULL, NULL);
COMMIT;
SELECT id FROM dept WHERE id = 600;
DELETE FROM dept WHERE id = 600;
SELECT count(*) FROM dept;
-- @read_own_writes
BEGIN;
UPDATE emp SET salary = 777 WHERE dept_id = 5;
SELECT count(*) FROM emp WHERE salary = 777;
SELECT id FROM emp WHERE dept_id = 5 AND salary = 777;
DELETE FROM emp WHERE dept_id = 5;
SELECT count(*) FROM emp WHERE dept_id = 5;
SELECT count(*) FROM emp;
INSERT INTO emp (id, dept_id, salary) VALUES (700, 5, 1);
SELECT id FROM emp WHERE dept_id = 5;
SELECT d.id, count(e.id) FROM dept d LEFT JOIN emp e ON e.dept_id = d.id WHERE d.id = 5 GROUP BY d.id;
ROLLBACK;
SELECT count(*) FROM emp WHERE dept_id = 5;
-- @error_aborts_tx
BEGIN;
UPDATE emp SET salary = 1 WHERE id = 1;
SELECT nosuchcol FROM emp;
SELECT 1;
COMMIT;
SELECT salary FROM emp WHERE id = 1;
-- @savepoint
BEGIN;
UPDATE emp SET salary = 1 WHERE id = 1;
SAVEPOINT s;
ROLLBACK TO SAVEPOINT s;
COMMIT;
-- @final_state_emp
SELECT * FROM emp;
-- @final_state_dept
SELECT * FROM dept;
-- @final_state_assign
SELECT * FROM assign;
