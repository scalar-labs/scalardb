-- @ac_update_pk_returning
UPDATE emp SET salary = salary + 1 WHERE id = 7 RETURNING id, salary;
-- @ac_update_scan
UPDATE emp SET bonus = coalesce(bonus, 0) * 2, note = 'raised' WHERE dept_id = 3 AND salary >= 1000;
-- @ac_update_null_where
UPDATE emp SET note = 'nodept' WHERE dept_id IS NULL;
-- @ac_update_set_null_returning
UPDATE emp SET salary = NULL WHERE id IN (1, 2, 3) RETURNING *;
-- @ac_update_swap
UPDATE emp SET salary = mgr_id, mgr_id = salary WHERE id = 10;
-- @ac_update_swap_scan
UPDATE emp SET salary = mgr_id, mgr_id = salary WHERE dept_id = 2;
-- @ac_update_subquery
UPDATE emp SET salary = (SELECT max(salary) FROM emp) WHERE id = 11;
-- @ac_update_correlated
UPDATE emp SET bonus = (SELECT count(*) FROM assign a WHERE a.emp_id = emp.id) WHERE dept_id = 4;
-- @ac_update_in_subquery
UPDATE emp SET active = false WHERE dept_id IN (SELECT id FROM dept WHERE region = 'east');
-- @ac_update_no_match_pk
UPDATE emp SET salary = 1 WHERE id = 9999;
-- @ac_update_no_match_pk_returning
UPDATE emp SET salary = 1 WHERE id = 9999 RETURNING id;
-- @ac_update_pk_extra_cond_false
UPDATE emp SET salary = 1 WHERE id = 7 AND salary = -12345;
-- @ac_update_pk_extra_cond_null
UPDATE emp SET salary = 1 WHERE id = 11 AND salary = 0;
-- @ac_update_pk_cond_true
UPDATE emp SET salary = 1 WHERE id = 6 AND salary IS NOT NULL;
-- @ac_update_clustering_range
UPDATE assign SET hours = 99 WHERE emp_id = 4 AND proj_id > 105;
-- @ac_update_pk_column
UPDATE dept SET id = 50 WHERE id = 1;
-- @ac_delete_scan_returning
DELETE FROM assign WHERE hours IS NULL RETURNING emp_id, proj_id;
-- @ac_delete_not_exists
DELETE FROM assign a WHERE NOT EXISTS (SELECT 1 FROM emp e WHERE e.id = a.emp_id);
-- @ac_delete_pk_twice
DELETE FROM emp WHERE id = 80;
DELETE FROM emp WHERE id = 80;
-- @ac_delete_pk_cond_false
DELETE FROM emp WHERE id = 79 AND name = 'nobody';
-- @ac_delete_partition
DELETE FROM proj WHERE dept_id = 2;
-- @ac_delete_using_join_semantics
DELETE FROM emp WHERE id IN (SELECT emp_id FROM assign WHERE role = 'qa');
-- @ac_delete_all
DELETE FROM assign;
-- @ac_insert_multi_returning
INSERT INTO dept VALUES (100, 'new', 'west', 1.5, NULL), (101, 'n2', NULL, NULL, 100) RETURNING id, name;
-- @ac_insert_partial_cols
INSERT INTO emp (id, name) VALUES (500, 'x');
-- @ac_insert_cols_reordered
INSERT INTO emp (name, salary, id) VALUES ('y', 5, 501);
-- @ac_insert_multi_dup_within
INSERT INTO dept VALUES (102, 'a', NULL, NULL, NULL), (102, 'b', NULL, NULL, NULL);
-- @ac_insert_multi_one_dup
INSERT INTO dept VALUES (103, 'a', NULL, NULL, NULL), (1, 'b', NULL, NULL, NULL);
-- @ac_insert_select
INSERT INTO dept (id, name, budget) SELECT id + 1000, name, salary FROM emp WHERE salary > 3000;
-- @ac_insert_select_self
INSERT INTO assign (emp_id, proj_id, hours, role) SELECT emp_id, proj_id + 1000, hours, role FROM assign WHERE role = 'lead';
-- @ac_insert_expr_values
INSERT INTO dept VALUES (104, upper('abc') || 'd', NULL, 2 * 3.5, 1 + 1);
-- @ac_insert_quote
INSERT INTO dept VALUES (400, 'it''s', NULL, NULL, NULL);
-- @ac_insert_date_ts
INSERT INTO proj VALUES (1, 900, 't', 1, '2024-02-29 13:45:00');
-- @ac_insert_negative_and_bigint
INSERT INTO proj VALUES (-1, -900, 't', -9223372036854775808, NULL);
-- @ac_upsert
INSERT INTO dept VALUES (1, 'up', 'x', 9.0, NULL), (300, 'ins', 'x', 9.0, NULL) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, region = EXCLUDED.region, budget = EXCLUDED.budget, parent_id = EXCLUDED.parent_id;
-- @ac_upsert_partial
INSERT INTO dept (id, name) VALUES (2, 'renamed') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;
-- @ac_upsert_expr_returning
INSERT INTO dept VALUES (2, 'zz', NULL, 1.0, NULL) ON CONFLICT (id) DO UPDATE SET budget = dept.budget + EXCLUDED.budget RETURNING id, budget;
-- @ac_upsert_nothing
INSERT INTO dept VALUES (3, 'zz', NULL, 1.0, NULL), (301, 'new', NULL, 1.0, NULL) ON CONFLICT (id) DO NOTHING;
-- @ac_upsert_nothing_no_target
INSERT INTO dept VALUES (3, 'zz', NULL, 1.0, NULL) ON CONFLICT DO NOTHING;
-- @ac_upsert_where
INSERT INTO dept VALUES (4, 'zz', NULL, 1.0, NULL) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE dept.region = 'nowhere';
-- @ac_upsert_composite
INSERT INTO assign VALUES (4, 106, 1, 'x'), (4, 999, 2, 'y') ON CONFLICT (emp_id, proj_id) DO UPDATE SET hours = EXCLUDED.hours, role = EXCLUDED.role;
-- @ac_tx_commit_multi
BEGIN;
UPDATE emp SET salary = 1 WHERE id = 1;
INSERT INTO dept VALUES (600, 'c', NULL, NULL, NULL);
DELETE FROM assign WHERE emp_id = 4 AND proj_id = 106;
COMMIT;
-- @ac_tx_rollback
BEGIN;
UPDATE emp SET salary = 1 WHERE id = 1;
INSERT INTO dept VALUES (600, 'c', NULL, NULL, NULL);
ROLLBACK;
-- @ac_tx_error_then_commit
BEGIN;
UPDATE emp SET salary = 1 WHERE id = 1;
SELECT nosuchcol FROM emp;
COMMIT;
-- @ac_tx_scan_then_write
BEGIN;
SELECT id, salary FROM emp WHERE dept_id = 2;
UPDATE emp SET salary = 5 WHERE dept_id = 2;
COMMIT;
-- @ac_tx_get_after_write
BEGIN;
UPDATE emp SET salary = 5 WHERE id = 2;
SELECT salary FROM emp WHERE id = 2;
DELETE FROM emp WHERE id = 3;
SELECT * FROM emp WHERE id = 3;
INSERT INTO emp (id) VALUES (3);
SELECT id, salary FROM emp WHERE id = 3;
COMMIT;
-- @ac_truncate
TRUNCATE assign;
-- @ac_update_from
UPDATE emp SET bonus = d.budget * 0.01 FROM dept d WHERE d.id = emp.dept_id AND d.region = 'east';
-- @ac_update_from_alias_returning
UPDATE emp e SET salary = e.salary + 1 FROM dept d WHERE d.id = e.dept_id AND e.id = 7 RETURNING e.id, e.salary;
-- @ac_update_from_values
UPDATE dept SET budget = v.b FROM (VALUES (1, 10.5), (2, 20.5)) AS v(id, b) WHERE dept.id = v.id;
-- @ac_update_from_many_matches
UPDATE dept SET name = p.title FROM proj p WHERE p.dept_id = dept.id AND dept.id = 2 AND p.proj_id = (SELECT min(proj_id) FROM proj WHERE dept_id = 2);
-- @ac_delete_using
DELETE FROM assign USING proj p WHERE p.proj_id = assign.proj_id AND p.dept_id = 2;
-- @ac_delete_using_returning
DELETE FROM assign a USING emp e WHERE e.id = a.emp_id AND e.id = 7 RETURNING a.emp_id, a.proj_id;

