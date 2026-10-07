-- @scan_all
SELECT * FROM emp;
-- @point_get
SELECT name, salary FROM emp WHERE id = 7;
-- @point_get_miss
SELECT * FROM emp WHERE id = 999;
-- @clustering_range ordered
SELECT * FROM proj WHERE dept_id = 2 AND proj_id >= 0 ORDER BY proj_id DESC;
-- @index_eq
SELECT id, name FROM emp WHERE dept_id = 3;
-- @where_null
SELECT id FROM emp WHERE dept_id IS NULL;
-- @where_not_null_and
SELECT id FROM emp WHERE salary IS NOT NULL AND bonus IS NULL;
-- @three_valued_not
SELECT id FROM emp WHERE NOT (salary > 1000);
-- @three_valued_or
SELECT id FROM emp WHERE salary > 3000 OR bonus > 50;
-- @neq_null
SELECT id FROM emp WHERE note <> 'x';
-- @empty_string_vs_null
SELECT id, note FROM emp WHERE note = '';
-- @bool_col
SELECT id FROM emp WHERE active;
-- @bool_not
SELECT id FROM emp WHERE NOT active;
-- @bool_is_distinct
SELECT id FROM emp WHERE active IS NOT TRUE;
-- @in_list
SELECT id FROM emp WHERE id IN (1, 5, 77, 1000);
-- @in_list_null
SELECT id FROM emp WHERE dept_id IN (1, NULL);
-- @not_in_list_null
SELECT id FROM emp WHERE dept_id NOT IN (1, NULL);
-- @not_in_subquery_null
SELECT id FROM dept WHERE id NOT IN (SELECT dept_id FROM emp);
-- @not_in_subquery_filtered
SELECT id FROM dept WHERE id NOT IN (SELECT dept_id FROM emp WHERE dept_id IS NOT NULL);
-- @between
SELECT id FROM emp WHERE salary BETWEEN 0 AND 2000;
-- @not_between
SELECT id FROM emp WHERE salary NOT BETWEEN 0 AND 2000;
-- @like
SELECT id, name FROM emp WHERE name LIKE 'b%';
-- @like_underscore
SELECT id, name FROM emp WHERE name LIKE '_ve%';
-- @ilike
SELECT id, name FROM emp WHERE name ILIKE 'alice%';
-- @like_escape_pct
SELECT id FROM emp WHERE note LIKE '\%wild%';
-- @not_like
SELECT id FROM emp WHERE note NOT LIKE '%e%';
-- @order_nulls_default ordered
SELECT id, salary FROM emp ORDER BY salary, id;
-- @order_desc_nulls ordered
SELECT id, salary FROM emp ORDER BY salary DESC, id;
-- @order_nulls_first ordered
SELECT id, bonus FROM emp ORDER BY bonus NULLS FIRST, id;
-- @order_desc_nulls_last ordered
SELECT id, bonus FROM emp ORDER BY bonus DESC NULLS LAST, id;
-- @order_text ordered
SELECT DISTINCT name FROM emp ORDER BY name;
-- @order_by_ordinal ordered
SELECT name, id FROM emp ORDER BY 2 DESC LIMIT 5;
-- @order_by_alias ordered
SELECT id, salary * 2 AS s2 FROM emp WHERE salary IS NOT NULL ORDER BY s2, id LIMIT 7;
-- @order_by_expr_not_selected ordered
SELECT id FROM emp ORDER BY coalesce(bonus, 0) - id, id LIMIT 10;
-- @limit_offset ordered
SELECT id FROM emp ORDER BY id LIMIT 5 OFFSET 10;
-- @offset_past_end ordered
SELECT id FROM emp ORDER BY id OFFSET 200;
-- @limit_zero
SELECT id FROM emp LIMIT 0;
-- @distinct_multi
SELECT DISTINCT dept_id, active FROM emp;
-- @distinct_nulls
SELECT DISTINCT note FROM emp;
-- @count_variants
SELECT count(*), count(salary), count(DISTINCT salary), count(DISTINCT dept_id), count(note) FROM emp;
-- @agg_basic
SELECT sum(salary), min(salary), max(salary), min(name), max(name), min(hired), max(hired) FROM emp;
-- @avg_int
SELECT avg(salary) FROM emp;
-- @avg_double
SELECT avg(bonus), sum(bonus) FROM emp;
-- @agg_empty
SELECT count(*), sum(salary), max(salary), avg(salary) FROM emp WHERE id < 0;
-- @group_empty
SELECT dept_id, count(*) FROM emp WHERE id < 0 GROUP BY dept_id;
-- @group_by_null_key
SELECT dept_id, count(*), sum(salary) FROM emp GROUP BY dept_id;
-- @group_by_two
SELECT dept_id, active, count(*), max(bonus) FROM emp GROUP BY dept_id, active;
-- @group_having
SELECT dept_id, count(*) AS c FROM emp GROUP BY dept_id HAVING count(*) >= 8;
-- @having_agg_not_selected
SELECT dept_id FROM emp GROUP BY dept_id HAVING sum(salary) > 10000 AND min(id) < 20;
-- @group_by_expr
SELECT salary / 1000 AS k, count(*) FROM emp GROUP BY salary / 1000;
-- @group_by_alias
SELECT coalesce(note, '?') AS n, count(*) FROM emp GROUP BY n;
-- @group_by_ordinal
SELECT active, count(*) FROM emp GROUP BY 1;
-- @agg_expr
SELECT dept_id, sum(salary * 2) + 1, count(*) * 10 FROM emp GROUP BY dept_id;
-- @agg_case
SELECT dept_id, sum(CASE WHEN active THEN 1 ELSE 0 END) AS act, count(CASE WHEN salary > 1000 THEN 1 END) FROM emp GROUP BY dept_id;
-- @agg_distinct_sum
SELECT sum(DISTINCT salary) FROM emp;
-- @agg_filter
SELECT count(*) FILTER (WHERE active) FROM emp;
-- @having_no_group
SELECT count(*) FROM emp HAVING count(*) > 1000;
-- @agg_order_limit ordered
SELECT dept_id, count(*) c FROM emp GROUP BY dept_id ORDER BY c DESC, dept_id NULLS FIRST LIMIT 3;
-- @string_agg
SELECT dept_id, string_agg(name, ',' ORDER BY id) FROM emp GROUP BY dept_id;
-- @bool_agg
SELECT bool_and(active), bool_or(active) FROM emp;
-- @int_division
SELECT id, salary / 3, salary % 7, -salary / 3, -salary % 7 FROM emp WHERE id <= 12;
-- @int_overflow
SELECT sum(cost) FROM proj;
-- @int_mult_overflow
SELECT 2147483647 * 2;
-- @division_by_zero
SELECT 1 / 0;
-- @mixed_arith
SELECT id, salary + bonus, salary * 1.5, bonus / 2 FROM emp WHERE id <= 10;
-- @arith_precedence
SELECT 2 + 3 * 4, (2 + 3) * 4, 10 - 2 - 3, 2 ^ 3, -2 * -3, 7 / 2, 7.0 / 2;
-- @null_arith
SELECT id, salary + NULL, NULL = NULL, NULL IS NULL FROM emp WHERE id = 1;
-- @coalesce_nullif
SELECT id, coalesce(salary, bonus, -1), nullif(salary, 2000) FROM emp WHERE id <= 15;
-- @case_searched
SELECT id, CASE WHEN salary IS NULL THEN 'none' WHEN salary < 1000 THEN 'low' WHEN salary < 3000 THEN 'mid' ELSE 'high' END FROM emp;
-- @case_simple
SELECT id, CASE dept_id WHEN 1 THEN 'one' WHEN 2 THEN 'two' END FROM emp;
-- @case_null_simple
SELECT CASE NULL WHEN NULL THEN 'match' ELSE 'nomatch' END;
-- @greatest_least
SELECT id, greatest(salary, 1500), least(salary, 1500) FROM emp WHERE id <= 12;
-- @string_funcs
SELECT id, upper(name), lower(name), length(name), substring(name, 2, 3), name || '-' || id, concat(name, NULL, id) FROM emp WHERE id <= 12;
-- @string_funcs2
SELECT id, trim('  ' || name || '  '), replace(name, 'a', 'A'), position('e' in name), left(name, 2), right(name, 2), reverse(name) FROM emp WHERE id <= 12;
-- @concat_null
SELECT id, name || note FROM emp WHERE id <= 12;
-- @abs_round
SELECT id, abs(salary), round(bonus), round(bonus, 1), ceil(bonus), floor(bonus) FROM emp WHERE id <= 12;
-- @mod_sign
SELECT id, mod(salary, 7), sign(bonus) FROM emp WHERE id <= 12;
-- @cast_text_int
SELECT CAST('42' AS INT) + 1, '7'::int * 2, CAST(salary AS TEXT) FROM emp WHERE id = 6;
-- @cast_double_int
SELECT CAST(2.5 AS INT), CAST(3.5 AS INT), CAST(-2.5 AS INT), CAST(bonus AS INT) FROM emp WHERE id = 7;
-- @cast_bool
SELECT CAST(active AS TEXT), 'true'::boolean FROM emp WHERE id = 2;
-- @date_cmp
SELECT id, hired FROM emp WHERE hired >= DATE '2020-01-01';
-- @date_cmp_str
SELECT id FROM emp WHERE hired < '2015-06-01';
-- @timestamp_cmp
SELECT proj_id, started FROM proj WHERE started > TIMESTAMP '2023-06-01 00:00:00';
-- @extract_year
SELECT id, EXTRACT(YEAR FROM hired) FROM emp WHERE id <= 10;
-- @date_arith
SELECT id, hired + 1 FROM emp WHERE id <= 5;
-- @date_trunc
SELECT proj_id, date_trunc('month', started) FROM proj;
-- @bigint_cmp
SELECT proj_id, cost FROM proj WHERE cost > 2147483647;
-- @double_eq
SELECT id FROM dept WHERE budget = 1000.5;
-- @text_cmp
SELECT id FROM emp WHERE name > 'carol';
-- @text_cmp_case
SELECT id FROM emp WHERE name < 'a';
-- @inner_join
SELECT e.id, d.name FROM emp e JOIN dept d ON e.dept_id = d.id;
-- @inner_join_where
SELECT e.id, d.name FROM emp e JOIN dept d ON e.dept_id = d.id WHERE d.region = 'north' AND e.salary > 1000;
-- @left_join
SELECT e.id, d.name FROM emp e LEFT JOIN dept d ON e.dept_id = d.id;
-- @left_join_is_null
SELECT d.id FROM dept d LEFT JOIN emp e ON e.dept_id = d.id WHERE e.id IS NULL;
-- @left_join_on_filter
SELECT d.id, e.id FROM dept d LEFT JOIN emp e ON e.dept_id = d.id AND e.salary > 3000;
-- @left_join_on_left_filter
SELECT d.id, e.id FROM dept d LEFT JOIN emp e ON e.dept_id = d.id AND d.region = 'north';
-- @right_join
SELECT e.id, d.id FROM emp e RIGHT JOIN dept d ON e.dept_id = d.id;
-- @full_join
SELECT e.id, d.id FROM emp e FULL JOIN dept d ON e.dept_id = d.id;
-- @full_join_where
SELECT e.id, d.id FROM emp e FULL JOIN dept d ON e.dept_id = d.id WHERE e.id IS NULL OR d.id IS NULL;
-- @cross_join
SELECT count(*) FROM dept a CROSS JOIN dept b;
-- @comma_join
SELECT e.id, p.proj_id FROM emp e, proj p WHERE e.dept_id = p.dept_id AND e.id < 10;
-- @self_join_mgr
SELECT e.id, m.name FROM emp e JOIN emp m ON e.mgr_id = m.id;
-- @self_join_dept_tree
SELECT c.id, p.name FROM dept c LEFT JOIN dept p ON c.parent_id = p.id;
-- @join_non_equi
SELECT e.id, d.id FROM emp e JOIN dept d ON e.salary < d.budget WHERE e.id < 10;
-- @join_three
SELECT e.name, p.title, a.hours FROM assign a JOIN emp e ON a.emp_id = e.id JOIN proj p ON a.proj_id = p.proj_id;
-- @join_four_left
SELECT e.id, d.name, a.proj_id, p.title FROM emp e LEFT JOIN dept d ON d.id = e.dept_id LEFT JOIN assign a ON a.emp_id = e.id LEFT JOIN proj p ON p.proj_id = a.proj_id WHERE e.id <= 20;
-- @join_left_then_inner
SELECT e.id, a.proj_id, p.title FROM emp e LEFT JOIN assign a ON a.emp_id = e.id JOIN proj p ON p.proj_id = a.proj_id;
-- @join_using
SELECT dept_id, e.id, p.proj_id FROM emp e JOIN proj p USING (dept_id) WHERE e.id <= 15;
-- @join_using_full
SELECT dept_id, count(*) FROM (SELECT id, dept_id FROM emp) e FULL JOIN (SELECT dept_id, proj_id FROM proj) p USING (dept_id) GROUP BY dept_id;
-- @natural_join
SELECT * FROM (SELECT id AS emp_id, name FROM emp) e NATURAL JOIN assign;
-- @join_group
SELECT d.name, count(e.id), coalesce(sum(e.salary), 0) FROM dept d LEFT JOIN emp e ON e.dept_id = d.id GROUP BY d.name;
-- @join_group_having ordered
SELECT d.id, count(*) AS n FROM dept d JOIN emp e ON e.dept_id = d.id GROUP BY d.id HAVING count(*) > 5 ORDER BY n DESC, d.id;
-- @join_null_keys
SELECT a.id, b.id FROM emp a JOIN emp b ON a.dept_id = b.dept_id WHERE a.id < b.id AND a.id < 15;
-- @join_or_condition
SELECT e.id, d.id FROM emp e JOIN dept d ON e.dept_id = d.id OR e.mgr_id = d.id WHERE e.id <= 10;
-- @join_derived
SELECT d.name, s.total FROM dept d JOIN (SELECT dept_id, sum(salary) AS total FROM emp GROUP BY dept_id) s ON s.dept_id = d.id;
-- @join_derived_filtered
SELECT e.id, x.title FROM emp e JOIN (SELECT * FROM proj WHERE cost > 0) x ON x.dept_id = e.dept_id WHERE e.id < 30;
-- @scalar_subquery
SELECT id, (SELECT name FROM dept d WHERE d.id = e.dept_id) FROM emp e;
-- @scalar_subquery_agg
SELECT id, salary - (SELECT avg(salary) FROM emp) FROM emp WHERE id <= 10;
-- @scalar_subquery_correlated_agg
SELECT d.id, (SELECT count(*) FROM emp e WHERE e.dept_id = d.id), (SELECT max(salary) FROM emp e WHERE e.dept_id = d.id) FROM dept d;
-- @scalar_subquery_multi_row_err
SELECT id, (SELECT id FROM emp) FROM dept;
-- @exists
SELECT d.id FROM dept d WHERE EXISTS (SELECT 1 FROM emp e WHERE e.dept_id = d.id AND e.salary > 3000);
-- @not_exists
SELECT d.id FROM dept d WHERE NOT EXISTS (SELECT 1 FROM emp e WHERE e.dept_id = d.id);
-- @in_subquery
SELECT id FROM emp WHERE dept_id IN (SELECT id FROM dept WHERE region = 'south');
-- @in_subquery_correlated
SELECT e.id FROM emp e WHERE e.id IN (SELECT a.emp_id FROM assign a WHERE a.hours > e.id / 4);
-- @where_gt_avg_correlated
SELECT e.id FROM emp e WHERE e.salary > (SELECT avg(x.salary) FROM emp x WHERE x.dept_id = e.dept_id);
-- @any_all
SELECT id FROM emp WHERE salary > ALL (SELECT salary FROM emp WHERE dept_id = 1);
-- @any_some
SELECT id FROM emp WHERE salary = ANY (SELECT salary FROM emp WHERE dept_id = 2);
-- @all_with_nulls
SELECT id FROM dept WHERE id > ALL (SELECT dept_id FROM emp WHERE id > 70);
-- @subquery_in_from_agg
SELECT max(c), min(c) FROM (SELECT dept_id, count(*) c FROM emp GROUP BY dept_id) t;
-- @nested_subquery
SELECT id FROM emp WHERE dept_id IN (SELECT id FROM dept WHERE parent_id IN (SELECT id FROM dept WHERE region = 'north'));
-- @exists_uncorrelated_empty
SELECT count(*) FROM emp WHERE EXISTS (SELECT 1 FROM dept WHERE id = -1);
-- @union
SELECT dept_id FROM emp UNION SELECT id FROM dept;
-- @union_all
SELECT dept_id FROM proj UNION ALL SELECT id FROM dept WHERE id > 8;
-- @intersect
SELECT dept_id FROM emp INTERSECT SELECT dept_id FROM proj;
-- @except
SELECT id FROM dept EXCEPT SELECT dept_id FROM emp;
-- @union_order_limit ordered
SELECT id, name FROM emp WHERE id < 5 UNION ALL SELECT id, name FROM dept ORDER BY id DESC, name LIMIT 6;
-- @union_nulls
SELECT NULL::int AS x UNION SELECT NULL::int;
-- @cte
WITH s AS (SELECT dept_id, sum(salary) total FROM emp GROUP BY dept_id) SELECT d.name, s.total FROM s JOIN dept d ON d.id = s.dept_id WHERE s.total > 5000;
-- @cte_twice
WITH a AS (SELECT id, salary FROM emp WHERE salary > 0) SELECT x.id, y.id FROM a x JOIN a y ON x.salary = y.salary AND x.id < y.id WHERE x.id < 20;
-- @cte_chain
WITH a AS (SELECT * FROM emp WHERE active), b AS (SELECT dept_id, count(*) n FROM a GROUP BY dept_id) SELECT * FROM b WHERE n > 2;
-- @cte_cols
WITH t(k, v) AS (SELECT id, name FROM dept) SELECT k, v FROM t WHERE k < 4;
-- @values
SELECT * FROM (VALUES (1, 'a'), (2, NULL), (3, 'c')) AS v(n, s) WHERE n > 1;
-- @select_no_from
SELECT 1 + 1, 'x', NULL, true;
-- @star_qualified
SELECT d.*, e.id FROM dept d JOIN emp e ON e.dept_id = d.id WHERE e.id = 4;
-- @column_alias_case
SELECT id AS "Id", name AS nm FROM dept WHERE id = 1;
-- @tpch_like_q1 ordered
SELECT active, count(*) AS cnt, sum(salary) AS sum_sal, avg(bonus) AS avg_bonus, min(hired) FROM emp WHERE hired <= DATE '2023-12-31' GROUP BY active ORDER BY active;
-- @tpch_like_q3 ordered
SELECT a.proj_id, sum(a.hours) AS h, p.started FROM proj p JOIN assign a ON a.proj_id = p.proj_id JOIN emp e ON e.id = a.emp_id WHERE e.active AND p.started < TIMESTAMP '2023-10-01 00:00:00' GROUP BY a.proj_id, p.started ORDER BY h DESC NULLS LAST, a.proj_id LIMIT 5;
-- @tpch_like_q13
SELECT c, count(*) AS dist FROM (SELECT e.id, count(a.proj_id) AS c FROM emp e LEFT JOIN assign a ON a.emp_id = e.id AND a.role <> 'qa' GROUP BY e.id) t GROUP BY c;
-- @tpch_like_q17
SELECT sum(a.hours) / 7.0 FROM assign a JOIN proj p ON p.proj_id = a.proj_id WHERE a.hours < (SELECT 0.5 * avg(x.hours) + 10 FROM assign x WHERE x.proj_id = a.proj_id);
-- @tpch_like_q22
SELECT dept_id, count(*), sum(salary) FROM emp WHERE salary > (SELECT avg(salary) FROM emp WHERE salary > 0) AND NOT EXISTS (SELECT * FROM assign a WHERE a.emp_id = emp.id) GROUP BY dept_id;
-- @top_n_per_group
SELECT e.dept_id, e.id, e.salary FROM emp e WHERE (SELECT count(*) FROM emp x WHERE x.dept_id = e.dept_id AND x.salary > e.salary) < 2;
-- @window_row_number
SELECT id, row_number() OVER (PARTITION BY dept_id ORDER BY id) FROM emp;
-- @recursive_cte
WITH RECURSIVE r AS (SELECT id, parent_id FROM dept WHERE id = 5 UNION ALL SELECT d.id, d.parent_id FROM dept d JOIN r ON d.id = r.parent_id) SELECT * FROM r;
-- @window_rank
SELECT id, dept_id, salary, rank() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS r, dense_rank() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS dr, row_number() OVER (PARTITION BY dept_id ORDER BY salary DESC, id) AS rn FROM emp WHERE dept_id IN (1, 2, 3);
-- @window_lag_lead
SELECT id, lag(salary) OVER (PARTITION BY dept_id ORDER BY id) AS prev, lead(salary, 2, -1) OVER (PARTITION BY dept_id ORDER BY id) AS next2 FROM emp WHERE dept_id <= 3;
-- @window_running_sum
SELECT id, sum(salary) OVER (PARTITION BY dept_id ORDER BY id) AS running, count(*) OVER (PARTITION BY dept_id) AS cnt FROM emp WHERE dept_id <= 3;
-- @window_frame_rows
SELECT id, sum(salary) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS moving, min(salary) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS low, max(salary) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS high FROM emp WHERE id <= 10;
-- @window_first_last
SELECT id, first_value(name) OVER (PARTITION BY dept_id ORDER BY id) AS f, last_value(name) OVER (PARTITION BY dept_id ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS l, nth_value(name, 2) OVER (PARTITION BY dept_id ORDER BY id) AS second FROM emp WHERE dept_id <= 3;
-- @window_over_groups
SELECT dept_id, count(*) AS n, rank() OVER (ORDER BY count(*) DESC, dept_id) AS r, sum(count(*)) OVER () AS total FROM emp GROUP BY dept_id;
-- @window_named
SELECT id, salary, rank() OVER w AS r, sum(salary) OVER w AS s FROM emp WHERE dept_id = 2 WINDOW w AS (ORDER BY salary DESC);
-- @window_ntile_cume
SELECT id, ntile(3) OVER (ORDER BY id) AS bucket, cume_dist() OVER (ORDER BY salary) AS cd, percent_rank() OVER (ORDER BY salary) AS pr FROM emp WHERE id <= 10;
-- @window_order_by_alias ordered
SELECT id, row_number() OVER (ORDER BY salary DESC, id) AS rn FROM emp WHERE dept_id = 1 ORDER BY rn LIMIT 3;
-- @window_in_order_by ordered
SELECT id FROM emp WHERE dept_id = 1 ORDER BY row_number() OVER (ORDER BY salary DESC, id) LIMIT 3;
-- @window_filter
SELECT id, count(*) FILTER (WHERE active) OVER (PARTITION BY dept_id) AS actives, sum(salary) FILTER (WHERE salary > 1000) OVER () AS rich FROM emp WHERE dept_id <= 3;
-- @window_peers_range
SELECT id, salary, sum(salary) OVER (ORDER BY salary) AS by_peers, count(*) OVER (ORDER BY salary RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS at_or_above FROM emp WHERE dept_id = 1;
-- @recursive_series
WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 10) SELECT i, i * i AS sq FROM n;
-- @recursive_down
WITH RECURSIVE tree AS (SELECT id, name, 1 AS depth FROM dept WHERE parent_id IS NULL UNION ALL SELECT d.id, d.name, t.depth + 1 FROM tree t JOIN dept d ON d.parent_id = t.id) SELECT id, name, depth FROM tree;
-- @recursive_up_cols
WITH RECURSIVE up(id, pid, lvl) AS (SELECT id, parent_id, 0 FROM dept WHERE id = 12 UNION ALL SELECT d.id, d.parent_id, u.lvl + 1 FROM up u JOIN dept d ON d.id = u.pid) SELECT * FROM up;
-- @recursive_mgr_chain
WITH RECURSIVE chain AS (SELECT id, mgr_id, name FROM emp WHERE id = 40 UNION ALL SELECT e.id, e.mgr_id, e.name FROM chain c JOIN emp e ON e.id = c.mgr_id) SELECT id, name FROM chain;
-- @recursive_union_cycle
WITH RECURSIVE c(x) AS (SELECT 1 UNION SELECT (x % 5) + 1 FROM c) SELECT x FROM c;
-- @recursive_agg_over
WITH RECURSIVE tree AS (SELECT id, 1 AS depth FROM dept WHERE parent_id IS NULL UNION ALL SELECT d.id, t.depth + 1 FROM tree t JOIN dept d ON d.parent_id = t.id) SELECT depth, count(*) AS n FROM tree GROUP BY depth;
-- @recursive_plain_union
WITH RECURSIVE u AS (SELECT id FROM dept WHERE id < 3 UNION ALL SELECT id FROM dept WHERE id > 12) SELECT * FROM u;
-- @generate_series
SELECT n, n * n AS sq FROM generate_series(1, 5) AS g(n);
-- @generate_series_step
SELECT * FROM generate_series(10, 1, -3);
-- @distinct_on ordered
SELECT DISTINCT ON (dept_id) dept_id, id, salary FROM emp WHERE dept_id <= 3 ORDER BY dept_id, salary DESC, id;
-- @grouping_sets
SELECT dept_id, active, count(*) AS n FROM emp WHERE dept_id <= 3 GROUP BY GROUPING SETS ((dept_id, active), (dept_id), ());
-- @rollup
SELECT dept_id, active, count(*) AS n, sum(salary) AS s FROM emp WHERE dept_id <= 3 GROUP BY ROLLUP (dept_id, active);
-- @cube
SELECT dept_id, active, count(*) AS n FROM emp WHERE dept_id <= 2 GROUP BY CUBE (dept_id, active);
-- @rollup_having
SELECT dept_id, count(*) AS n FROM emp WHERE dept_id <= 3 GROUP BY ROLLUP (dept_id) HAVING count(*) > 5;
-- @fetch_first ordered
SELECT id FROM dept ORDER BY id FETCH FIRST 3 ROWS ONLY;
-- @fetch_first_row ordered
SELECT id FROM dept ORDER BY id OFFSET 2 ROWS FETCH FIRST ROW ONLY;
-- @escape_string
SELECT E'a\nb' AS s, length(E'a\nb') AS n, E'\x41\101B' AS h, E'it\'s' AS q, E'back\\slash' AS b, 'a\nb' AS plain;
-- @interval_literals
SELECT interval '1 year 2 months 3 days 04:05:06.5' AS a, interval '90 minutes' AS b, interval '1.5 days' AS c, interval '1 day' * 2.5 AS d, -interval '1 day 01:00:00' AS e, '36 hours'::interval AS f, interval '1' day AS g, interval '2 weeks' AS h, interval '1 day' / 3 AS i, interval '1 day 2 hours' - interval '3 hours' AS j;
-- @interval_arith
SELECT dept_id, proj_id, started + interval '1 day 2 hours' AS a, started - interval '1 month' AS b, started - timestamp '2024-01-01 00:00:00' AS c, age(timestamp '2025-01-01 00:00:00', started) AS d FROM proj WHERE dept_id <= 2;
-- @interval_extract
SELECT dept_id, proj_id, extract(epoch from (timestamp '2025-01-01 00:00:00' - started)) AS e, extract(day from (timestamp '2025-01-01 00:00:00' - started)) AS d FROM proj WHERE dept_id <= 2;
-- @interval_date
SELECT id, hired + interval '1 week' AS a, hired - interval '36 hours' AS b FROM emp WHERE id <= 5;
-- @interval_compare
SELECT interval '1 day' = interval '24 hours' AS a, interval '1 month' > interval '29 days' AS b, interval '1 hour' < interval '90 minutes' AS c;
-- @interval_pushdown
SELECT dept_id, proj_id FROM proj WHERE started > timestamp '2025-01-01 00:00:00' - interval '2 years';
-- @at_time_zone
SET TIME ZONE 'UTC';
SELECT id, tz AT TIME ZONE 'Asia/Tokyo' AS local FROM types WHERE id <= 3;
-- @timestamp_at_time_zone
SET TIME ZONE 'UTC';
SELECT dept_id, proj_id, started AT TIME ZONE 'Asia/Tokyo' AS tz FROM proj WHERE dept_id = 1;
-- @any_array
SELECT id, name FROM emp WHERE id = ANY(ARRAY[1, 3, 5]);
-- @any_array_nonkey
SELECT id FROM emp WHERE dept_id = ANY(ARRAY[1, 2]) AND salary = ANY('{1000,2000,3000}');
-- @any_text_array
SELECT id FROM emp WHERE name = ANY('{alice,"Bob",nobody}');
-- @all_array
SELECT id FROM emp WHERE dept_id <> ALL(ARRAY[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
-- @any_gt_array
SELECT id FROM emp WHERE salary > ALL(ARRAY[3000, 3500]) OR id < ANY(ARRAY[3, 2]);
-- @any_bind
SELECT id FROM emp WHERE id = ANY($1) \bind '{2,4,6}' \g
-- @array_output
SELECT ARRAY[1, 2] AS a, ARRAY['x', 'y z', NULL] AS b, '{1,2}'::int[] AS c, cardinality(ARRAY[1, 2, 3]) AS n, array_length(ARRAY[7]::int[], 1) AS l;
-- @unnest_rows
SELECT n, n * 10 AS d FROM unnest(ARRAY[3, 4]) AS u(n);
-- @any_empty
SELECT id FROM emp WHERE id = ANY(ARRAY[]::int[]) OR id = 1;
-- @dollar_quote
SELECT $$it's$$ AS s, $tag$a $ b$tag$ AS t;
-- @bitwise_ops
SELECT 5 # 3, 1 << 2, 8 >> 1, ~5, 6 & 3, 6 | 3;
-- @except_all
SELECT id FROM emp WHERE id <= 5 EXCEPT ALL SELECT 2 ORDER BY 1;
-- @intersect_all
SELECT id FROM emp WHERE id <= 5 INTERSECT ALL SELECT 2 ORDER BY 1;
-- @with_ordinality
SELECT v, n FROM unnest(ARRAY['x','y']) WITH ORDINALITY AS u(v, n);
-- @is_distinct
SELECT id FROM emp WHERE dept_id IS DISTINCT FROM 3 AND id <= 12 ORDER BY id;
-- @is_not_distinct_null
SELECT id FROM emp WHERE dept_id IS NOT DISTINCT FROM NULL ORDER BY id;
-- @similar_to
SELECT id, name FROM emp WHERE name SIMILAR TO '(a|b)%' ORDER BY id;
-- @similar_to_underscore
SELECT id, name FROM emp WHERE name NOT SIMILAR TO '%e_' AND id <= 12 ORDER BY id;
-- @between_symmetric
SELECT id FROM emp WHERE id BETWEEN SYMMETRIC 5 AND 3 ORDER BY id;
-- @row_compare
SELECT id FROM emp WHERE (dept_id, id) > (2, 60) ORDER BY id;
-- @row_equal
SELECT id FROM emp WHERE (dept_id, active) = (3, true) ORDER BY id;
-- @row_in
SELECT id FROM emp WHERE (id, dept_id) IN ((1, 1), (2, 2), (7, 3), (7, 1)) ORDER BY id;
-- @array_subscript
SELECT (ARRAY[10,20,30])[2], (ARRAY[10,20,30])[2:3], (ARRAY[10,20,30])[5] IS NULL;
-- @fetch_with_ties
SELECT id, salary FROM emp ORDER BY salary DESC FETCH FIRST 3 ROWS WITH TIES;
-- @tablesample_all
SELECT count(*) FROM emp TABLESAMPLE SYSTEM (100);
-- @uuid_json_casts
SELECT 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'::uuid, '{"a":1}'::json;
-- @analyze
ANALYZE emp;
-- @vacuum
VACUUM emp;
-- @grouping_fn
SELECT dept_id, active, grouping(dept_id) AS gd, grouping(active) AS ga, grouping(dept_id, active) AS g, count(*) AS n FROM emp WHERE dept_id <= 3 GROUP BY ROLLUP (dept_id, active) ORDER BY 5, 1, 2;
-- @grouping_sets_fn
SELECT dept_id, active, grouping(dept_id, active) AS g, count(*) AS n FROM emp WHERE dept_id <= 3 GROUP BY GROUPING SETS ((dept_id), (active), ()) ORDER BY 3, 1, 2;
-- @lateral_count
SELECT d.id, l.n FROM dept d JOIN LATERAL (SELECT count(*) AS n FROM emp e WHERE e.dept_id = d.id) l ON true WHERE d.id <= 3 ORDER BY d.id;
-- @lateral_top_n
SELECT d.id, l.id AS emp_id, l.salary FROM dept d, LATERAL (SELECT e.id, e.salary FROM emp e WHERE e.dept_id = d.id ORDER BY e.salary DESC, e.id LIMIT 2) l WHERE d.id <= 3 ORDER BY d.id, l.salary DESC, l.id;
-- @lateral_left
SELECT d.id, l.id AS emp_id FROM dept d LEFT JOIN LATERAL (SELECT e.id FROM emp e WHERE e.dept_id = d.id AND e.salary > 100000 ORDER BY e.id LIMIT 1) l ON true WHERE d.id <= 3 ORDER BY d.id;
-- @json_ops
SELECT j -> 'a' AS a, j ->> 'b' AS b, j -> 'c' -> 0 AS c0, j -> 'c' ->> -1 AS clast, j #> '{c,1}' AS c1, j #>> '{d,e}' AS de, j -> 'zz' IS NULL AS missing FROM (SELECT '{"a": {"x": 1}, "b": "text", "c": [10, "s", true, null], "d": {"e": 2.50}}'::jsonb AS j) t;
-- @json_predicates
SELECT j @> '{"b": "text"}' AS c1, j @> '{"c": [10]}' AS c2, '{"c": [10]}'::jsonb <@ j AS c3, j ? 'a' AS k1, j ? 'zz' AS k2, j ?| ARRAY['zz', 'b'] AS any_key, j ?& ARRAY['a', 'b'] AS all_keys FROM (SELECT '{"a": {"x": 1}, "b": "text", "c": [10, "s", true, null]}'::jsonb AS j) t;
-- @json_edit
SELECT j || '{"z": 0, "b": 1}'::jsonb AS merged, j - 'a' AS without_a, (j -> 'c') - 0 AS tail, (j -> 'c') || '[7]'::jsonb AS appended FROM (SELECT '{"a": 1, "b": "text", "c": [10, 20]}'::jsonb AS j) t;
-- @json_functions
SELECT jsonb_build_object('b', 1, 'a', 'x', 'n', NULL, 'arr', ARRAY[1, 2]), json_build_object('b', 1, 'a', 'x'), jsonb_build_array(1, 'a', true, NULL), to_jsonb('x'::text), to_json(1.5), jsonb_typeof('[1]'::jsonb), jsonb_typeof('{"a":1}'::jsonb -> 'a'), jsonb_array_length('[1, 2, 3]'::jsonb), jsonb_extract_path_text('{"a": {"b": "c"}}'::jsonb, 'a', 'b');
-- @json_output_format
SELECT '{"b": [1, 2, {"zz": "é", "a": "q\"t"}], "a": 1.50, "longer": null, "c": true, "B": 1e3}'::jsonb, '{"b":1,"a":2}'::json, '"str"'::jsonb, '[]'::jsonb, '{}'::jsonb;
-- @json_agg
SELECT dept_id, json_agg(name ORDER BY id) AS names, jsonb_agg(salary ORDER BY id) AS salaries, jsonb_object_agg(id, name ORDER BY id) AS by_id FROM emp WHERE dept_id <= 2 AND id <= 20 GROUP BY dept_id ORDER BY dept_id;
-- @json_elements
SELECT e.value, e.value ->> 'k' AS k FROM jsonb_array_elements('[{"k": "a"}, {"k": "b"}]'::jsonb) AS e(value) ORDER BY 2;
-- @json_elements_lateral
SELECT t.id, e.v FROM (VALUES (1, '[1, 2]'::jsonb), (2, '[3]'::jsonb)) AS t(id, j), jsonb_array_elements_text(t.j) AS e(v) ORDER BY t.id, e.v;
-- @json_filter
SELECT id, note FROM (VALUES (1, '{"status": "open"}'), (2, '{"status": "closed"}'), (3, NULL)) AS t(id, note) WHERE note::jsonb ->> 'status' = 'open';

