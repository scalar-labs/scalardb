CREATE TABLE dept (id INT, name TEXT, region TEXT, budget DOUBLE PRECISION, parent_id INT, PRIMARY KEY (id));
CREATE TABLE emp (id INT, name TEXT, dept_id INT, mgr_id INT, salary INT, bonus DOUBLE PRECISION, active BOOLEAN, hired DATE, note TEXT, PRIMARY KEY (id));
CREATE INDEX emp_dept_idx ON emp (dept_id);
CREATE TABLE proj (dept_id INT, proj_id INT, title TEXT, cost BIGINT, started TIMESTAMP, PRIMARY KEY (dept_id, proj_id));
CREATE TABLE assign (emp_id INT, proj_id INT, hours INT, role TEXT, PRIMARY KEY (emp_id, proj_id));
CREATE INDEX assign_proj_idx ON assign (proj_id);
CREATE TABLE types (id INT, t TIME, tz TIMESTAMPTZ, b BYTEA, r REAL, PRIMARY KEY (id));
