# Driver and ORM compatibility runs

One script per stack, each a typical application session against a frontend on port 15444 with
database (namespace) `orm`: connect, introspect, create a table, insert with parameters, list
membership, update, delete, and a transaction. Every step prints `ok` or `FAIL` with the error, so a
run doubles as a gap list. First run on 2026-10-04.

```sh
java -jar ../build/libs/scalardb-postgres-frontend-4.0.0-SNAPSHOT.jar scalardb.properties 15444 &   # Java 17
psql -h localhost -p 15444 -U postgres -d orm -c "CREATE SCHEMA IF NOT EXISTS orm"

# Python: psycopg 3 + SQLAlchemy 2
python3 -m venv venv && ./venv/bin/pip install 'psycopg[binary]' sqlalchemy && ./venv/bin/python python/test_py.py

# Node: pg (node-postgres) + Sequelize 6
(cd node && npm install pg sequelize && node test_node.js)

# Go: pgx v5
(cd go && go mod init compat && go get github.com/jackc/pgx/v5 && go run .)

# Ruby: pg + ActiveRecord 7
gem install pg activerecord && ruby ruby/test_rb.rb
```

| Stack | Raw driver | ORM |
|---|---|---|
| psycopg 3 / SQLAlchemy 2 | all steps | declarative ORM CRUD ok; `get_pk_constraint` and `get_indexes` return empty, `get_columns` raises NoSuchTableError: their catalog queries need `json_build_object`, `pg_sequence`, `pg_get_serial_sequence` and `pg_attribute` joins the engine does not plan |
| node-postgres / Sequelize 6 | all steps | `sync`, CRUD, `showIndex`, `describeTable` and `showAllTables` ok; the transaction step reads rows it wrote (DB-CORE-10106) |
| pgx v5 | all steps, binary parameters and results | |
| pg / ActiveRecord 7 | all steps | all steps, including `serial` primary keys and the primary-key lookup via `generate_subscripts` |

Known limits that show up here: a read of rows the same transaction wrote fails with DB-CORE-10106
(Sequelize's transaction step, SQLAlchemy's `create_all` plus insert plus select in one transaction);
DDL inside a transaction runs at once and is not undone by ROLLBACK; `ROLLBACK TO SAVEPOINT` is
honored only while nothing was written since the savepoint. The connected namespace is presented as
schema `public`, as PostgreSQL would, so tools that assume `public` find the tables; the namespace
name still works as a qualifier.

Two things the runs taught about the frontend itself. ScalarDB caches table metadata and its admin
does not refresh the cache, so a table dropped and recreated with other columns (two test suites
sharing a name, or `ALTER TABLE ADD COLUMN` followed by an insert) was rejected with DB-CORE-10017
until the cache expired: the frontend now sets `scalar.db.metadata.cache_expiration_time_secs` to 1
unless the properties file sets it, and answers DROP/ALTER/index DDL only after that second has
passed. And a failed statement inside a transaction left the ORM's ROLLBACK reporting "No active
transaction" instead of the real error; COMMIT and ROLLBACK outside a transaction are now no-ops, as
in PostgreSQL, and statement failures are logged at INFO.

A catalog query the engine cannot plan is answered with no rows but with its output columns
described, as PostgreSQL would answer an empty result; before, the missing RowDescription made
psycopg report no result at all (SQLAlchemy's ResourceClosedError). A Java Error inside a statement
(a stack overflow on deeply nested SQL, a missing class) is reported as SQLSTATE XX000 and the
connection stays open; before, the connection thread died without a message.
