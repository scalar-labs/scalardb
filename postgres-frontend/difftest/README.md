# Differential tests: native PostgreSQL vs the frontend on ScalarDB-on-PostgreSQL

Same schema and data on both sides; every case runs through psql on both and the outputs are diffed.

```sh
initdb -D pgdata -U postgres --auth=trust -E UTF8 --locale=C && pg_ctl -D pgdata -o "-p 15440 -k ''" start
psql -h localhost -p 15440 -U postgres -c "alter system set timezone = 'UTC'" -c "select pg_reload_conf()"
psql -h localhost -p 15440 -U postgres -c "create database native_c template template0 encoding 'UTF8' lc_collate 'C' lc_ctype 'C'" -c "create database sdb"
java -jar ../build/libs/scalardb-postgres-frontend-4.0.0-SNAPSHOT.jar scalardb.properties 15441 &   # the jar needs Java 17
python3 gen.py > data.sql
psql -h localhost -p 15440 -U postgres -d native_c -f schema.sql -f data.sql
psql -h localhost -p 15441 -U postgres -d sdb -f fe_init.sql -f schema.sql -f data.sql
python3 run.py cases_read.sql                         # read queries
STOP=0 python3 run.py cases_types.sql               # time, timestamptz, bytea and real columns (table types)
STOP=0 python3 run.py cases_write.sql                 # DML inside BEGIN ... ROLLBACK
STOP=0 SNAP=1 python3 run.py cases_write_ac.sql       # autocommit DML, compares all tables, reloads after each case
STOP=0 python3 run.py cases_bind.sql cases_bind2.sql  # extended protocol (psql \bind), plan-cache reuse
python3 leak.py cases_read.sql                        # reports cases that leave a backend idle in transaction
```

`FEPORT` overrides the frontend port (default 15441) for `run.py` and `leak.py`.

The cluster must be UTF-8 with a C locale and a UTC time zone, or `string_funcs` (an `e` with an
accent) and the `types` snapshots (`timestamptz` output) differ for reasons unrelated to the frontend.
Run the sets one at a time, and reload both sides (`trunc.sql` then `data.sql`) before a read run that
follows the write sets: the write and bind sets leave the two sides slightly different.

Statuses: OK, NAMES (only result column names differ), NUM_FMT (numbers differ only in formatting, e.g. 2500 vs 2500.0),
DIFF (wrong result), FE_ERR / PG_ERR (only one side errors), BOTH_ERR.

## Concurrency

`Concurrency.java` drives a frontend with 8 pgjdbc clients (prepared statements, as BenchBase does).
It needs only the frontend's fat jar, which bundles pgjdbc:

```sh
java -cp ../build/libs/scalardb-postgres-frontend-4.0.0-SNAPSHOT.jar Concurrency.java 15441 conc_si SNAPSHOT
# a second frontend whose properties add scalar.db.consensus_commit.isolation_level=SERIALIZABLE
java -cp ../build/libs/scalardb-postgres-frontend-4.0.0-SNAPSHOT.jar Concurrency.java 15443 conc_ser SERIALIZABLE
```

It fails on lost updates, duplicate keys, a changed bank total, unexpected SQLSTATEs (conflicts must be
40001) or a broken session after an error, and under SERIALIZABLE on any read skew, write skew or
phantom insert. Under SNAPSHOT those anomalies are only counted, since ScalarDB's SNAPSHOT allows them.
