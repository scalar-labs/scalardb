# Microbenchmarks: the PostgreSQL frontend on ScalarDB versus native PostgreSQL

One pgbench script per engine feature, run with the same SQL against both stacks on the same
PostgreSQL instance. Each script is an OLTP-style statement with random parameters. Alongside
time, `check_ops.py` verifies how many ScalarDB operations each statement issues, which catches
planner regressions independently of machine noise.

## Layout

| Path | What |
|---|---|
| `schema/postgres.sql`, `schema/scalardb.sql` | the schema for each stack, with the same access paths (primary-key index = partition + clustering key, same secondary indexes) |
| `gen_data.py` | writes `data.sql`, multi-row INSERTs that psql loads into either stack |
| `scripts/*.sql` | the pgbench scripts, one feature each (variables `ncust`, `nitems`, `norders`) |
| `expected.tsv` | ScalarDB reads one execution of each script must issue (checked on customer 1, which `insert` and `txn_mix` never touch) |
| `check_ops.py` | runs `EXPLAIN ANALYZE` of each script on the frontend and compares with `expected.tsv` |
| `run.sh` | runs all scripts with pgbench against one stack, writes `results/<label>.tsv` |
| `report.py` | `summarize` a run directory, or `compare` two result files as a markdown table |
| `DirectBaseline.java` | the point read and range scan issued through the ScalarDB Java API, to see what the frontend adds on top of ScalarDB |
| `Breakdown.java`, `breakdown.sh` | one point read, write and insert through every layer from the same Java client, with CPU per operation for the client, PostgreSQL and the frontend |
| `DelayProxy.java` | a TCP proxy adding a fixed one-way delay each way, to measure under a realistic round trip: put one between the client and the frontend and one between the frontend (its `scalar.db.contact_points`) or the client and PostgreSQL |
| `statements.py` | summarizes a PostgreSQL log written with `log_min_duration_statement=0`: the SQL each layer actually sends per operation, with counts and server-side execution times |
| `tpcc/` | the BenchBase TPC-C comparison: ScalarDB DDL, configs, two BenchBase patches, `latency.sh` for the same three placements, and its own README with the results on localhost and under network delay |

## Setup

Needs PostgreSQL client tools (`psql`, `pgbench`) and python3. Both stacks use one PostgreSQL server:

```sh
createdb bench            # native tables
createdb scalardb_bench   # ScalarDB's tables
psql -d bench -f schema/postgres.sql
python3 gen_data.py --customers 20000 --items 10000 --orders 10 --out data.sql
psql -d bench -f data.sql

cp scalardb.properties.example scalardb.properties   # edit contact_points/user/password
java -jar ../build/libs/scalardb-postgres-frontend-*.jar scalardb.properties 15432 &
psql -h localhost -p 15432 -d bench -f schema/scalardb.sql
psql -h localhost -p 15432 -d bench -f data.sql       # a few minutes: every batch is a transaction
```

## Run

```sh
python3 check_ops.py -h localhost -p 15432 -d bench     # operation counts, frontend only
./run.sh pg  postgresql://localhost:5432/bench  -c 4 -T 10
./run.sh sdb postgresql://localhost:15432/bench -c 4 -T 10
python3 report.py compare results/pg.tsv results/sdb.tsv
```

Every write script leaves ScalarDB's PostgreSQL tables with dead tuples (each Consensus Commit
write is two UPDATEs of the record), and `insert`/`txn_mix` grow `orders`, so numbers drift
across runs: compare configurations on a freshly loaded database, or at least after `VACUUM
ANALYZE` on the ScalarDB database, and run a warm-up pass first when the frontend was just
started (the JIT needs tens of seconds on the Consensus Commit path).

`run.sh` defaults to `-M prepared` (the extended protocol, as drivers use it), which hits the
frontend's plan cache: a statement is parsed and planned once per session and only bound to its
values per execution (EXPLAIN shows such a plan with `$n` in its keys and conditions). `-M simple`
sends literal SQL, so every statement is parsed and planned; the difference between the two modes
is that cost. Plans that embed a value (LIMIT $n, IN lists, VALUES rows other than a plain
INSERT) are not cached, which `EXPLAIN` does not show but `QueryParser.Plan#isCacheable` does. Scale variables must match the data: `--ncust`, `--nitems`, `--norders`. Write
conflicts, which ScalarDB reports as SQLSTATE 40001 where PostgreSQL would block, are retried up
to three times and counted in the `retried` column.

The third configuration, for attributing the gap on reads, needs no server:

```sh
java -cp ../build/libs/scalardb-postgres-frontend-*.jar DirectBaseline.java scalardb.properties get 4 5 20000
java -cp ../build/libs/scalardb-postgres-frontend-*.jar DirectBaseline.java scalardb.properties scan 4 5 20000
```

To break a point operation down by layer (PostgreSQL over JDBC, ScalarDB storage Get/Put, ScalarDB
Consensus Commit Get/Update, the frontend over JDBC), with the CPU each layer burns per operation:

```sh
./breakdown.sh scalardb.properties jdbc:postgresql://127.0.0.1:5432/bench jdbc:postgresql://127.0.0.1:15432/bench \
    $(head -1 /path/to/pgdata/postmaster.pid) $(pgrep -f scalardb-postgres-frontend) 1 4
```

It needs the `kv` table from the schema files loaded with one row per customer, for example
`INSERT INTO kv (id, v) SELECT id, 0 FROM customers` (raise `scalardb.max_rows_per_write` on
the frontend first). CPU is sampled with `ps` at 10 ms resolution, so use runs of several seconds,
and let the frontend warm up (JIT) before trusting its single-thread CPU figure.

To measure under a network round trip, run `java DelayProxy.java 15437 127.0.0.1 5432 250` (0.25
ms each way, a 0.5 ms round trip) and point the frontend's `scalar.db.contact_points` and the
native runs at port 15437; a second proxy in front of the frontend's port stands for the hop from
the application. The proxy itself costs about 40 µs per round trip, so run it with a delay of 0
first for the baseline. `latency.sh <one-way µs>` automates five scripts for four placements
(PostgreSQL remote, the ScalarDB API in the app, the frontend next to the app, the frontend
remote) and `latency-all.sh <one-way µs>...` runs the whole suite at one client for the three
SQL placements; both label the runs by the effective round trip they measure. Results of both are
in `results/latency-*.txt` and `results/lat<delay>-*.tsv`.

## Reading the results

Count round trips before anything else. A ScalarDB Get on the JDBC storage is one round trip
(autocommit), but a Scan is four: `BEGIN READ ONLY`, the cursor's execute, a second fetch when
the first returned exactly `scalar.db.scan_fetch_size` rows (default 10), and `COMMIT`. A
Consensus Commit update is a pre-read plus the conditional update. A lookup join reads its outer
rows ahead (16, doubling to 128) and issues their lookups together: lookups by full key into one
partition become a single Scan with an OR of the keys, which also works inside `BEGIN` because the
scan goes through the transaction (if the transaction already wrote one of the rows, ScalarDB
rejects the scan with DB-CORE-10106 and that batch falls back to Gets); lookups that cannot share
a scan run concurrently, 16 at a time, for auto-commit statements only (a ScalarDB transaction
object is not thread-safe, so inside `BEGIN` they stay sequential). An IN list or a fan-out join
therefore costs a few round trips instead of one per row.

The gap has three layers: the frontend (parsing, planning, in-memory operators), Consensus
Commit (metadata on every read, prepare plus coordinator plus commit writes per transaction), and
access patterns (one SQL statement per ScalarDB Get or Scan, no server-side joins). Reads with one
operation (`point_get`, `range_scan`, `index_*`) isolate the first two layers; `join_fanout` with
its 11 operations shows the third; `txn_mix` shows the commit cost, and `txn_read` versus
`txn_read_ro` (`BEGIN READ ONLY`, which the frontend maps to a read-only ScalarDB transaction)
shows that read-only transactions do not write to the coordinator either way: the core omits the
coordinator write for transactions without writes by default. One-phase commit
(`scalar.db.consensus_commit.one_phase_commit.enabled`, see the properties example) roughly
halves single-partition writes. `insert` and `txn_mix` add
rows to `orders` with ids above 1,000,000; `update_scan` flips statuses; `upsert` writes to
`counters`. Reload `data.sql` to start over.

## Results under network latency (2026-10-02, current jar)

`results/latency-all-rtt0.7ms.txt`, `latency-all-rtt2.6ms.txt` (whole suite, 1 client, three
placements), `latency-all-fetch1000.txt` (frontend next to the app with
`scalar.db.scan_fetch_size=1000`), `latency-rtt0.5ms.txt`, `latency-rtt2ms.txt` (five scripts with
the ScalarDB API column and 8-client throughput). p50 ms at 1 client, frontend next to the app;
the ratio to native PostgreSQL over the same delay in parentheses:

| script | RTT 0.7 ms: PG / frontend / frontend fetch 1000 | RTT 2.6 ms: PG / frontend / frontend fetch 1000 |
|---|---|---|
| point_get | 0.64 / 0.79 (1.2x) / 0.75 | 2.34 / 2.75 (1.2x) / 2.62 |
| insert | 0.70 / 0.82 (1.2x) / 0.81 | 2.42 / 2.76 (1.1x) / 2.65 |
| in_list | 0.67 / 0.95 (1.4x) / 0.89 | 2.37 / 2.63 (1.1x) / 2.54 |
| txn_mix | 3.14 / 3.16 (1.0x) / 2.96 | 12.9 / 11.4 (0.9x) / 11.4 |
| txn_read | 2.57 / 3.17 (1.2x) / 2.40 | 9.84 / 11.0 (1.1x) / 8.06 |
| update_key, upsert | 0.66 / 1.44 (2.2x) / 1.38 | 2.39 / 5.24 (2.2x) / 5.24 |
| range_scan, cte, join_one | 0.65 / 1.48 (2.3x) / 1.41 | 2.34 / 5.1-5.4 (2.2x) / 5.1 |
| aggregate, sort_memory, subquery | 0.65 / 2.2 (3.4x) / 1.5-2.1 | 2.35 / 7.3-8.1 (3.4x) / 4.9-7.6 |
| index_point | 0.64 / 2.72 (4.2x) / 2.81 | 2.33 / 10.7 (4.6x) / 10.5 |
| join_lookup | 0.66 / 2.85 (4.3x) / 2.09 | 2.35 / 11.0 (4.7x) / 7.61 |
| join_fanout | 0.66 / 3.32 (5.0x) / 2.62 | 2.37 / 10.5 (4.4x) / 8.14 |
| update_scan, set_op, index_multi | 0.67 / 3.6-4.2 (5-6x) / 2.8-2.9 | 2.4 / 13.8-16.7 (6-7x) / 10.6-10.8 |
| scan_filter | 0.71 / 20.2 (28x) / 3.29 | 2.51 / 67.6 (27x) / 11.5 |
| join_hash | 0.66 / 40.8 (61x) / 5.32 | 2.45 / 161 (66x) / 17.2 |

The frontend as its own tier adds one round trip per statement on top (`fe remote` columns in the
files): point_get 1.44 ms at 0.7 ms RTT, txn_mix 6.19 ms.

Reading it: with a real network the CPU cost of the frontend disappears into the round trips, and
the ratio to native becomes the ratio of round trips. One Get or Insert is one round trip on both
stacks (1.1-1.2x). An Update is two (pre-read, then the conditional update). A ScalarDB scan on
JDBC is about three (begin pipelined with the execute, a fetch to see the cursor's end, commit),
which is the 2.2x of every single-scan script, and then one more fetch per
`scalar.db.scan_fetch_size` rows: at the default of 10 that is what makes `scan_filter`,
`join_hash` and `index_multi` 6-60x slower, and raising the fetch size to 1000 brings them to
4-8x. The 8-client throughputs in `latency-rtt*.txt` tell the same story: at 2 ms RTT the
co-located frontend matches native PostgreSQL on point reads (2935 vs 2949 tps) and on `txn_mix`
(719 vs 635 tps), because 8 clients are latency-bound on both sides.

The TPC-C comparison, on localhost and under the same delays, is in `tpcc/README.md`.

