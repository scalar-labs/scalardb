# TPC-C with BenchBase: native PostgreSQL versus the frontend on ScalarDB

[BenchBase](https://github.com/cmu-db/benchbase) runs TPC-C over pgjdbc, so the same benchmark
drives both stacks. This directory holds what the frontend side needs.

| File | What |
|---|---|
| `ddl-scalardb.sql` | the TPC-C schema for ScalarDB through the frontend: partition and clustering keys per table, `DECIMAL` as `DOUBLE PRECISION`, no foreign keys, column order identical to BenchBase's `ddl-postgres.sql` because its loader inserts positionally |
| `tpcc-postgres.xml`, `tpcc-scalardb.xml` | BenchBase configs for each stack (edit url, user, scale factor, terminals, time) |
| `benchbase-catalog.patch` | a one-line BenchBase patch: with `-Dbenchbase.catalog.hsqldb=true` it derives the table catalog from its own DDL instead of pgjdbc's metadata query, which uses a window function the frontend does not support |
| `benchbase-delivery.patch` | swaps two statements of the Delivery transaction so that it sums the order lines before updating them: ScalarDB refuses to scan records the same transaction has already written (DB-CORE-10106); the sum is identical either way, and the native run uses the same patched code |
| `summarize.py` | prints the summaries of several result directories side by side |

## Build BenchBase

BenchBase needs JDK 23 (its compiler plugin breaks on 25). Clone it, apply the patch, build the
PostgreSQL profile, and put the HSQLDB driver on the classpath (the profile does not ship it):

```sh
git clone --depth 1 https://github.com/cmu-db/benchbase.git && cd benchbase
git apply /path/to/benchbase-catalog.patch /path/to/benchbase-delivery.patch
JAVA_HOME=/path/to/jdk-23 ./mvnw -B clean package -P postgres -DskipTests
tar -xzf target/benchbase-postgres.tgz && cd benchbase-postgres
cp ~/.m2/repository/org/hsqldb/hsqldb/*/hsqldb-*.jar hsqldb.jar   # or download it from Maven Central
```

## Run

Native PostgreSQL creates, loads and runs in one go:

```sh
java -jar benchbase.jar -b tpcc -c tpcc-postgres.xml --create=true --load=true --execute=true -d results-pg
```

The frontend takes its schema from psql first (BenchBase cannot parse the ScalarDB options in it),
then loads and runs with the catalog derived from BenchBase's generic DDL:

```sh
psql -h localhost -p 15432 -d tpcc -f ddl-scalardb.sql
java -Dbenchbase.catalog.hsqldb=true -cp "benchbase.jar:hsqldb.jar:lib/*" com.oltpbenchmark.DBWorkload \
    -b tpcc -c tpcc-scalardb.xml --create=false --load=true --execute=true -d results-sdb
python3 summarize.py pg=results-pg sdb=results-sdb
```

Both configs use `TRANSACTION_REPEATABLE_READ`, which is snapshot isolation on PostgreSQL and the
closest match to ScalarDB's default `SNAPSHOT`. Conflicts that PostgreSQL resolves by blocking are
aborts on ScalarDB (SQLSTATE 40001), which BenchBase retries; compare the retry counts in the
summaries as well as throughput.

## Results (2026-10-02)

Scale factor 1 (one warehouse), 4 terminals, 60 s, `REPEATABLE READ`, everything on one laptop,
one-phase commit on, frontend started cold before each run. `results/` keeps the summaries, the
per-procedure latencies and the frontend's EXPLAIN ANALYZE output for the scan-heavy statements.

| | native PostgreSQL | frontend on ScalarDB | frontend + `c_last` index |
|---|---|---|---|
| throughput, tps | 3203 | 570 | 566 |
| goodput, tps | 3096 | 554 | 550 |
| p50 / p95 / p99 ms | 1.10 / 2.33 / 3.91 | 5.10 / 14.5 / 23.8 | 5.21 / 16.3 / 25.2 |

Per procedure, p50 / p95 ms:

| procedure | native | frontend | frontend + index | why the frontend is slower |
|---|---|---|---|---|
| NewOrder | 1.64 / 2.37 | 6.50 / 10.7 | 7.25 / 11.6 | ~26 point reads, one PostgreSQL round trip each, then a commit that writes ~23 records (ScalarDB batches writes with the same statement shape); ~46 statements through the frontend at ~30-60 µs each |
| Payment | 0.50 / 1.32 | 2.80 / 7.56 | 2.25 / 6.74 | updates pre-read their row; 60% of Payments look the customer up by last name, a partition scan (4 round trips) that filtered 3000 customers in PostgreSQL until the index |
| OrderStatus | 0.24 / 0.59 | 2.49 / 3.62 | 1.95 / 3.58 | three scans (customer by name, latest order, its lines) at 4 round trips each |
| Delivery | 2.57 / 5.11 | 12.7 / 51.5 | 14.4 / 52.4 | 10 districts x 7 statements, two scans and three updates per district |
| StockLevel | 0.67 / 0.83 | 15.4 / 21.6 | 17.5 / 24.0 | ~200 order lines each looked up in `stock` by key: inside a transaction the lookups run one after another (27 ms in BEGIN vs 8-12 ms auto-commit with parallel lookups) |

Retries and aborts (60 s): NewOrder's "aborted" are TPC-C's own 1% invalid-item rollbacks. Native
PostgreSQL retried 49k of 126k Payments (serialization failures on the single warehouse row under
`REPEATABLE READ`) and hit 3 deadlocks, which BenchBase does not retry; the frontend retried 9k of
23k Payments and 0.5k of 1.8k Deliveries (ScalarDB conflicts, reported as SQLSTATE 40001).

What this says: ScalarDB's per-operation round trips, not SQL processing, set the frontend's TPC-C
cost. A ScalarDB Get is one PostgreSQL statement, a scan several (begin, execute, end-of-cursor
fetch, commit), an update two (pre-read plus conditional update) unless the row was read earlier in
the transaction, and lookups inside a transaction cannot overlap because a `DistributedTransaction`
is not thread-safe. Multi-get and a cheaper scan in ScalarDB would move every procedure; the
frontend's own share is the per-statement protocol and planning work measured in `../README.md`.

Setup notes that cost time:

- `benchbase-delivery.patch` is needed because BenchBase's Delivery updates the order lines and
  then sums them; ScalarDB raises DB-CORE-10106 on scanning records the transaction has written.
- pgjdbc sends `setBigDecimal` parameters in binary once a statement is server-prepared (OID 1700);
  the frontend decodes PostgreSQL's base-10000 numeric format (`PostgresServer.binary`).
- Create indexes before loading. ScalarDB's `CREATE INDEX` alters the column type, and pgjdbc
  statements that its connection pool already prepared then fail with "cached plan must not change
  result type" until the frontend restarts.
- The one `Unsupported statement: SHOW ALL` error per run is BenchBase's parameter collector.
- A key condition written with arithmetic (`ol_o_id >= $4 - 20`, as older oltpbench did) is not
  pushed down yet; BenchBase computes the bound in Java, so the runs above are unaffected.

## Under network latency (2026-10-02)

`latency.sh <one-way delay µs>` repeats the comparison through `../DelayProxy.java`, with the data
reloaded through the direct ports before every run. The effective round trip is measured as a
native point read through the proxy. Three placements, 4 terminals, 60 s each; full output in
`results/latency-rtt*.txt`:

- **PostgreSQL remote**: app → delay → PostgreSQL.
- **frontend local**: app → frontend on the app's host → delay → PostgreSQL (ScalarDB's connection pool crosses the network).
- **frontend remote**: app → delay → frontend → delay → PostgreSQL (the frontend as its own tier).

| RTT | | PostgreSQL remote | frontend local | frontend remote |
|---|---|---|---|---|
| 0.03 ms (localhost) | tps / p50 ms | 3203 / 1.10 | 566 / 5.21 | |
| 0.68 ms | tps / p50 ms | 327 / 11.7 | 144 / 20.8 | 101 / 33.0 |
| 2.5 ms | tps / p50 ms | 88 / 45.0 | 42 / 72.3 | 31 / 115 |

The gap shrinks from 5.7x on localhost to 2.3x at 0.68 ms and 1.6x at 2.5 ms for the co-located
frontend, because what remains is round trips, and the frontend's CPU work stops mattering. The
remote frontend pays one extra round trip per statement on top (3.2x and 2.6x).

Round trips per transaction, taken as the slope of the p50 latency between the two RTTs
(`results/latency-roundtrips.txt`), with the localhost p50 for reference:

| procedure | PostgreSQL remote | frontend local | frontend remote |
|---|---|---|---|
| NewOrder | 25.5 | 43.7 | 67.8 |
| Payment | 7.4 | 10.0 | 17.0 |
| OrderStatus | 3.6 | 5.4 | 8.8 |
| Delivery | 43.6 | 60.2 | 102.2 |
| StockLevel | 2.6 | 190.3 | 206.9 |

Two things stand out:

- **StockLevel** is 4% of the mix but cost ~190 round trips on the frontend: the ~200 `stock`
  lookups ran one after another inside the transaction, where lookups are not parallelised. At
  2.5 ms RTT each StockLevel took 0.48 s, and the 127 of them occupied 61 of the 240
  terminal-seconds (native: 1.6 s). Batched lookups (below) fixed most of this.
- **NewOrder, Delivery, Payment** cost 1.4-1.7x native in round trips. BenchBase batches its stock
  updates and order-line inserts into one round trip each; through the frontend they are still one
  statement each at the ScalarDB level, and at commit ScalarDB's JDBC adapter batches same-shaped
  updates but runs each insert (put-if-not-exists) on its own to catch duplicate keys. Batching
  those inserts and skipping the pre-read for already-read rows are the ScalarDB-side levers; the
  frontend already avoids re-parsing and re-planning.

### Batched lookups (2026-10-02, later)

The lookup join now reads its outer rows ahead and turns lookups by full key into one partition
into a single transactional Scan with an OR of the keys (see `../README.md`, "Reading the
results"). StockLevel's ~200 stock lookups became 4 scans (`ScalarDB reads=4` in EXPLAIN ANALYZE).
Same setup as above, frontend with the `c_last` index; `results/*-batched.txt`:

| RTT | | PostgreSQL remote | frontend local, before | frontend local, batched | frontend remote, batched |
|---|---|---|---|---|---|
| 0.03 ms | tps / p50 / p95 ms | 3203 / 1.10 / 2.33 | 566 / 5.21 / 16.3 | 554 / 4.22 / 8.54 | |
| 0.65 ms | tps / p50 / p95 ms | 330 / 11.8 / 23.1 | 144 / 20.8 / 76.2 | 157 / 21.0 / 44.7 | 110 / 31.6 / 72.1 |
| 2.4 ms | tps / p50 / p95 ms | 89 / 45.0 / 85.8 | 42 / 72.3 / 421 | 51 / 68.8 / 158 | 31 / 112 / 253 |

StockLevel p50, frontend local: 17.5 → 7.9 ms on localhost, 131 → 24.8 ms at 0.65 ms RTT,
479 → 74 ms at 2.4 ms; its round trips went from ~190 to ~28 (`results/latency-roundtrips-batched.txt`).
Throughput moved by the share StockLevel had: +9% at 0.65 ms, +22% at 2.4 ms, and the p95 halved
or better because StockLevel no longer sits in the tail. The other procedures are unchanged, as
expected: BenchBase issues their reads one statement at a time, so there is nothing for the
frontend to batch.

What remains of StockLevel's ~28 round trips is the scan itself: each of the 4 scans costs a
begin, one fetch per `scalar.db.scan_fetch_size` rows (default 10) and a commit. Raising the fetch
size is a ScalarDB setting, not a frontend change. With `scalar.db.scan_fetch_size=1000` in the
frontend's properties (`PROXIED_PROPERTIES=... TAG=-fetch1000 latency.sh`,
`results/*-batched-fetch1000.txt`), frontend local went to 171 tps / p50 19.3 ms at 0.65 ms RTT
and 52.7 tps / 65.8 ms at 2.6 ms; StockLevel p50 24.8 → 13.1 ms and 74 → 30.8 ms. Every scan in
the mix benefits (OrderStatus and Delivery read order lines by scan), so the setting is worth
raising whenever the frontend talks to a remote PostgreSQL.

