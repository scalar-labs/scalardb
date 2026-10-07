#!/usr/bin/env bash
# latency-all.sh <one-way delay us> [more delays...]: every script, one client, under network delay,
# for three placements: PostgreSQL remote (app -> delay -> PG), frontend local (app -> frontend ->
# delay -> PG), frontend remote (app -> delay -> frontend -> delay -> PG). Data is reloaded once and
# the frontend warmed up before the first delay. Needs SCRATCH as latency.sh does.
set -u
S=${SCRATCH:?set SCRATCH to a directory holding pgbench/data, pgbench/data.sql and pgbench/scalardb-proxied.properties}
T=${T:-4}
export JAVA_HOME=/Users/hiroyuki/Library/Java/JavaVirtualMachines/temurin-17.0.12/Contents/Home
export PATH=$JAVA_HOME/bin:/opt/homebrew/opt/postgresql@18/bin:$PATH
JAR=/Users/hiroyuki/Dropbox/Docs/git/scalardb/postgres-frontend/build/libs/scalardb-postgres-frontend-4.0.0-SNAPSHOT.jar
cd /Users/hiroyuki/Dropbox/Docs/git/scalardb/postgres-frontend/bench
pg_isready -q -h 127.0.0.1 -p 15434 || pg_ctl -D $S/pgbench/data -o "-p 15434 -c listen_addresses=127.0.0.1" -l $S/pgbench/pg.log start >/dev/null 2>&1
for i in $(seq 30); do pg_isready -q -h 127.0.0.1 -p 15434 && break; sleep 1; done
pg_isready -q -h 127.0.0.1 -p 15434 || { echo "PostgreSQL is not up on 15434"; exit 1; }
pkill -f "scalardb-postgres-frontend.*15435"; pkill -f "java DelayProxy"; sleep 2
# the frontend reaches PostgreSQL through the proxy, so a proxy with no delay serves the reload
(java DelayProxy.java 15437 127.0.0.1 15434 0 >| $S/pgbench/proxy-pg.log 2>&1 &)
sleep 2
(java -jar $JAR $S/pgbench/scalardb-proxied.properties 15435 >| $S/pgbench/frontend-latency-all.log 2>&1 &)
for i in $(seq 90); do psql -h 127.0.0.1 -p 15435 -d bench -Atc "SELECT 1" >/dev/null 2>&1 && break; sleep 1; done
psql -h 127.0.0.1 -p 15435 -d bench -Atc "SELECT 1" >/dev/null 2>&1 || { echo "frontend did not start"; tail -5 $S/pgbench/frontend-latency-all.log; exit 1; }
echo "=== reload $(date +%T)"
psql -h 127.0.0.1 -p 15434 -d bench -X -q -v ON_ERROR_STOP=1 -c "TRUNCATE orders, customers, items, kv, counters" -f $S/pgbench/data.sql -c "INSERT INTO kv SELECT id, 0 FROM customers" -c "VACUUM ANALYZE" 2>&1 | tail -1
psql -h 127.0.0.1 -p 15435 -d bench -X -q -v ON_ERROR_STOP=1 -c "TRUNCATE TABLE orders" -c "TRUNCATE TABLE customers" -c "TRUNCATE TABLE items" -c "TRUNCATE TABLE kv" -c "TRUNCATE TABLE counters" -f $S/pgbench/data.sql -c "SET scalardb.max_rows_per_write = 100000" -c "INSERT INTO kv (id, v) SELECT id, 0 FROM customers" 2>&1 | tail -1
psql -h 127.0.0.1 -p 15434 -d sdb -X -q -c "VACUUM ANALYZE"
echo "=== warm-up $(date +%T)"
./run.sh warm-latency postgresql://hiroyuki@127.0.0.1:15435/bench -c 1 -T 2 >/dev/null 2>&1
for D in "$@"; do
  pkill -f "java DelayProxy"; sleep 1
  (java DelayProxy.java 15437 127.0.0.1 15434 $D >| $S/pgbench/proxy-pg.log 2>&1 &)
  (java DelayProxy.java 15436 127.0.0.1 15435 $D >| $S/pgbench/proxy-fe.log 2>&1 &)
  sleep 3
  echo "=== one-way delay $D us: effective RTT $(pgbench -n -M prepared -c 1 -T 3 -D ncust=20000 -D nitems=10000 -D norders=10 -f scripts/point_get.sql postgresql://hiroyuki@127.0.0.1:15437/bench 2>/dev/null | grep 'latency average' | awk '{print $4}') ms (native point read through the proxy) $(date +%T)"
  ./run.sh lat$D-pg postgresql://hiroyuki@127.0.0.1:15437/bench -c 1 -T $T >/dev/null 2>&1
  ./run.sh lat$D-fe-local postgresql://hiroyuki@127.0.0.1:15435/bench -c 1 -T $T >/dev/null 2>&1
  ./run.sh lat$D-fe-remote postgresql://hiroyuki@127.0.0.1:15436/bench -c 1 -T $T >/dev/null 2>&1
  python3 - lat$D-pg lat$D-fe-local lat$D-fe-remote <<'PY'
import csv, sys
runs = {label: {r["script"]: r for r in csv.DictReader(open(f"results/{label}.tsv"), delimiter="\t")} for label in sys.argv[1:]}
pg, local, remote = (runs[l] for l in sys.argv[1:])
p50 = next(k for k in next(iter(pg.values())) if k.startswith("p50"))
print(f"{'script':<13}{'PG remote':>11}{'fe local':>10}{'fe remote':>10}{'local/PG':>10}{'remote/PG':>10}   p50 ms, 1 client")
for s in sorted(pg):
    a, b, c = (float(r[s][p50]) for r in (pg, local, remote))
    print(f"{s:<13}{a:>11.3f}{b:>10.3f}{c:>10.3f}{b / a:>10.1f}{c / a:>10.1f}")
PY
done
pkill -f "java DelayProxy"; pkill -f "scalardb-postgres-frontend.*15435"; sleep 2
(java -jar $JAR $S/pgbench/scalardb.properties 15435 >| $S/pgbench/frontend-restored.log 2>&1 &)
