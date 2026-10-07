#!/usr/bin/env bash
# latency.sh <one-way delay us>: TPC-C under network delay through bench/DelayProxy.java.
# Scenarios: PostgreSQL remote; frontend next to the app with PostgreSQL remote; frontend remote
# (app -> delay -> frontend -> delay -> PostgreSQL). Data is reloaded through the direct ports before
# every run. Needs SCRATCH (pgbench/data cluster on 15434, pgbench/scalardb.properties and
# scalardb-proxied.properties pointing at 15434 and 15437, tpcc/*.xml configs), JAVA23, BENCHBASE.
# Optional: PROXIED_PROPERTIES (the frontend's ScalarDB properties for the delayed runs, to try
# settings such as scalar.db.scan_fetch_size) and TAG (suffix of the output directory).
set -u
D=$1
S=${SCRATCH:?set SCRATCH}
JAVA23=${JAVA23:?set JAVA23 to a JDK 23 java binary}
BB=${BENCHBASE:?set BENCHBASE to the benchbase-postgres directory}
PROXIED=${PROXIED_PROPERTIES:-$S/pgbench/scalardb-proxied.properties}
PG=/opt/homebrew/opt/postgresql@18/bin
T=$(cd "$(dirname "$0")" && pwd)
JAR=$T/../../build/libs/scalardb-postgres-frontend-4.0.0-SNAPSHOT.jar
OUT=$S/tpcc/lat$D${TAG:-}
mkdir -p "$OUT"
bb() { (cd "$BB" && $JAVA23 -Dbenchbase.catalog.hsqldb=true -cp "benchbase.jar:hsqldb.jar:lib/*" com.oltpbenchmark.DBWorkload -b tpcc "$@"); }
bb_pg() { (cd "$BB" && $JAVA23 -jar benchbase.jar -b tpcc "$@"); }
start_fe() {
  pkill -f "scalardb-postgres-frontend.*15435"; sleep 2
  (java -jar "$JAR" "$1" 15435 >| "$OUT/frontend-$2.log" 2>&1 &)
  for i in $(seq 60); do $PG/psql -h 127.0.0.1 -p 15435 -d tpcc -Atc "SELECT 1" >/dev/null 2>&1 && return; sleep 1; done
  echo "frontend did not start"; tail -3 "$OUT/frontend-$2.log"
}
load_fe() {
  start_fe "$S/pgbench/scalardb.properties" direct-$1
  $PG/psql -h 127.0.0.1 -p 15435 -d tpcc -X -q -f "$T/ddl-scalardb.sql" 2>&1 | tail -3
  bb -c "$S/tpcc/sdb.xml" --create=false --load=true --execute=false >| "$OUT/fe-load-$1.log" 2>&1 || echo "frontend load failed"
}
pkill -f DelayProxy; sleep 1
(cd "$T/.." && java DelayProxy.java 15437 127.0.0.1 15434 "$D" >| "$OUT/proxy-pg.log" 2>&1 &)
(cd "$T/.." && java DelayProxy.java 15436 127.0.0.1 15435 "$D" >| "$OUT/proxy-fe.log" 2>&1 &)
sleep 4
echo "one-way delay $D us; effective RTT = $($PG/pgbench -n -M prepared -c 1 -T 3 -D ncust=20000 -D nitems=10000 -D norders=10 -f "$T/../scripts/point_get.sql" postgresql://hiroyuki@127.0.0.1:15437/bench 2>/dev/null | grep 'latency average' | awk '{print $4, $5}') for a native point read through the proxy (0.03 ms direct)"
echo "=== PostgreSQL remote $(date +%T)"
bb_pg -c "$S/tpcc/pg.xml" --create=true --load=true --execute=false >| "$OUT/pg-load.log" 2>&1 || echo "native load failed"
bb_pg -c "$S/tpcc/pg-remote.xml" --create=false --load=false --execute=true -d "$OUT/results-pg" >| "$OUT/pg-run.log" 2>&1 || echo "native run failed"
echo "=== frontend next to the app, PostgreSQL remote $(date +%T)"
load_fe local
start_fe "$PROXIED" local
bb -c "$S/tpcc/sdb.xml" --create=false --load=false --execute=true -d "$OUT/results-fe-local" >| "$OUT/fe-local-run.log" 2>&1 || echo "frontend local run failed"
echo "=== frontend remote $(date +%T)"
load_fe remote
start_fe "$PROXIED" remote
bb -c "$S/tpcc/fe-remote.xml" --create=false --load=false --execute=true -d "$OUT/results-fe-remote" >| "$OUT/fe-remote-run.log" 2>&1 || echo "frontend remote run failed"
pkill -f DelayProxy
start_fe "$S/pgbench/scalardb.properties" restored
echo "--- unexpected errors: $(cat "$OUT"/*-run.log | grep -c 'will not be retried')"
python3 "$T/summarize.py" "PG remote=$OUT/results-pg,$OUT/pg-run.log" "frontend local=$OUT/results-fe-local,$OUT/fe-local-run.log" "frontend remote=$OUT/results-fe-remote,$OUT/fe-remote-run.log"
