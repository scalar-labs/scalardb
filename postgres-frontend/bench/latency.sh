#!/usr/bin/env bash
# latency.sh <one-way delay us> : reloads the data, then runs the scenarios under that delay
set -u
D=$1
S=${SCRATCH:?set SCRATCH to a directory holding pgbench/data, pgbench/data.sql and pgbench/scalardb-proxied.properties}
export JAVA_HOME=/Users/hiroyuki/Library/Java/JavaVirtualMachines/temurin-17.0.12/Contents/Home
export PATH=$JAVA_HOME/bin:/opt/homebrew/opt/postgresql@18/bin:$PATH
JAR=/Users/hiroyuki/Dropbox/Docs/git/scalardb/postgres-frontend/build/libs/scalardb-postgres-frontend-4.0.0-SNAPSHOT.jar
cd /Users/hiroyuki/Dropbox/Docs/git/scalardb/postgres-frontend/bench
pg_ctl -D $S/pgbench/data -o "-p 15434 -c listen_addresses=127.0.0.1" -l $S/pgbench/pg.log start >/dev/null 2>&1
(java DelayProxy.java 15437 127.0.0.1 15434 $D >| $S/pgbench/proxy-pg.log 2>&1 &)
python3 -c 'import time; time.sleep(4)'
(java -jar $JAR $S/pgbench/scalardb-proxied.properties 15435 >| $S/pgbench/frontend.log 2>&1 &)
(java DelayProxy.java 15436 127.0.0.1 15435 $D >| $S/pgbench/proxy-fe.log 2>&1 &)
python3 -c 'import time; time.sleep(8)'
grep -q "Listening" $S/pgbench/frontend.log || { echo "frontend failed to start"; tail -3 $S/pgbench/frontend.log; }
# fresh data, loaded through the direct ports
psql -h 127.0.0.1 -p 15434 -d bench -X -q -v ON_ERROR_STOP=1 -c "TRUNCATE orders, customers, items, kv, counters" -f $S/pgbench/data.sql -c "INSERT INTO kv SELECT id, 0 FROM customers" -c "VACUUM ANALYZE" 2>&1 | tail -1
psql -h 127.0.0.1 -p 15435 -d bench -X -q -v ON_ERROR_STOP=1 -c "TRUNCATE TABLE orders" -c "TRUNCATE TABLE customers" -c "TRUNCATE TABLE items" -c "TRUNCATE TABLE kv" -c "TRUNCATE TABLE counters" -f $S/pgbench/data.sql -c "SET scalardb.max_rows_per_write = 100000" -c "INSERT INTO kv (id, v) SELECT id, 0 FROM customers" 2>&1 | tail -1
psql -h 127.0.0.1 -p 15434 -d sdb -X -q -c "VACUUM ANALYZE"
PG=postgresql://hiroyuki@127.0.0.1:15437/bench
FE_LOCAL=postgresql://hiroyuki@127.0.0.1:15435/bench
FE_REMOTE=postgresql://hiroyuki@127.0.0.1:15436/bench
p50() { pgbench -n -M prepared -c $1 -j $1 -T $2 -D ncust=20000 -D nitems=10000 -D norders=10 -f scripts/$3.sql $4 2>/dev/null | grep "latency average" | awk '{print $4}'; }
tps() { pgbench -n -M prepared -c $1 -j $1 -T $2 -D ncust=20000 -D nitems=10000 -D norders=10 -f scripts/$3.sql $4 2>/dev/null | grep "^tps" | awk '{printf "%.0f", $3}'; }
PM=$(head -1 $S/pgbench/data/postmaster.pid)
direct() { WARMUP=8 java -cp $JAR Breakdown.java $S/pgbench/scalardb-proxied.properties jdbc:postgresql://127.0.0.1:15437/bench x $1 tx 1 5 $PM 0 2>/dev/null | grep -v INFO | tail -1 | cut -f6; }
echo "RTT $((2*D)) us, 1 client, p50 ms"
printf '%-12s %10s %12s %14s %14s\n' script "PG remote" "ScalarDB API" "frontend local" "frontend remote"
for s in point_get join_fanout; do p50 1 3 $s $FE_LOCAL >/dev/null; p50 1 3 $s $FE_REMOTE >/dev/null; done
for s in point_get join_fanout; do
  api="-"; [ $s = point_get ] && api=$(direct get)
  printf '%-12s %10s %12s %14s %14s\n' $s "$(p50 1 5 $s $PG)" "$api" "$(p50 1 6 $s $FE_LOCAL)" "$(p50 1 6 $s $FE_REMOTE)"
done
echo "RTT $((2*D)) us, 8 clients, point_get tps: PG remote $(tps 8 5 point_get $PG), frontend local $(tps 8 5 point_get $FE_LOCAL), frontend remote $(tps 8 5 point_get $FE_REMOTE)"
for s in update_key insert txn_mix; do p50 1 3 $s $FE_LOCAL >/dev/null; p50 1 3 $s $FE_REMOTE >/dev/null; done
for s in update_key insert txn_mix; do
  api="-"; case $s in update_key) api=$(direct put);; insert) api=$(direct insert);; esac
  printf '%-12s %10s %12s %14s %14s\n' $s "$(p50 1 5 $s $PG)" "$api" "$(p50 1 6 $s $FE_LOCAL)" "$(p50 1 6 $s $FE_REMOTE)"
done
echo "RTT $((2*D)) us, 8 clients, txn_mix tps: PG remote $(tps 8 5 txn_mix $PG), frontend local $(tps 8 5 txn_mix $FE_LOCAL), frontend remote $(tps 8 5 txn_mix $FE_REMOTE)"
pkill -f "DelayProxy"; pkill -f "scalardb-postgres-frontend.*pgbench"; pg_ctl -D $S/pgbench/data stop -m fast >/dev/null 2>&1
