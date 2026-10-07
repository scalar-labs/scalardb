#!/usr/bin/env bash
# The point read, point write and point insert through every layer, with CPU per operation.
#
#   breakdown.sh <scalardb.properties> <pg jdbc url> <frontend jdbc url> <postmaster pid> <frontend pid> [threads...]
#
# e.g. breakdown.sh scalardb.properties jdbc:postgresql://127.0.0.1:5432/bench \
#        jdbc:postgresql://127.0.0.1:15432/bench $(head -1 /path/to/data/postmaster.pid) $(pgrep -f scalardb-postgres-frontend) 1 4
# Needs the kv table (schema/*.sql) loaded with a row per customer, see README.
set -euo pipefail
cd "$(dirname "$0")"
props=$1; pg=$2; fe=$3; postmaster=$4; frontend=$5; shift 5
threads=${*:-"1 4"}
jar=$(ls ../build/libs/scalardb-postgres-frontend-*.jar | head -1)
seconds=${SECONDS_PER_RUN:-5}   # timed seconds per run; WARMUP (default 10) is untimed
printf 'op\tlayer\tthreads\tops\ttps\tp50_ms\tp95_ms\tp99_ms\tclient_cpu_us\tpg_cpu_us\tfrontend_cpu_us\n'
for op in get put insert; do
  for t in $threads; do
    for layer in pg storage tx frontend; do
      java -cp "$jar" Breakdown.java "$props" "$pg" "$fe" "$op" "$layer" "$t" "$seconds" "$postmaster" "$frontend" 2>/dev/null | grep -v INFO
    done
  done
done
