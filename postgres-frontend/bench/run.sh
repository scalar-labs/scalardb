#!/usr/bin/env bash
# Runs every pgbench script against one stack and summarizes the results.
#
#   run.sh <label> <conninfo> [-c clients] [-T seconds] [-M simple|prepared] [--only a,b,...]
#          [--ncust N] [--nitems N] [--norders N]
#
# <conninfo> is a libpq URI such as postgresql://user@localhost:5432/bench.
# Output: results/<label>.tsv (one line per script) and results/<label>/ (pgbench output and logs).
set -euo pipefail
cd "$(dirname "$0")"
label=${1:?label}; conn=${2:?conninfo}; shift 2
clients=4; seconds=10; mode=prepared; only=""; ncust=20000; nitems=10000; norders=10
while [ $# -gt 0 ]; do
  case "$1" in
    -c) clients=$2; shift 2;;
    -T) seconds=$2; shift 2;;
    -M) mode=$2; shift 2;;
    --only) only=$2; shift 2;;
    --ncust) ncust=$2; shift 2;;
    --nitems) nitems=$2; shift 2;;
    --norders) norders=$2; shift 2;;
    *) echo "unknown option $1" >&2; exit 2;;
  esac
done
dir=results/$label
rm -rf "$dir"; mkdir -p "$dir"
# reads run before writes, so they see the data as loaded
scripts=$(ls scripts/*.sql | grep -v -E '/(insert|update_key|update_scan|upsert|txn_mix)\.sql$'; ls scripts/*.sql | grep -E '/(insert|update_key|update_scan|upsert|txn_mix)\.sql$')
for script in $scripts; do
  name=$(basename "$script" .sql)
  if [ -n "$only" ] && ! echo ",$only," | grep -q ",$name,"; then continue; fi
  echo "== $name ($clients clients, ${seconds}s, $mode)"
  # --max-tries: ScalarDB resolves write conflicts by aborting (SQLSTATE 40001), PostgreSQL by
  # blocking; retrying keeps the two comparable
  pgbench -n -r -M "$mode" -c "$clients" -j "$clients" -T "$seconds" --max-tries 3 \
    -D ncust="$ncust" -D nitems="$nitems" -D norders="$norders" \
    -l --log-prefix="$dir/$name" -f "$script" "$conn" > "$dir/$name.txt" 2> "$dir/$name.err" || {
      echo "   pgbench failed, see $dir/$name.err"; cat "$dir/$name.err" | tail -3; continue; }
  grep -E "^(tps|latency average|number of failed)" "$dir/$name.txt" | sed 's/^/   /'
done
python3 report.py summarize "$dir" > "results/$label.tsv"
echo "wrote results/$label.tsv"
