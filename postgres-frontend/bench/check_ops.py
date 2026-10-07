#!/usr/bin/env python3
"""Checks, for every script, how many ScalarDB reads one execution issues against expected.tsv.

Runs EXPLAIN ANALYZE through psql on the frontend inside a transaction that is rolled back, so
writes leave no trace. Fails (exit 1) when any count differs: that is a planner regression.
Usage: check_ops.py <psql connection options or URI for the frontend>
"""
import os
import re
import subprocess
import sys

here = os.path.dirname(os.path.abspath(__file__))
conn = sys.argv[1:]
if not conn:
    sys.exit(__doc__)
failures = 0
print(f"{'script':<14} {'expected':>8} {'actual':>8}  result")
for line in open(os.path.join(here, "expected.tsv")):
    if line.startswith("#") or not line.strip():
        continue
    script, expected, params = line.rstrip("\n").split("\t")
    values = dict(kv.split("=") for kv in params.split(",")) if params else {}
    sql = open(os.path.join(here, "scripts", script + ".sql")).read()
    sql = "\n".join(l for l in sql.splitlines() if not l.startswith("\\"))
    sql = re.sub(r"(?<!:):([A-Za-z_]\w*)", lambda m: values.get(m.group(1), m.group(0)), sql)
    statements = [s.strip() for s in sql.split(";") if s.strip() and s.strip().upper() not in ("BEGIN", "BEGIN READ ONLY", "COMMIT")]
    actual = 0
    plans = []
    for statement in statements:
        cmd = ["psql", *conn, "-X", "-A", "-t", "-v", "ON_ERROR_STOP=1", "-c", "BEGIN", "-c", "EXPLAIN ANALYZE " + statement, "-c", "ROLLBACK"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"{script:<14} psql failed: {result.stderr.strip()}")
            failures += 1
            break
        plans.append(result.stdout)
        actual += sum(int(n) for n in re.findall(r"ScalarDB reads=(\d+)", result.stdout))
    else:
        ok = actual == int(expected)
        print(f"{script:<14} {expected:>8} {actual:>8}  {'ok' if ok else 'MISMATCH'}")
        if not ok:
            failures += 1
            print("".join(plans))
sys.exit(1 if failures else 0)
