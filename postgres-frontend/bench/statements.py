#!/usr/bin/env python3
"""Summarizes a PostgreSQL log written with log_min_duration_statement=0: how many times each
statement shape ran and its average server-side execution time. Use it to see exactly what SQL
ScalarDB sends per operation. Usage: statements.py <postgresql log> [min count]"""
import collections
import re
import sys

log = open(sys.argv[1], errors="replace").read()
minimum = int(sys.argv[2]) if len(sys.argv) > 2 else 50
pat = re.compile(r"([\d.]+) (?:ms|ミリ秒)\s+(?:[^:\n]*?)[:：] (.*)")
groups = collections.OrderedDict()
for m in pat.finditer(log):
    key = re.sub(r"\s+", " ", m.group(2).strip())
    key = re.sub(r"\$\d+", "$n", key)
    key = re.sub(r"= '[^']*'|= [-\d.]+", "= ?", key)
    groups.setdefault(key, []).append(float(m.group(1)))
print(f"{'count':>7} {'avg ms':>8}  statement")
for key, d in sorted(groups.items(), key=lambda kv: -len(kv[1])):
    if len(d) >= minimum:
        print(f"{len(d):>7} {sum(d) / len(d):>8.3f}  {key[:400]}")
