#!/usr/bin/env python3
"""Summarizes pgbench runs and compares two stacks.

  report.py summarize <results dir>          -> TSV on stdout (script, tps, avg, p50, p95, p99, max, n, failed)
  report.py compare <a.tsv> <b.tsv>          -> markdown table: a versus b, with the tps ratio
"""
import glob
import os
import re
import sys


def summarize(d):
    print("script\ttps\tavg_ms\tp50_ms\tp95_ms\tp99_ms\tmax_ms\tcount\tfailed\tretried")
    for txt in sorted(glob.glob(os.path.join(d, "*.txt"))):
        name = os.path.basename(txt)[:-4]
        out = open(txt).read()
        tps = re.search(r"^tps = ([\d.]+)", out, re.M)
        failed = re.search(r"^number of failed transactions: (\d+)", out, re.M)
        retried = re.search(r"^number of transactions retried: (\d+)", out, re.M)
        times = []
        for log in glob.glob(os.path.join(d, name + ".*")):
            if log.endswith(".txt") or log.endswith(".err"):
                continue
            for line in open(log):
                parts = line.split()
                if len(parts) > 2 and parts[2].isdigit():
                    times.append(int(parts[2]) / 1000.0)
        times.sort()

        def pct(p):
            return times[min(len(times) - 1, int(p * len(times)))] if times else float("nan")

        avg = sum(times) / len(times) if times else float("nan")
        print(
            f"{name}\t{tps.group(1) if tps else 'nan'}\t{avg:.3f}\t{pct(0.5):.3f}\t{pct(0.95):.3f}\t{pct(0.99):.3f}"
            f"\t{(times[-1] if times else float('nan')):.3f}\t{len(times)}\t{failed.group(1) if failed else 0}"
            f"\t{retried.group(1) if retried else 0}"
        )


def read(path):
    rows = {}
    with open(path) as f:
        header = f.readline().rstrip("\n").split("\t")
        for line in f:
            parts = line.rstrip("\n").split("\t")
            rows[parts[0]] = dict(zip(header, parts))
    return rows


def compare(a, b):
    ra, rb = read(a), read(b)
    la, lb = os.path.basename(a)[:-4], os.path.basename(b)[:-4]
    print(f"| script | {la} tps | {lb} tps | {la}/{lb} | {la} p50 | {lb} p50 | {la} p95 | {lb} p95 |")
    print("|---|---:|---:|---:|---:|---:|---:|---:|")
    for name in sorted(set(ra) | set(rb)):
        x, y = ra.get(name), rb.get(name)
        tx = float(x["tps"]) if x else float("nan")
        ty = float(y["tps"]) if y else float("nan")
        ratio = tx / ty if y and ty else float("nan")
        print(
            f"| {name} | {tx:.0f} | {ty:.0f} | {ratio:.1f}x | {x['p50_ms'] if x else '-'} | {y['p50_ms'] if y else '-'}"
            f" | {x['p95_ms'] if x else '-'} | {y['p95_ms'] if y else '-'} |"
        )


if len(sys.argv) >= 3 and sys.argv[1] == "summarize":
    summarize(sys.argv[2])
elif len(sys.argv) >= 4 and sys.argv[1] == "compare":
    compare(sys.argv[2], sys.argv[3])
else:
    sys.exit(__doc__)
