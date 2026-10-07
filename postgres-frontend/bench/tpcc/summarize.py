#!/usr/bin/env python3
"""Prints BenchBase run results side by side: summarize.py <label>=<results dir>[,<run log>] ...

Reads each directory's *.summary.json (throughput, latency percentiles). With BenchBase's console
output saved to a file, also prints its per-procedure histograms: completed, aborted (the
benchmark's own rollbacks), retried (SQLSTATE 40001) and unexpected SQL errors.
"""
import csv
import glob
import json
import os
import re
import sys

SECTIONS = {
    "Completed Transactions": "completed",
    "Aborted Transactions": "aborted",
    "Rejected Transactions (Server Retry)": "retried",
    "Unexpected SQL Errors": "errors",
}


def histograms(log):
    """{procedure: {section: count}} from the histogram tables at the end of a run log."""
    counts, section = {}, None
    for line in open(log, errors="replace"):
        line = re.sub(r"\x1b\[[0-9;]*m", "", line).rstrip()
        if line.endswith(":") and line[:-1] in SECTIONS:
            section = SECTIONS[line[:-1]]
            continue
        m = re.match(r"\S+\.procedures\.(\w+)/\d+\s+\[\s*(\d+)\]", line)
        if m and section:
            counts.setdefault(m.group(1), {})[section] = int(m.group(2))
    return counts


def latencies(d):
    """{procedure: sorted latencies in ms} of the successful requests in *.raw.csv."""
    files = sorted(glob.glob(os.path.join(d, "*.raw.csv")))
    by = {}
    if files:
        for r in csv.DictReader(open(files[-1])):
            by.setdefault(r["Transaction Name"], []).append(int(r["Latency (microseconds)"]) / 1000)
    return {k: sorted(v) for k, v in by.items()}


rows = []
logs = []
raws = []
for arg in sys.argv[1:]:
    label, d = arg.split("=", 1)
    d, _, log = d.partition(",")
    logs.append((label, histograms(log) if log else {}))
    raws.append((label, latencies(d)))
    files = sorted(glob.glob(os.path.join(d, "*.summary.json")))
    if not files:
        rows.append((label, None))
        continue
    s = json.load(open(files[-1]))
    lat = s.get("Latency Distribution", {})
    rows.append((label, {
        "tps": s.get("Throughput (requests/second)"),
        "goodput": s.get("Goodput (requests/second)"),
        "avg": lat.get("Average Latency (microseconds)"),
        "p50": lat.get("Median Latency (microseconds)"),
        "p95": lat.get("95th Percentile Latency (microseconds)"),
        "p99": lat.get("99th Percentile Latency (microseconds)"),
        "terminals": s.get("terminals"),
        "scale": s.get("scalefactor"),
        "time": s.get("Benchmark Runtime (nanoseconds)"),
    }))
print(f"{'':<12}" + "".join(f"{label:>16}" for label, _ in rows))
def line(name, key, scale=1.0, fmt="{:.0f}"):
    out = f"{name:<12}"
    for _, r in rows:
        v = None if r is None else r.get(key)
        try:
            out += f"{fmt.format(float(v) / scale):>16}"
        except (TypeError, ValueError):
            out += f"{'-':>16}"
    print(out)
line("tps", "tps", 1, "{:.1f}")
line("goodput", "goodput", 1, "{:.1f}")
line("avg ms", "avg", 1000, "{:.2f}")
line("p50 ms", "p50", 1000, "{:.2f}")
line("p95 ms", "p95", 1000, "{:.2f}")
line("p99 ms", "p99", 1000, "{:.2f}")
line("terminals", "terminals")
line("warehouses", "scale")

if any(h for _, h in logs):
    procedures = sorted({p for _, h in logs for p in h}, key=lambda p: min(h.get(p, {}).get("completed", 0) for _, h in logs), reverse=True)
    print()
    print(f"{'procedure':<12}{'':<10}" + "".join(f"{label:>16}" for label, _ in logs))
    for proc in procedures:
        for section in ["completed", "aborted", "retried", "errors"]:
            vals = [h.get(proc, {}).get(section, 0) for _, h in logs]
            if any(vals):
                print(f"{proc:<12}{section:<10}" + "".join(f"{v:>16}" for v in vals))

if any(r for _, r in raws):
    print()
    print(f"{'p50 / p95 ms':<12}" + "".join(f"{label:>16}" for label, _ in raws))
    for proc in ["NewOrder", "Payment", "OrderStatus", "Delivery", "StockLevel"]:
        out = f"{proc:<12}"
        for _, r in raws:
            v = r.get(proc)
            out += f"{(f'{v[len(v) // 2]:.2f} / {v[int(len(v) * 0.95)]:.2f}' if v else '-'):>16}"
        print(out)
