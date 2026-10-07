#!/usr/bin/env python3
"""Writes the benchmark data as multi-row INSERT statements that psql can load into either stack.

The data is deterministic: customer i has region i % 1000 and tier i % 5; item i has code
1000000 + 7 * i and category "cat" + str(i % 20); every customer has orders 1..ORDERS_PER_CUSTOMER
with a random item, amount and status. The pgbench scripts and expected.tsv rely on these rules.
"""
import argparse
import random

p = argparse.ArgumentParser()
p.add_argument("--customers", type=int, default=20000)
p.add_argument("--items", type=int, default=10000)
p.add_argument("--orders", type=int, default=10, help="orders per customer")
p.add_argument("--batch", type=int, default=500, help="rows per INSERT statement")
p.add_argument("--out", default="data.sql")
args = p.parse_args()
rng = random.Random(42)
STATUSES = ["new", "paid", "shipped"]


def batches(rows):
    for i in range(0, len(rows), args.batch):
        yield rows[i : i + args.batch]


with open(args.out, "w") as out:
    rows = [f"({i}, 'cust{i}', {i % 1000}, {i % 5}, {rng.randint(0, 100000) / 100})" for i in range(1, args.customers + 1)]
    for b in batches(rows):
        out.write("INSERT INTO customers (id, name, region, tier, balance) VALUES " + ", ".join(b) + ";\n")
    rows = [f"({i}, {1000000 + 7 * i}, 'item{i}', 'cat{i % 20}', {rng.randint(100, 99900) / 100})" for i in range(1, args.items + 1)]
    for b in batches(rows):
        out.write("INSERT INTO items (id, code, name, category, price) VALUES " + ", ".join(b) + ";\n")
    rows = []
    for c in range(1, args.customers + 1):
        for o in range(1, args.orders + 1):
            rows.append(
                f"({c}, {o}, {rng.randint(1, args.items)}, {rng.randint(100, 50000) / 100},"
                f" '{rng.choice(STATUSES)}', {1700000000 + rng.randint(0, 10000000)})"
            )
    for b in batches(rows):
        out.write("INSERT INTO orders (customer_id, order_id, item_id, amount, status, created) VALUES " + ", ".join(b) + ";\n")
print(f"wrote {args.out}: {args.customers} customers, {args.items} items, {args.customers * args.orders} orders")
