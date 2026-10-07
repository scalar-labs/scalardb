#!/usr/bin/env python3
"""Writes data.sql: 10,000 customers, 10,000 items and 10,000 orders as multi-row INSERTs.

  python3 gen_data.py [--rows 10000] [--out data.sql]
"""
import argparse
import datetime
import random

p = argparse.ArgumentParser()
p.add_argument("--rows", type=int, default=10000, help="rows per table")
p.add_argument("--batch", type=int, default=500, help="rows per INSERT statement")
p.add_argument("--out", default="data.sql")
args = p.parse_args()
rng = random.Random(42)

REGIONS = ["Tokyo", "Osaka", "Nagoya", "Fukuoka", "Sapporo", "Sendai", "Hiroshima", "Kyoto"]
FIRST = ["Aiko", "Haruto", "Mei", "Ren", "Sakura", "Sora", "Yui", "Yuto", "Hina", "Kaito"]
LAST = ["Sato", "Suzuki", "Takahashi", "Tanaka", "Ito", "Watanabe", "Yamamoto", "Nakamura", "Kobayashi", "Kato"]
CATEGORIES = ["books", "music", "games", "kitchen", "garden", "sports", "toys", "tools", "office", "pets"]
NOUNS = ["lamp", "mug", "chair", "kettle", "ball", "puzzle", "notebook", "wrench", "leash", "poster"]
STATUSES = ["new", "paid", "shipped", "shipped", "shipped", "cancelled"]


def batches(rows):
    for i in range(0, len(rows), args.batch):
        yield rows[i : i + args.batch]


with open(args.out, "w") as out:
    customers = [
        f"({i}, '{rng.choice(FIRST)} {rng.choice(LAST)}', '{rng.choice(REGIONS)}', {rng.randint(1, 3)}, {rng.randint(0, 100000) / 100})"
        for i in range(1, args.rows + 1)
    ]
    for b in batches(customers):
        out.write("INSERT INTO customers (id, name, region, tier, balance) VALUES\n  " + ",\n  ".join(b) + ";\n")
    prices = {}
    items = []
    for i in range(1, args.rows + 1):
        prices[i] = rng.randint(100, 20000) / 100
        items.append(f"({i}, '{rng.choice(CATEGORIES)} {rng.choice(NOUNS)} #{i}', '{rng.choice(CATEGORIES)}', {prices[i]})")
    for b in batches(items):
        out.write("INSERT INTO items (id, name, category, price) VALUES\n  " + ",\n  ".join(b) + ";\n")
    next_order = {}
    orders = []
    start = datetime.datetime(2026, 1, 1)
    for _ in range(args.rows):
        c = rng.randint(1, args.rows)
        next_order[c] = next_order.get(c, 0) + 1
        item = rng.randint(1, args.rows)
        quantity = rng.randint(1, 5)
        when = start + datetime.timedelta(minutes=rng.randint(0, 270 * 24 * 60))
        orders.append(
            f"({c}, {next_order[c]}, {item}, {quantity}, {round(prices[item] * quantity, 2)}, '{rng.choice(STATUSES)}', '{when:%Y-%m-%d %H:%M:%S}')"
        )
    orders.sort(key=lambda r: tuple(int(x) for x in r[1:].split(",")[:2]))
    for b in batches(orders):
        out.write(
            "INSERT INTO orders (customer_id, order_id, item_id, quantity, amount, status, ordered_at) VALUES\n  "
            + ",\n  ".join(b)
            + ";\n"
        )
print(f"wrote {args.out}: {args.rows} customers, {args.rows} items, {args.rows} orders")
