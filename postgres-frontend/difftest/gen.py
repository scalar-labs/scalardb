import random
r = random.Random(42)
def q(v):
    if v is None: return "NULL"
    if isinstance(v, bool): return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)): return repr(v)
    return "'" + str(v).replace("'", "''") + "'"
def ins(t, rows):
    for row in rows:
        print(f"INSERT INTO {t} VALUES ({', '.join(q(v) for v in row)});")
regions = ['north', 'south', 'east', None]
depts = []
for i in range(1, 13):
    depts.append((i, f"dept{i:02d}" if i != 7 else "", regions[i % 4], [None, 1000.5, 2500.0, -300.25, 0.0][i % 5],
                  None if i <= 3 else r.randint(1, 3) if i != 11 else 99))  # 99: dangling parent
ins('dept', depts)
names = ['alice', 'bob', 'carol', 'oneil', 'Dave', 'eve', 'Zed', 'émile', 'bob', 'ALICE']
emps = []
for i in range(1, 81):
    d = None if i % 13 == 0 else (r.randint(1, 10) if i % 17 else 42)  # NULL dept and dangling dept 42; depts 11,12 empty
    emps.append((i, names[i % len(names)] + ('' if i < 10 else str(i % 7)), d,
                 None if i <= 5 else r.randint(1, 5),
                 None if i % 11 == 0 else r.choice([1000, 2000, 2000, 3500, -50, 0, 12345]),
                 None if i % 3 == 0 else round(r.uniform(-10, 100), 2),
                 None if i % 19 == 0 else (i % 2 == 0),
                 f"20{r.randint(10, 24)}-{r.randint(1, 12):02d}-{r.randint(1, 28):02d}",
                 r.choice([None, '', 'x', 'needs review', 'Remote', '%wild_card%'])))
ins('emp', emps)
projs = []
pid = 100
for d in range(1, 11):
    for k in range(r.randint(0, 3)):
        pid += 1
        projs.append((d, pid, r.choice(['alpha', 'beta', 'gamma', None]) , r.choice([None, 10, 5000000000, 0, -7]),
                      f"2023-{r.randint(1, 12):02d}-{r.randint(1, 28):02d} {r.randint(0, 23):02d}:{r.randint(0, 59):02d}:00"))
ins('proj', projs)
seen = set()
rows = []
for _ in range(150):
    e, p = r.randint(1, 85), r.randint(101, pid + 2)  # some dangling emp/proj ids
    if (e, p) in seen: continue
    seen.add((e, p))
    rows.append((e, p, r.choice([None, 0, 5, 10, 40, 40]), r.choice(['dev', 'lead', 'qa', None])))
ins('assign', rows)
# A small table of the column types the generated tables do not use
for row in [
    "(1, '00:00:00', '2024-01-01 00:00:00+00', '\\x00', 0.5)",
    "(2, '09:30:15', '2024-06-15 12:00:00+09', '\\xdeadbeef', 1.25)",
    "(3, '23:59:59.5', '2024-12-31 23:59:59.25+00', '\\x', 1234.5678)",
    "(4, NULL, NULL, NULL, NULL)",
    "(5, '12:00:00', '2024-03-10 02:30:00-05', '\\x7f80', 0.1)",
]:
    print(f"INSERT INTO types VALUES {row};")
