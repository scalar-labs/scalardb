"""Runs each case in cases*.sql on native PG and the frontend via psql and diffs the outputs.
Case header: `-- @name [ordered]`. Rows inside each result are sorted unless `ordered`."""
import os, re, subprocess, sys
FOOT = re.compile(r'^\((\d+) rows?\)$')
def run(port, db, sql):
    p = subprocess.run(['psql', '-X', '-A', '-h', 'localhost', '-U', 'postgres', '-p', port, '-d', db,
                        '-P', 'null=<NULL>', '-v', 'ON_ERROR_STOP=' + os.environ.get('STOP', '1')], input=sql, capture_output=True, text=True, timeout=60, env=dict(os.environ, LANG='C', LC_ALL='C'))
    return p.stdout, p.stderr.strip()
def num(tok):
    try: return '%.6g' % float(tok)
    except ValueError: return tok
def norm(out, ordered, loose):
    blocks, cur = [], []
    for line in out.splitlines():
        cur.append(line)
        if FOOT.match(line):
            head, rows = cur[0], cur[1:-1]
            if loose:
                rows = ['|'.join(num(t) for t in r.split('|')) for r in rows]
            if loose: head = str(head.count('|'))
            blocks.append([head] + (rows if ordered else sorted(rows)) + [line]); cur = []
        elif len(cur) == 1 and not line.count('|') and re.match(r'^[A-Z ]+( \d+)*$', line):
            blocks.append([line]); cur = []  # command tag
    if cur: blocks.append(cur)
    return blocks
cases = []
for f in sys.argv[1:]:
    for chunk in re.split(r'^-- @', open(f).read(), flags=re.M)[1:]:
        head, _, body = chunk.partition('\n')
        cases.append((head.split()[0], 'ordered' in head.split()[1:], body.strip()))
stats = {}
for name, ordered, sql in cases:
    if os.environ.get('SNAP'):
        sql = sql + '\n' + ''.join(f'SELECT * FROM {t};\n' for t in ('emp', 'dept', 'proj', 'assign', 'types'))
    (no, ne), (fo, fe) = run('15440', os.environ.get('NATIVE', 'native_c'), sql), run(os.environ.get('FEPORT', '15441'), 'sdb', sql)
    if ne and fe: st = 'BOTH_ERR'
    elif fe: st = 'FE_ERR'
    elif ne: st = 'PG_ERR'
    elif norm(no, ordered, False) == norm(fo, ordered, False): st = 'OK'
    elif [b[1:] for b in norm(no, ordered, False)] == [b[1:] for b in norm(fo, ordered, False)]: st = 'NAMES'
    elif norm(no, ordered, True) == norm(fo, ordered, True): st = 'NUM_FMT'
    else: st = 'DIFF'
    if os.environ.get('SNAP'):
        for port, db in (('15440', os.environ.get('NATIVE', 'native_c')), (os.environ.get('FEPORT', '15441'), 'sdb')):
            subprocess.run(['psql', '-X', '-q', '-h', 'localhost', '-U', 'postgres', '-p', port, '-d', db, '-f', 'trunc.sql', '-f', 'data.sql'], check=True, capture_output=True)
    stats[st] = stats.get(st, 0) + 1
    if st != 'OK':
        print(f'=== {st} {name}\n{sql}')
        if st == 'NAMES': print('  pg:', no.splitlines()[0], '\n  fe:', fo.splitlines()[0]); continue
        if st in ('FE_ERR', 'PG_ERR', 'BOTH_ERR'): print(f'  pg: {ne}\n  fe: {fe}')
        else:
            import difflib
            a = [l for b in norm(no, ordered, False) for l in b]; b2 = [l for b in norm(fo, ordered, False) for l in b]
            print('\n'.join(list(difflib.unified_diff(a, b2, 'pg', 'fe', lineterm='', n=1))[:40]))
print(stats, file=sys.stderr)
