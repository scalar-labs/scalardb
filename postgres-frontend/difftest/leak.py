import re, subprocess, sys, os
env = dict(os.environ, LANG='C', LC_ALL='C')
def idle():
    return subprocess.run(['psql','-X','-Atq','-h','localhost','-U','postgres','-p','15440','-d','sdb','-c',
        "select count(*) from pg_stat_activity where datname='sdb' and state like 'idle in%'"], capture_output=True, text=True).stdout.strip()
base = idle()
for chunk in re.split(r'^-- @', open(sys.argv[1]).read(), flags=re.M)[1:]:
    head, _, body = chunk.partition('\n')
    subprocess.run(['psql','-X','-h','localhost','-U','postgres','-p',os.environ.get('FEPORT', '15441'),'-d','sdb'], input=body, capture_output=True, text=True, env=env, timeout=60)
    now = idle()
    if now != base: print('LEAK after', head.split()[0], base, '->', now); base = now
