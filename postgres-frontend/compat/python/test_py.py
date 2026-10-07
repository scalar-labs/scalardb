import datetime, traceback
import psycopg
DSN = "host=localhost port=15444 dbname=orm user=postgres"
def step(name, fn):
    try:
        r = fn(); print("ok  ", name, "" if r is None else str(r)[:200])
    except Exception as e:
        print("FAIL", name, type(e).__name__ + ": " + str(e).splitlines()[0][:200])
conn = psycopg.connect(DSN, autocommit=True)
print("psycopg server_version", conn.info.parameter_status("server_version"))
step("drop/create", lambda: (conn.execute("DROP TABLE IF EXISTS py_items"), conn.execute("CREATE TABLE py_items (id int PRIMARY KEY, name text, price double precision, created timestamp)")) and None)
def inserts():
    with conn.cursor() as cur:
        cur.executemany("INSERT INTO py_items (id, name, price, created) VALUES (%s, %s, %s, %s)",
            [(1, 'a', 1.5, datetime.datetime(2024, 1, 1)), (2, 'b', 2.5, datetime.datetime(2024, 6, 1, 12, 30)), (3, 'c', None, None)])
        return cur.rowcount
step("executemany", inserts)
step("any list param", lambda: conn.execute("SELECT id, name, price, created FROM py_items WHERE id = ANY(%s) ORDER BY id", ([1, 3],)).fetchall())
step("in tuple", lambda: conn.execute("SELECT id FROM py_items WHERE id IN (%s, %s) ORDER BY id", (1, 2)).fetchall())
step("aggregates", lambda: conn.execute("SELECT count(*), avg(price), max(created) FROM py_items").fetchone())
def tx():
    with conn.transaction():
        conn.execute("UPDATE py_items SET price = price * 2 WHERE id = %s", (1,))
        conn.execute("DELETE FROM py_items WHERE id = %s", (2,))
    return conn.execute("SELECT id, price FROM py_items ORDER BY id").fetchall()
step("transaction", tx)
def prepared():
    for i in range(6):
        conn.execute("SELECT name FROM py_items WHERE id = %s", (1,)).fetchone()
    return "6 executions (auto-prepared after 5)"
step("auto-prepared", prepared)
step("binary types", lambda: conn.execute("SELECT %s::int, %s::bigint, %s::float8, %s::bool, %s::date, %s::timestamptz, %s::numeric, %s::bytea", (1, 2**40, 1.5, True, datetime.date(2024, 2, 29), datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc), __import__('decimal').Decimal('12.34'), b'\x01\x02')).fetchone())
conn.close()
from sqlalchemy import create_engine, inspect, text, Column, Integer, String, Float, select
from sqlalchemy.orm import declarative_base, Session
engine = create_engine("postgresql+psycopg://postgres@localhost:15444/orm")
insp = inspect(engine)
step("sa tables", lambda: insp.get_table_names())
step("sa columns", lambda: [(c['name'], str(c['type']), c['nullable']) for c in insp.get_columns('py_items')])
step("sa pk", lambda: insp.get_pk_constraint('py_items'))
step("sa indexes", lambda: insp.get_indexes('py_items'))
step("sa fks", lambda: insp.get_foreign_keys('py_items'))
Base = declarative_base()
class Item(Base):
    __tablename__ = 'py_items'
    id = Column(Integer, primary_key=True); name = Column(String); price = Column(Float)
def orm():
    out = []
    with Session(engine) as s:
        s.add(Item(id=10, name='orm', price=9.5)); s.commit()
        out.append([(i.id, i.name) for i in s.execute(select(Item).where(Item.id.in_([1, 10])).order_by(Item.id)).scalars()])
        item = s.get(Item, 10); item.price = 1.0; s.commit()
        out.append(s.get(Item, 10).price)
        s.delete(s.get(Item, 10)); s.commit()
        out.append(s.execute(text("SELECT count(*) FROM py_items")).scalar())
    return out
step("sa orm crud", orm)
def create_all():
    from sqlalchemy import Table, MetaData, Column, Integer, String, DateTime, Boolean
    md = MetaData()
    t = Table('py_widgets', md, Column('id', Integer, primary_key=True), Column('name', String(50)), Column('active', Boolean), Column('at', DateTime))
    md.drop_all(engine); md.create_all(engine)
    with engine.begin() as c:
        c.execute(t.insert().values(id=1, name='w', active=True, at=datetime.datetime(2024, 1, 1)))
        return c.execute(select(t)).fetchall()
step("sa create_all + insert", create_all)
