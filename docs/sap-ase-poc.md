# SAP ASE (Sybase) JDBC adapter: proof of concept

An `RdbEngineStrategy` implementation for SAP ASE (Adaptive Server Enterprise, formerly Sybase ASE)
in the JDBC adapter. It targets **ScalarDB running in front of data that is already in an existing
ASE**, so it asks the server for as little as possible: no `sa_role` for normal operation, no server
logins created by ScalarDB, no device space, and no database-level setting changed behind your back.

**Status: verified against a live server; the integration suite runs but is not green.** ASE 16.0
SP03 PL02 (2 KB pages), reached over jConnect 16.0.

- `tools/ase-preflight/AsePreflight.java` — 18 checks, all passing, each running the SQL the engine
  actually emits.
- `AseSmokeTest` — ScalarDB itself: DDL with a secondary index, put/get/scan/delete, an upsert,
  dropping and recreating an index, a committed Consensus Commit transaction read back, and
  importing a table that already existed and reading a pre-existing row through ScalarDB.
- `AseExistingDatabaseIntegrationTest` — transactions on top of a database created entirely outside
  ScalarDB, with the existing tables left unchanged. See *Running in front of an existing database*.
- `integrationTestJdbc` — 6741 tests, 26 failing, in about 5 minutes. Every remaining failure is one
  of three ASE behaviours rather than an adapter defect; see *The integration suite*.

Getting that last one to zero is the bar for calling this supported, and three of the 26 cannot be
reached without changing what a namespace maps to.

## What was added

| File | |
| --- | --- |
| `core/src/main/java/com/scalar/db/storage/jdbc/RdbEngineSybase.java` | The engine: dialect, types, nullability, error codes, import |
| `core/src/main/java/com/scalar/db/storage/jdbc/RdbEngineTimeTypeSybase.java` | Date and time values as ASE-readable text |
| `core/src/test/java/com/scalar/db/storage/jdbc/RdbEngineSybaseTest.java` | 17 tests |
| `core/src/test/java/com/scalar/db/storage/jdbc/RdbEngineTimeTypeSybaseTest.java` | 4 tests |
| `core/src/integration-test/java/com/scalar/db/storage/jdbc/AseSmokeTest.java` | The end-to-end run against a live server |
| `tools/ase-preflight/AsePreflight.java` | Standalone checker for any ASE you can reach |

Changed, all of it inert for the other engines:

- `RdbEngineFactory` dispatches `jdbc:sybase:` to the new engine.
- `RdbEngineStrategy` gains two default methods: `getConnectionInitSql()`, which `JdbcUtils` passes
  to HikariCP's `connectionInitSql` (ASE needs `SET QUOTED_IDENTIFIER ON` per connection), and
  `getNullableColumnClause()`, which returns an empty string everywhere except ASE.
- `JdbcAdmin`, `TableMetadataService` and `VirtualTableMetadataService` append that clause at the six
  places where they declare a nullable column.
- `CoreError` gains `0304`, for a namespace whose ASE user does not exist.
- `core/build.gradle` takes `-PjconnectJar=<path>` to put the driver on the integration test
  classpath, since jConnect cannot be a normal dependency.

## What the live server changed

Six things were wrong or unnecessary in the version written from documentation alone. Every one was
found by running against ASE.

**ASE 16 does support `MERGE`, and it still cannot be used.** The version written from
documentation assumed no `MERGE` and emitted a two-statement `UPDATE`/`IF @@rowcount = 0`/`INSERT`
upsert. The live server showed both branches of a parameterised `MERGE` working, so
`UpdateThenInsertQuery` was deleted for the shared `MergeQuery` -- and that held right up until the
first table ScalarDB had not created itself. ASE compiles the `WHEN NOT MATCHED THEN INSERT` branch
even for a row that is there, so a `MERGE` fails on any table with a NOT NULL column the upsert is
not writing. `UpdateThenInsertQuery` is back, for a reason the first version never knew. See *The
upsert had to stop being a MERGE*. The duplicate-key race it reintroduces is handled by treating a
duplicate key as a conflict, which for this upsert shape it always is.

**`sp_rename` takes neither an owner qualifier nor quoted identifiers, and fails silently.**
`sp_rename '"n1"."t1"."v1"', ...` returns "You do not own a table, column, index or partition of that
name"; `sp_rename 'n1.t1.v1', ...` returns "The combination of input parameters is invalid". Both
arrive as *messages with a non-zero return status*, not as errors, so the driver raises nothing and
ScalarDB would have recorded a rename that never happened. Renames now run as the owner:

```sql
setuser 'n1'
exec sp_rename 't1.v1', 'v1_new', 'column'
setuser
```

**`DROP INDEX` cannot be owner-qualified either** — `DROP INDEX "n1"."t1".idx` gets "DROP INDEX does
not allow specifying the database name as a prefix to the object name", because ASE reads three
parts as `database.owner.object`. It uses the same `setuser` form.

**An index name must not be quoted.** `create index "idx" on t (c)` creates an index whose name is
`"idx"`, quote characters included, which nothing can subsequently drop or rename. The engine emits
the index name bare; ScalarDB generates names that need no quoting.

**jConnect reports `bigdatetime` as type code 11 and `bigtime` as 10**, neither of which
`java.sql.Types` defines, so `JdbcUtils#getJdbcType` maps them to `OTHER`. Import matched on
`TIMESTAMP` and `TIME` and would have rejected every such column. It now matches on the type name
under `OTHER`. Plain `datetime` and `smalldatetime` do arrive as `TIMESTAMP`, and `time` as `TIME`.

**Reading an imported `bigdatetime` overflowed ScalarDB's TIMESTAMP.** ASE keeps microseconds,
ScalarDB TIMESTAMP holds milliseconds, and `TimestampColumn.ofStrict` rejected a real legacy value.
Reads now truncate to each type's contract — milliseconds for TIMESTAMP and TIMESTAMPTZ,
microseconds for TIME. A table ScalarDB created only ever holds milliseconds; an imported column can
hold more, and the alternative is for the read to fail.

Confirmed as written: error codes 1205/2601/2714/3701, `LOCK DATAROWS`, the explicit nullability
scheme, `SELECT TOP`, the date and time text format including a pre-1582 date round trip with
microseconds preserved, the `RAISERROR` namespace guard, the `sysobjects`/`sysusers` catalog
queries, and the 600-byte index row limit that the 128-byte key column size exists to stay under.

## The decisions worth knowing about

**A namespace is an ASE owner, and ScalarDB never creates one.** A ScalarDB table is `owner.table`,
the same shape as a SQL Server schema, which is what makes existing tables addressable. ASE differs
in that an owner is a database user and a database user needs a server-level login, so creating a
namespace only *checks* that the user exists and dropping one does nothing. See
[Why the namespace is an owner](#why-the-namespace-is-an-owner-that-scalardb-does-not-create).

**`BOOLEAN` is a `tinyint`, not a `bit`.** An ASE `bit` column cannot be null, and every non-key
ScalarDB column can be, including the before-image columns Consensus Commit writes.

**Time types avoid the ASE defaults.** `datetime` resolves to 1/300 of a second and `time` to a
millisecond, so TIMESTAMP and TIME use `bigdatetime` and `bigtime`. ASE has no time zone aware type,
so TIMESTAMPTZ is a `bigdatetime` holding UTC; an existing column that already holds UTC can be
imported as one by overriding its type. The ASE type named `timestamp` is a row version rather than a
point in time: it is never written, and importing it is refused. Values are written as text, because
`java.sql.Date` and `java.sql.Timestamp` shift dates before October 15, 1582 — the round trip of the
year 1000 is one of the preflight checks.

**Tables ScalarDB creates use `LOCK DATAROWS`.** The ASE default is allpages locking (confirmed on
the test server), whose page level locks would make Consensus Commit transactions block each other
over rows they never touched. An imported table keeps the locking scheme it already has, which is
worth checking.

**The driver is named, not linked.** `getDriverClassName()` returns the string
`com.sybase.jdbc4.jdbc.SybDriver` rather than a class literal, because jConnect ships with ASE under
the SAP license and is not on Maven Central. jTDS predates `bigdatetime` and `bigtime`, so it is not
an alternative.

Variable length columns are sized for a 2 KB page server: `UNIVARCHAR(900)` for a value,
`UNIVARCHAR(128)` for a key or secondary index column. Both are constants in the engine. They are
counted in characters and ASE stores two bytes per character, so 900 characters is 1800 bytes and
fits the 1964-byte row limit. See the univarchar note under the integration suite for why TEXT is
not a `varchar`.

## Nullability

ASE columns are `NOT NULL` unless the database sets `allow nulls by default` (off on the test
server), the reverse of what ScalarDB's generated DDL assumes, and a primary key column must be
explicitly `NOT NULL`. Turning the database option on would work but changes DDL semantics for every
other application in that database.

Keys carry `NOT NULL` from `getDataTypeForKey`, which the engine owns. Nullable columns could not be
handled the same way, because `TableMetadataService` appends a literal `" NOT NULL"` to the result of
`getDataTypeForEngine` for some metadata columns and leaves others implicit — an engine folding
`" NULL"` into its type strings would emit `TINYINT NULL NOT NULL`. Hence `getNullableColumnClause()`,
appended by the six callers that declare a nullable column, empty for every other engine.

## Prerequisites

A database administrator has to do three things once.

1. **A database user for every namespace.** For imported data these already exist — they are the
   owners of the tables you are putting ScalarDB in front of. ScalarDB additionally needs one for its
   system namespace (`scalar.db.system_namespace_name`, default `scalardb`) and, for Consensus
   Commit, one for the coordinator namespace (default `coordinator`). A missing one is reported as
   `DB-CORE-10304` when the namespace is created, not as a confusing failure later.

   ```sql
   use master
   create login scalardb with password <password>
   use <database>
   sp_adduser scalardb
   ```

2. **`select into` on the database.** Without it, `ALTER TABLE` with data copy fails with "Neither
   the 'select into' nor the 'full logging for alter table' database options are enabled". ScalarDB
   needs it to change a column's type when creating a secondary index, and to drop a column.

   ```sql
   sp_dboption <database>, "select into", true
   ```

3. **Permission to create tables under the namespace's owner**: `create any table` where granular
   permissions are enabled, `sa_role` where they are not (they were not, on the test server).

## Running it

```properties
scalar.db.storage=jdbc
scalar.db.contact_points=jdbc:sybase:Tds:<host>:<port>/<database>
scalar.db.username=<user>
scalar.db.password=<password>
scalar.db.system_namespace_name=<an existing ASE user>
```

`jconn4.jar` (or `jConnect40*.jar`) has to be on the classpath. HikariCP logs "Driver does not
support get/set network timeout for connections" at startup; jConnect predates that JDBC method and
the pool carries on without it.

The preflight answers every ASE-specific question in one run, against any server you can reach:

```
java -cp /path/to/jConnect40.jar tools/ase-preflight/AsePreflight.java \
    "jdbc:sybase:Tds:localhost:5000/scalardb" sa <password>
```

The end-to-end test runs ScalarDB itself:

```
./gradlew integrationTestJdbc --tests "*AseSmokeTest*" \
    -PjconnectJar=/path/to/jConnect40.jar \
    -Dscalardb.jdbc.url=jdbc:sybase:Tds:localhost:5000/scalardb \
    -Dscalardb.jdbc.username=sa -Dscalardb.jdbc.password=<password> \
    -Dscalardb.ase.namespace=<an existing ASE user>
```

## Importing existing tables

`importTable` works and was exercised end to end. The engine maps the ASE types jConnect reports,
verified against a table holding every one of them, and refuses those with no ScalarDB counterpart:
`decimal`, `numeric`, `money`, `smallmoney`, `unsigned bigint`, and the row-version `timestamp`.
`char`, `varchar`, `unichar`, `univarchar` and `text` become TEXT; `binary`, `varbinary` and `image`
become BLOB; `tinyint` and `smallint` widen to INT; `unsigned int` widens to BIGINT; `datetime`,
`smalldatetime` and `bigdatetime` become TIMESTAMP, or TIMESTAMPTZ if you override the type and the
column holds UTC; `time` and `bigtime` become TIME.

Two things to weigh, neither ASE-specific but both sharper on a legacy schema:

- **The default path alters your tables.** It adds the transaction metadata columns *and a
  before-image column for each non-key column* (`ConsensusCommitAdmin:505-520`), roughly doubling the
  column count. ASE `alter table ... add` on a large table is not free, and it changes a schema the
  existing application also uses.
- **Transaction metadata decoupling leaves them alone**, keeping the metadata in a side table joined
  through a virtual table (`ConsensusCommitAdmin:458-501`). It requires
  `isConsistentVirtualTableReadGuaranteed`, and this engine declares the minimum isolation for that
  as SERIALIZABLE (ASE level 3), a heavy read mode. That is still a conservative guess: SQL Server
  guarantees the same at repeatable read, and if ASE matches, decoupled import gets much cheaper.
  **This is the one significant thing the preflight does not settle.**

## Running in front of an existing database

The question this was built for: can ScalarDB run on top of an ASE that is already in use, without
changing the tables in it? Yes, using transaction metadata decoupling. `AseExistingDatabaseIntegrationTest`
proves it against a database created entirely outside ScalarDB.

The setup is an `erp` database with a `sales` owner and two tables that plain SQL created and
populated, as an existing application would have left them. Importing `sales.customers` with
`transaction-metadata-decoupling=true` leaves it alone and adds objects beside it:

| Object | What it is |
| --- | --- |
| `sales.customers` | the existing table, **unchanged**: same columns, same nullability, same indexes |
| `sales.customers_tx_metadata` | new, holds the Consensus Commit metadata that a plain import would have added as `tx_` columns on the table itself |
| `sales.customers_scalardb` | new view, `customers LEFT OUTER JOIN customers_tx_metadata`. Reads and writes go through this |
| `scalardb.metadata`, `scalardb.namespaces`, `scalardb.virtual_tables` | ScalarDB's own catalog |
| `coordinator.state` | the Consensus Commit coordinator |

The test asserts the existing table's columns and indexes are identical before and after, commits a
transaction that updates a row, checks the existing application still reads the committed value
through plain SQL, checks an aborted transaction leaves the row alone, and checks that a row the
existing application inserts directly is visible to ScalarDB, which the LEFT OUTER JOIN allows.

Two things have to be true of the database before this works.

**The isolation level has to be REPEATABLE READ**, `scalar.db.jdbc.isolation_level=REPEATABLE_READ`.
See below for why, and why it is not SERIALIZABLE.

**Every column type has to map.** `numeric` and `decimal` are the ones that bite; see below.

### Why the isolation level has to be REPEATABLE READ

A virtual table read is one SELECT through a join, and Consensus Commit needs the application row
and its transaction metadata to come from the same instant. It reads `tx_id`, `tx_state`,
`tx_version` and the before-image alongside the data, and decides from them whether a record is
committed, whether to recover it, and whether a write conflicts. Give it data from one version and
metadata from another and those decisions are made on a record that never existed. With the
undecoupled layout this cannot happen, because everything is one row read atomically. Decoupling is
what introduces the requirement.

ScalarDB does not decide the level itself: it asks the engine, through
`getMinimumIsolationLevelForConsistentVirtualTableRead()`, and refuses decoupling if the configured
level is lower. So `scalar.db.jdbc.isolation_level=REPEATABLE_READ` is mandatory, and level 1 is
refused.

**Level 1 is genuinely not enough.** SAP's *Performance and Tuning Series: Locking and Concurrency
Control* for 16.0 says the shared lock is released "when the row qualification completes" at level
1. The two halves of the join are read at different instants, so a writer can commit in between and
the reader gets a torn pair.

**Level 2 is enough**, on three independent grounds:

- **The documentation.** The same guide's isolation level table says level 2 "Holds shared locks
  until the transaction completes". So once the reader has the data row, no writer can change it.
- **The write ordering.** A writer always updates the data table before the transaction metadata
  table, in one JDBC transaction — `JdbcDatabase.dividePutForSourceTables` builds the pair in that
  order. Combined with the above, a writer cannot reach the metadata row without first getting past
  the data row the reader holds.
- **Measured on the server.** A reader holds a row inside an open transaction while a second
  connection tries to update it. At level 1 the update succeeds, so the lock was released. At level
  2 it fails with error 12205, so the lock is held. True for both locking schemes.

This was originally set to SERIALIZABLE as a conservative guess by analogy with SQL Server, and it
was wrong to leave it there: the guess cost a level of locking that ASE's own documentation says is
unnecessary.

#### The allpages complication, which turns out to be safe

An existing table is usually **allpages-locked** — that is the ASE server default, so a table
created without a `lock` clause gets it. On the fixture, `sales.customers` and `sales.orders` are
allpages while every table ScalarDB creates is datarows, so a decoupled read joins one of each.

Level 2 is not supported on an allpages-locked table. That sounds like a hole, and is not one:

> If transaction level 2 is set in a session, and an allpages-locked table is included in a query,
> isolation level 3 is also applied on the allpages-locked tables. Transaction level 2 is used on
> all data-only-locked tables in the session.

ASE escalates such a table to level 3 rather than dropping it to level 1, so an existing table is
read at least as strictly as asked. Asking for level 2 is never weaker than asking for level 3. The
practical effect is that on an existing allpages schema the setting buys nothing, and on the
datarows tables ScalarDB creates it buys the lower level.

### numeric and decimal

ScalarDB's type system has no decimal, so a `numeric` or `decimal` column has no exact counterpart.
What is possible depends on the scale, and on what jConnect will let ScalarDB bind. Measured against
the server:

| Binding | Into `numeric` | Notes |
| --- | --- | --- |
| `setLong`, `setInt` | works | exact |
| `setDouble` | works | ASE rounds to the column's scale |
| `setString` | **rejected** | "Implicit conversion from datatype 'VARCHAR' to 'NUMERIC' is not allowed" |

That last row rules out the mapping that would otherwise be attractive. Reading a decimal as text is
lossless, so TEXT looks like the natural home for one, but ScalarDB binds TEXT with `setString` and
ASE refuses it, so a TEXT mapping would read correctly and fail on every write. Rejecting the column
is better than that.

So the engine maps them by scale, and jConnect reports the precision as the column size and the
scale as the digits:

| Column | Maps to | Asked for? |
| --- | --- | --- |
| `numeric(p,0)`, p <= 9 | INT, exact | no |
| `numeric(p,0)`, p <= 18 | BIGINT, exact | no |
| `numeric(p,0)`, p > 18 | refused, `DB-CORE-10308` | -- |
| any scale > 0 | DOUBLE | **yes**, an explicit DOUBLE override |
| any scale > 0, no override | refused, `DB-CORE-10307` | -- |

A scaled column is refused unless the caller passes `DataType.DOUBLE` for it in
`importTable`'s `overrideColumnsType`, because DOUBLE stops being exact past 15 significant digits
and that is not a loss to choose on someone's behalf. `money` and `smallmoney` report a scale of 4,
so they need the override too. The `orders` table in the fixture holds both cases down: `amount` is
`numeric(12,2)` and needs the override, `ledger_ref` is `numeric(18,0)` and comes through as a
BIGINT that keeps all 18 digits, which a DOUBLE would not.

Worth saying plainly: DOUBLE on a money column is a real approximation. For a schema where that is
not acceptable, the options are to keep those columns out of ScalarDB, or to change them in the
source database to something exact -- which is a change to the existing database, and so outside
what this was trying to show.

### The upsert had to stop being a MERGE

This is what actually stood in the way, and it only shows up on a table ScalarDB did not create.

ASE 16 has MERGE and it works on a ScalarDB table, whose non-key columns are all nullable. On an
existing table it fails. ASE compiles the `WHEN NOT MATCHED THEN INSERT` branch even for a row that
is there, and rejects the whole statement if that insert would leave a NOT NULL column unset:

```sql
MERGE INTO "sales"."customers" t USING (SELECT 1 AS "customer_id") s ON (t."customer_id" = s."customer_id")
WHEN MATCHED THEN UPDATE SET "credit_limit" = 77000.0
WHEN NOT MATCHED THEN INSERT ("customer_id","credit_limit") VALUES (1, 77000.0)
-->  Msg 233: The column name in table "sales"."customers" does not allow null values.
-->  Msg 233: The column active in table "sales"."customers" does not allow null values.
```

Customer 1 exists, so only the update branch would ever run, and `credit_limit` was left unchanged.
A ScalarDB upsert carries only the columns being written, so on any table with NOT NULL columns of
its own -- which is to say most tables an existing application owns -- every write fails. Other
engines do not do this: PostgreSQL and MySQL only fail if the insert actually runs.

`UpdateThenInsertQuery` replaces it: an UPDATE, then an INSERT guarded by `IF @@rowcount = 0`. ASE
compiles the INSERT only when the UPDATE matched nothing, so updating an existing row never reaches
it, and inserting a genuinely new row still fails if a NOT NULL column has no value, which is
correct. Two consequences:

- **It cannot go in a JDBC batch.** ASE answers one with "Only single DML command without references
  to local or global variables can be executed with homogeneous batch parameters", so
  `JdbcCrudService` runs each of these as its own round trip. The suite went from 5m40 to about 6m,
  so the cost is real but small at this scale. Promoting the decision to a method on `Query` would
  be tidier than the instanceof check it uses today.
- **A duplicate key is now a conflict.** The INSERT is only reached when the UPDATE matched nothing,
  so a duplicate key from it means another transaction took the key in between. `isConflict` reports
  it so the caller retries, rather than surfacing a permanent error. A `PutIfNotExists` that wants
  to see the duplicate is unaffected: it builds a plain INSERT and `ConditionalMutationQuery`
  intercepts the error first.

## The integration suite

`integrationTestJdbc` against the live server: **6741 tests, 6122 passed, 28 failed, 591 skipped**,
in about 5 minutes. The first complete run was 209 failures in 76
minutes.

| Run | Failed | Green classes |
| --- | --- | --- |
| First complete run | 209 | 19 |
| After the scan-lock and truncation fixes | 53 | 31 |
| After alterColumnType, blob/bit index, harness fixes | 32 | 37 |
| After the schema loader fixture | 33 | 38 |
| After the remaining fixture switches | 33 | 39 |
| After TEXT to univarchar | 29 | 41 |
| After the sp_rename guard, the duplicate-schema predicate and server sizing | 26 | 43 |
| After the upsert became an update-then-insert | **28** | **43** |

The last two runs differ only inside the empty string category: the conditional mutation tests draw
random column values and print their seed, so which of them happens to write an empty string moves
between runs. The three causes and their totals are otherwise the same.

The count is not monotonic: several classes were dying at initialization, contributing one failure
each while hiding hundreds of tests. Fixing those raises both the passing count and, sometimes, the
failure count. Passing tests went 4485 to 6124 over the same period.

### What remains, and why

Every remaining failure is one of three ASE behaviours, each confirmed by a direct probe rather than
inferred from the test output.

| Count | Cause | Fixable? |
| --- | --- | --- |
| 11 | ASE rejects the Unicode noncharacters U+FFFF and U+FFFE outright ("Illegal byte sequence encountered in Unicode data"). The tests use `Character.MAX_VALUE`, which is U+FFFF, as the maximum TEXT value | Not without an encoding layer that would make the stored bytes differ from the text, which the "in front of existing data" goal rules out |
| 10 | An empty string is stored as a single space, so `''` and `' '` are indistinguishable. The same applies to an empty blob, which comes back NULL. The count moves between runs with the random test data | No |
| 7 | Namespace names that cannot be an ASE user: over 30 characters, or a reserved word such as `between` | Not under the owner-based mapping; see below |

`sysusers.name` and `syslogins.name` are both `varchar(30)`, so the 30-character cap is a catalog
limit rather than a configuration one. `sp_adduser 'between'` fails with "'between' is not a valid
name" — ASE rejects reserved words as user names even when they are passed as a quoted string
literal. Both are consequences of mapping a namespace onto an object owner. Mapping long or
reserved names onto a generated alias recorded in ScalarDB's `namespaces` table would make these
pass, at the cost of the property that the ASE owner is the namespace name, which is the point of
the owner-based design for existing data.

### What the failing tests turned out to be worth

Four things came out of chasing these down, and three of them were adapter or environment defects
rather than ASE limits. The earlier conclusion that none of the remaining failures was an adapter
defect was wrong.

**TEXT had to be `univarchar`, not `varchar`.** A `varchar` holds the server's own character set, so
on this `iso_1` server it could not store Japanese at all: "Error converting characters into
server's character set". This looked like a property of the server build, and it is not — a
`univarchar` holds Unicode whatever the server was built with, and jConnect handles it. TEXT is now
`univarchar(900)` and a TEXT key or index column is `univarchar(128)`; ASE charges two bytes per
character, so 900 characters is 1800 bytes and still fits the 1964-byte row limit on a 2 KB page.
This fixed the Japanese tests and four others.

**`sp_rename` failing silently defeated an index-rename fallback.** ASE reports a name `sp_rename`
cannot resolve as a message with a non-zero return status, not as an error, so renaming an index
that was not there did nothing and still looked like it worked. `renameTable` renames each index
after the table, and the caller only falls back to the pre-shortening long index name when the first
attempt throws. Because nothing threw, the fallback never ran, the index kept a name derived from
the old table, and a later `dropIndex` failed. `renameIndexSqls` now checks `sysindexes` and raises
error 20001 first, which `isUndefinedIndexError` recognises. The hazard was named in the javadoc on
`renameAsOwner` and still went unnoticed in the code right next to it.

**The 100 MB blob needed server sizing, in three separate places.** Raising the procedure cache from
512 MB to 1.5 GB moved the failure on to "'default' segment is full", and growing the database from
140 MB to 1.16 GB moved it on again to a silent LOG SUSPEND, which presents as a hang rather than an
error. A 2 GB log device fixed it. The blob is inlined into the statement text as a hex literal
because `DYNAMIC_PREPARE=false` (the upsert is a two-statement language batch), so a 100 MB blob
becomes a roughly 200 MB statement, and log space has to be sized for it. A deployment that stores
large blobs on ASE needs the procedure cache, data segment and log segment sized against the largest
blob, not against the data volume.

**One test was asserting something ASE cannot produce.** `isDuplicateSchemaError` creates a schema
twice and expects an error. ASE never creates a namespace, so `createSchemaSqls` only checks that
the owner is there and is idempotent by construction — the same reason MySQL, PostgreSQL and SQLite
are already excluded. The predicate is now `isDuplicateSchemaNotProducible` and covers ASE.

### Adding an engine to this suite

The work is dominated by finding every place the tests switch on engine type. They fail with a bare
`AssertionError` that names neither the engine nor the missing case, so each one costs a full run to
discover. `JdbcSchemaLoaderImportIntegrationTest` alone has four:
`createImportableTable`, `createNonImportableTable`, `getImportableTableOverrideColumnsType` and
`getImportableTableMetadata`. Others live in `JdbcAdminTestUtils`, `JdbcAdminImportTestUtils`, and
the `isIndexOnBlobColumnSupported` / `isColumnTypeConversionToTextNotFullySupported` /
`isConcurrentWriteToRowUnderOpenScanSupported` predicates, which are spelled differently in different
classes: some take `rdbEngine`, others use the static `JdbcEnv` helpers, and
`JdbcDatabaseSecondaryIndexIntegrationTest` uses a type-filter map keyed by engine class instead of a
predicate.

Provisioning namespaces is also a treadmill: coordinator and system namespaces are suffixed per test
class, so each run that unblocks new classes reveals new names to create.

## Two bugs the failing tests uncovered

Chasing the 149 failures in `ConsensusCommitSpecificIntegrationTestWithJdbcDatabaseInHighestIsolation`
found two real defects. The class is now **1431 passed, 0 failed**.

### Silent truncation, which was losing data

The residual `InvalidProtocolBufferException` in `CoordinatorStateAccessor.get` came from Consensus
Commit storing its serialized write set in a BLOB column. On the server:

```sql
-- a 3000 byte value into varbinary(1900)
select 'stored' = datalength(b) from tr_b   -->  1900     -- no error, no warning
-- the same into varchar(1900)
select 'stored' = datalength(c) from tr_c   -->  1900     -- no error, no warning
set string_rtruncation on
insert ...                                  -->  Msg 9502, string data right truncated
```

ASE silently shortens any value that exceeds its column. A transaction with several writes therefore
produced a truncated protobuf that failed to parse on read, and any user TEXT or BLOB value over the
limit would have been quietly corrupted. Two changes:

- **BLOB maps to `image`**, not to a sized `varbinary`. It holds up to 2 GB and is stored off-row,
  which also keeps rows inside ASE's row size limit (1964 bytes on a 2 KB page server).
- **The connection sets `STRING_RTRUNCATION ON`**, so an over-length value raises error 9502 instead
  of being silently shortened.

### A self-deadlock the framework already knew about

The other 144 were not an adapter defect. `ConsensusCommitSpecificIntegrationTestBase` has a hook,
`isConcurrentWriteToRowUnderOpenScanSupported()`, for engines that hold locks for an open scan
cursor: the scan-path recovery tests write the scanned row from inside lazy recovery while their own
scanner is still open, which self-deadlocks on a lock-based engine. MySQL and MariaDB under
SERIALIZABLE, SQL Server and Db2 were already excluded, the comment noting that their "lock wait is
effectively unbounded, so it hangs". SAP ASE is the same kind of engine and has been added to that
list.

This also explains the runs that appeared to stall: the helper the test uses retries forever on a
retriable exception, so with a bounded lock wait the deadlock became an endless retry loop rather
than a failure. The class now completes in about 3 minutes rather than 75.

### Isolation level: a negative result

Before finding the above, the obvious hypothesis was that ASE's level 3 range locks caused the
contention. Overriding `getHighestIsolationLevel()` to REPEATABLE READ produced a byte-for-byte
identical result (1474 passed, 149 failed, 144 lock-wait) and took longer, so the override was
reverted.

That investigation did turn up something important. **ASE's server-wide `lock wait period` defaults
to 2147483647 seconds**, effectively forever. The session-level `SET LOCK WAIT` this engine issues
works in every scenario that could be constructed for it, but does not govern every wait: a `DELETE`
was observed blocked for over twenty minutes with the session setting in place. Setting the server
value is dynamic and needs no reboot:

```sql
sp_configure "lock wait period", 30
```

This belongs in [Prerequisites](#prerequisites): the engine cannot set it on a user's behalf, and
without it contention becomes an unrecoverable hang rather than a retriable error.

## Known limitations

- An identifier that would need quoting cannot be renamed, because `sp_rename` takes bare names.
- Importing a `bigdatetime` truncates sub-millisecond precision on read.
- The virtual-table isolation level is unverified, as above.
- `SYBASE` is deliberately absent from the test-only `RdbEngine` enum: `QueryBuilderTest` and
  `JdbcAdminTest` switch on it with per-engine expected SQL, so adding it means writing expectations
  across roughly 8,000 lines. The standalone test classes cover the engine instead.
- The Unicode noncharacters U+FFFF and U+FFFE cannot be stored in a TEXT column: ASE rejects them
  with "Illegal byte sequence encountered in Unicode data". Every other code point tested round
  trips, U+FFFD included.
- An empty string comes back as a single space, and an empty blob comes back NULL. Neither can be
  distinguished from the value ASE substitutes.
- A namespace has to be a legal ASE user name: at most 30 characters, and not a reserved word.
- Storing a large blob needs the procedure cache, data segment and log segment sized against the
  blob, not against the data volume. An undersized log presents as a hang (LOG SUSPEND), not an
  error.
- Transaction metadata decoupling requires `scalar.db.jdbc.isolation_level=REPEATABLE_READ`.
- A `numeric`/`decimal` column with a scale imports only with an explicit DOUBLE override and is
  then approximate. Without a scale it maps exactly to INT or BIGINT up to 18 digits.
- An upsert is an UPDATE followed by a guarded INSERT rather than a MERGE, so it costs one round
  trip each and cannot be batched. See the existing database section for why.
- The full `integrationTestJdbc` suite is not green; see above.

## Getting a server to test against

There is no official SAP ASE image, and ASE is x86-64 only. On an Apple Silicon Mac, **QEMU cannot
run it** — three community images were tried and all died in ASE's own startup. Rosetta can:

| Image | Result |
| --- | --- |
| `nguoianphu/docker-sybase` (ASE 16.0 SP02, 5.9 GB) | QEMU: segfault after the license check |
| `ifnazar/sybase_15_7` (ASE 15.7, 0.7 GB) | QEMU: segfault after the license check |
| `blieusong/sybase-ase` (ASE 16 Express, 4 KB pages) | QEMU and Rosetta: `SIGSEGV` in `Snap::Validate()` during `dsinit` |
| **`blieusong/ase-server`** (ASE 16 Express, 2 KB pages, 0.25 GB) | **Boots under Rosetta** |

The recipe that works:

1. In Docker Desktop, enable "Use Virtualization framework" and "Use Rosetta for x86_64/amd64
   emulation" (`useVirtualizationFramework` and `useVirtualizationFrameworkRosetta`). Switching the
   VM backend restarts Docker; images and volumes survived here, but back up anything precious.
2. `docker run -d --platform linux/amd64 --name ase -p 5000:5000 -v ase-data:/data blieusong/ase-server sleep infinity`
3. Extract the premade database and **turn kernel async I/O off**, which no emulated host supports:

   ```
   docker exec ase sh -c 'cd / && tar -xzf /tmp/data.tar.gz --no-same-owner'
   docker exec ase sh -c 'sed -i "s|allow sql server async i/o = DEFAULT|allow sql server async i/o = 0|" /opt/sap/ASE-16_0/DB_TEST.cfg'
   ```

   Without it: "Kernel asynchronous I/O not initialized because it is not supported by this host",
   then "It was not possible to create the disk controllers for the server".
4. Start `dataserver` with a real errorlog. The image's `RUN_DB_TEST` points `-e` at `/proc/1/fd/0`,
   which is `/dev/null` in a container with no TTY, so ASE otherwise appears to start and vanishes
   with no diagnostics:

   ```
   docker exec -d ase sh -c '. /opt/sap/SYBASE.sh; exec /opt/sap/ASE-16_0/bin/dataserver \
     -d/data/master.dat -e/tmp/ase.log -c/opt/sap/ASE-16_0/DB_TEST.cfg -M/opt/sap/ASE-16_0 \
     -N/opt/sap/ASE-16_0/sysam/DB_TEST.properties -i/opt/sap -sDB_TEST -T11889'
   ```

   Wait for `Recovery complete.` in `/tmp/ase.log`.
5. The login is `sa` / `sybase`. Create a scratch database on its own device, since the master device
   is small:

   ```sql
   disk init name = "scalardb_dev", physname = "/data/scalardb.dat", size = "80M"
   create database scalardb on scalardb_dev = 60
   ```

Then follow [Prerequisites](#prerequisites) and run the preflight.

## Why the namespace is an owner that ScalarDB does not create

ASE is close enough to SQL Server that this engine reuses `SELECT TOP`, `sp_rename`, the `[`/`]` LIKE
escaping and much of the error code layout. Schemas are where the two diverged: SQL Server 2005
separated the schema from its owner, and ASE never did.

- ASE's `create schema authorization <name>` requires that name to be the **current user's**
  ([reference](https://infocenter.sybase.com/help/topic/com.sybase.infocenter.dc36272.1570/html/commands/X48762.htm)).
  There is no `CREATE SCHEMA foo` that makes a schema named `foo`; a schema in ASE is another word
  for an owner, and an owner is a database user.
- A database user must map to a **server-level login**: `sp_adduser` requires the name to exist in
  `master.dbo.syslogins`
  ([reference](https://help.sap.com/docs/SAP_ASE/29a04b8081884fb5b715fe4aa1ab4ad2/ab53a8d8bc2b1014805e8f366a248b3c.html)).
  ASE has no equivalent of SQL Server's `CREATE USER ... WITHOUT LOGIN`.
- Creating a table under another owner needs `create any table` with granular permissions enabled, or
  `sa_role` without them
  ([reference](https://infocenter.sybase.com/help/topic/com.sybase.infocenter.dc36272.1600/doc/html/san1393050945163.html)).

Having ScalarDB create namespaces would therefore mean creating server logins, with passwords it
invents, in `master`. On an existing server that is not a reasonable thing to ask for, so the owners
are created by a DBA and `createNamespace` compiles to a check.

Mapping a namespace to an ASE *database* instead would give real isolation and per-namespace backup,
at the cost of `sa_role` and device space per namespace. Transactions would still work, since ASE
spans databases within one server. It is a contained change to the same handful of methods.
