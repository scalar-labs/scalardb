# Core Work Surfaced by the PostgreSQL Frontend

Status: 2026-10-04. A list, not a design. Each item is a change to ScalarDB core (or its adapters) that the
frontend work exposed and that the frontend cannot fix on its own. The two larger items already have design
documents at the repository root; the rest are described here far enough to decide whether to do them.

| # | Item | Who it hurts | Size | State |
|---|---|---|---|---|
| 1 | Scan after write in one transaction | every ORM, any multi-statement transaction | large | design written |
| 2 | TEXT comparison semantics (collation) | JDBC backends with non-binary defaults; keys silently collide | medium | being implemented in core: PRs in flight, merging to master soon (2026-10-07) |
| 3 | Table metadata cache invalidation | any application that runs DDL while serving | small | workaround in the frontend |
| 4 | DECIMAL type | money and any exact numeric column | large | discussed, no design |
| 5 | Reader starvation under SERIALIZABLE | long reads next to frequent writers | medium | measured, no design |
| 6 | Index names | tooling that drops indexes by name | small | noted |

Suggested order: 1, with 3 as a small warm-up PR beside it; then 2; 4 and 5 start as design notes.

## 1. Scan after write in one transaction

**Problem.** Inside a Consensus Commit transaction, a scan whose range contains a record the same transaction
has written or deleted fails with `CONSENSUS_COMMIT_SCANNING_ALREADY_WRITTEN_OR_DELETED_DATA_NOT_ALLOWED`
(DB-CORE-10106). A Get in the same situation works: it overlays the buffered write on the stored record.
SQL users expect read-your-writes for every read, so `UPDATE ... WHERE dept = 3; SELECT ... WHERE dept = 3`
in one transaction fails, as does `INSERT; SELECT count(*)`.

**Evidence.** 11 of the 31 difftest write cases inside `BEGIN`; Sequelize's transaction step; SQLAlchemy's
`create_all` plus insert plus select; the frontend's own UPDATE and DELETE with a non-key WHERE after an
earlier write in the same range. TPC-C is unaffected because it reads before it writes.

**Proposal.** Merge the transaction's write set and delete set into the scan stream the way Get already merges
them: skip storage rows whose key the transaction wrote, emit the merged image where that row would have been,
drop deleted keys, apply the limit to the merged output, keep validation on the raw rows consumed. Phase one
covers updates and deletes in place and all unordered scans; phase two places inserted rows by a comparator,
which for TEXT keys depends on item 2. Full design: `scan-read-your-writes-design.md`.

**Pointers.** `Snapshot.verifyNoOverlap` (the refusal), `Snapshot.mergeResult` (what Get does),
`CrudHandler.scan` and the two scanner classes, `Snapshot.validateScanResults`.

**Open.** Blind writes with implicit pre-read disabled (read a base image, or keep the error); whether the base
Get goes through `read()` and joins the get set; TEXT ordering guarantee (item 2).

## 2. TEXT comparison semantics (collation)

**Status 2026-10-07.** Collation support is being added to core in separate PRs that are about to merge. Once
they land, the frontend's in-memory text comparison (Java code-point order today) and the TEXT rule in
item 1's design should follow whatever those PRs define, and the difftest `text_cmp_case` expectation gets
re-checked against it.

**Problem.** ScalarDB never says how TEXT compares. Core's Java-side checks, Cassandra, DynamoDB and object
storage are binary. The JDBC adapter sets no `COLLATE`, so equality, ordering and uniqueness of TEXT keys follow
the database default. On MySQL 8 (`utf8mb4_0900_ai_ci`) and SQL Server defaults, `'a'`, `'A'` and `'Á'` are one
key to the database and three keys to ScalarDB.

**Evidence.** On MySQL 8.0.44 through the frontend, an UPDATE by key `'Á'` modified the record `'a'`. On
PostgreSQL with `ja_JP.UTF-8`, a predicate pushed down to storage and the same predicate evaluated in Java
ordered rows differently (difftest `text_cmp_case`).

**Proposal.** Declare TEXT comparison binary (by code point, case- and accent-sensitive, no padding) on every
storage. In the JDBC adapter create key, clustering and indexed TEXT columns with a binary collation
(`COLLATE "C"`, `utf8mb4_bin`, `BIN2`, Oracle `NLS_*`), with an opt-out and a path for existing tables. Change
`TextColumn.compareTo` from UTF-16 code unit order to code point order so Java agrees with the databases. Full
design: `text-collation-design.md`.

**Open.** Default for new tables in the next minor release, or behind a flag first; whether `repairTable`
converts existing tables; whether Oracle's session-level `NLS_*` setting is acceptable on pooled connections.

## 3. Table metadata cache invalidation

**Problem.** `TableMetadataManager` caches table metadata for `scalar.db.metadata.cache_expiration_time_secs`
(default 60) and nothing refreshes it after DDL. The admin that runs the DDL (`ConsensusCommitAdmin` over
`JdbcAdmin`) and the caches that serve operations (`JdbcDatabase`'s `TableMetadataManager`,
`ConsensusCommitManager`'s `TransactionTableMetadataManager`) are separate objects created by the same
`TransactionFactory`, and no public API reaches the caches:
`ConsensusCommitManager.getTableMetadataManager()` is protected. After DROP plus CREATE with other columns, or
ADD COLUMN followed by an insert, writes fail with DB-CORE-10017 ("The column value is not properly specified")
and reads miss the new column until the entry expires.

**Evidence.** Two ORM test suites recreating the same table one after the other through one frontend process;
an ActiveRecord migration pattern (add a column, then backfill) fails the same way.

**Workaround in the frontend.** It sets the expiration to 1 second unless the properties file sets it, and a
session sleeps that long after DROP TABLE, DROP SCHEMA, DROP INDEX, ALTER TABLE and CREATE INDEX. This costs a
second per DDL statement and is only a workaround.

**Proposal.** Either of these, the first being cleaner:
- Wire the admin to the caches inside `TransactionFactory` (and `StorageFactory`), so DDL run through the same
  factory invalidates the affected table's entry in every cache it owns. No new API; applications that run DDL
  and operations from one factory are fixed without changes.
- Add a public method, for example `DistributedTransactionManager.invalidateTableMetadata(namespace, table)`,
  for applications whose DDL runs elsewhere in the same process.

DDL from another process (the schema loader, another node) stays governed by the expiration, as today.

**Pointers.** `common/TableMetadataManager.java`, `storage/jdbc/JdbcDatabase.java` (builds its own cache
over a private `JdbcAdmin`), `transaction/consensuscommit/ConsensusCommitManager.java` (the protected getter),
`config/DatabaseConfig.java` (`METADATA_CACHE_EXPIRATION_TIME_SECS`, default 60).

## 4. DECIMAL type

**Problem.** `DataType` has no exact numeric type. The frontend maps SQL `NUMERIC` and `DECIMAL` columns to
DOUBLE on CREATE TABLE and emulates PostgreSQL's numeric arithmetic in memory with `BigDecimal`, so every value
passes through a double on its way to storage: 0.1 + 0.2 is exact in a SELECT but not once stored. Money
columns, the most common use of NUMERIC, cannot round-trip exactly.

**Evidence.** Difftest `avg_double` and the NUMERIC discussion of 2026-10-03; every ORM in the compatibility
runs declares at least one decimal column.

**Proposal.** A `DECIMAL(precision, scale)` data type with a `BigDecimal` column class, stored natively on JDBC
backends and as a string or scaled integer where the backend has no decimal (DynamoDB has one; Cassandra has
`decimal`; Cosmos DB and object storage would need an encoding that preserves order for keys). It touches the
API, every adapter, the schema loader, the cluster protocol and the data loader, which is why it is large. A
design note should settle the ordering encoding for key columns and whether precision is enforced.

**Open.** Whether DECIMAL may be a key column (ordering encoding), and how the frontend migrates columns it has
already created as DOUBLE.

## 5. Reader starvation under SERIALIZABLE

**Problem.** Under SERIALIZABLE, Consensus Commit validates every read at commit by re-reading it. A
transaction that reads many rows while other transactions keep changing some of them is aborted at commit
almost every time, and its retries fare no better. The result is correct but a steady writer load can lock a
reader out indefinitely. PostgreSQL's SSI lets a read-only transaction commit regardless, and its read-write
transactions abort only on an actual dangerous structure.

**Evidence.** `postgres-frontend/difftest/Concurrency.java` (8 threads, transfers between 10 accounts): a
10-row reader next to 6 writers committed between 1 and 10 percent of its attempts under SERIALIZABLE, for both
the read-only and the read-write reader. SNAPSHOT showed the expected skew instead.

**Proposal, to be designed.** Options in rising order of ambition: let a read-only transaction
(`beginReadOnly`) commit without validation when the snapshot it read is itself consistent; validate only
anti-dependencies that can form a cycle; or a bounded retry inside core with a consistent snapshot. The first
needs an argument that a read-only transaction over a per-record snapshot can still observe a non-serializable
state, which is exactly what the current validation prevents, so this is a design note before any code.

**Pointers.** `Snapshot.validateScanResults` and the SERIALIZABLE validation path in `ConsensusCommit`,
`ConsensusCommitManager.beginReadOnly`.

## 6. Index names

**Problem.** `Admin.createIndex(namespace, table, columnName, options)` takes a column, and adapters derive the
index name (`JdbcAdmin.getIndexName`: `table_column_idx`). SQL tooling names indexes and later drops them by
that name; the frontend has to map `DROP INDEX name` back to a column and cannot when the name is the user's.

**Evidence.** A probe `CREATE INDEX IF NOT EXISTS probe_i ON emp (mgr_id)` created `emp_mgr_id_idx`, and
`DROP INDEX probe_i` failed, leaving the index on a shared table.

**Proposal.** Record the user's index name in table metadata (an option on `createIndex`, surfaced by
`TableMetadata`) so it can be listed and dropped by name; the physical name can stay generated. Low priority:
the frontend can keep a name-to-column map of its own for indexes it created.

## Not core

- Client-to-cluster round trips, forwarding only on the transaction-coordinator path, and the in-node
  PostgreSQL listener belong to scalardb-cluster: `postgres-frontend-integration-design.md`.
- INTERVAL, arrays, JSON and session time zones stay frontend-only emulation by design; none needs a storage
  type.
- TIME precision (microseconds) and TIMESTAMPTZ precision (milliseconds) are documented limits of the existing
  types, not bugs.
