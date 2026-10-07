# TEXT Comparison Semantics: Binary Collation for the JDBC Adapter

Status: draft, 2026-10-02. Nothing implemented yet. Related: `scan-read-your-writes-design.md` (open question 1).

## Problem

ScalarDB never says how TEXT values compare: whether `'a'` equals `'A'`, and whether `'B'` sorts before `'a'`. Different parts of the system currently answer differently:

- **Core's own checks are binary.** The Java-side evaluation uses `String` natural order (`TextColumn.compareTo`), which is case- and accent-sensitive. This covers `ScalarDbUtils.columnsMatchAnyOfConjunctions`: the post-read filter in `CrudHandler` (line 355), Get read-your-writes in `Snapshot.mergeResult`, and validation re-scans in `Snapshot.getNextResult`. It also covers `FilterableScanner` (Cassandra, Cosmos DB, DynamoDB, object storage) and the object-storage `ClusteringKeyComparator`.
- **Cassandra, DynamoDB and object storage store TEXT keys in binary order.**
- **The JDBC adapter sets no collation at all.** There is no `COLLATE` or charset in any `RdbEngine*`. TEXT columns are created as `TEXT`, `VARCHAR(n)`, `LONGTEXT`, `VARCHAR2(n)` and so on. Equality, ordering and uniqueness therefore follow whatever the database or server default is.

When the backend's default is not binary, the same predicate means different things depending on whether the database or Java evaluates it. On case- and accent-insensitive defaults (MySQL, MariaDB, SQL Server), primary keys and secondary indexes stop meaning what ScalarDB thinks they mean.

### Observed on MySQL 8.0.44 (default `utf8mb4_0900_ai_ci`)

Tables were created through ScalarDB, using the PostgreSQL frontend's DDL, which calls `DistributedTransactionAdmin`. Every TEXT column was created with `utf8mb4_0900_ai_ci`, including key columns (`varchar(128)`), before-image columns and `tx_id`.

| # | Operation (Consensus Commit) | Binary semantics expect | Observed |
|---|---|---|---|
| 1 | Insert key `'a'`, then insert key `'A'` | Two records | The second insert fails: `DB-CORE-20013 The record being prepared already exists` |
| 2 | Get key `'A'` (only `'a'` exists) | Not found | Returns the record with key `'a'`. Get `'á'` also returns `'a'`. |
| 3 | Update key `'A'` | Nothing updated | **Record `'a'` updated** |
| 4 | Delete key `'Á'` | Nothing deleted | **Record `'a'` deleted** |
| 5 | ScanAll `name = 'alice'` over alice, ALICE | 1 row | 1 row: MySQL returns both, and the Java re-check drops `ALICE` |
| 6 | ScanAll `name < 'b'` over alice, ALICE, Bob, bob | alice, ALICE, Bob | alice, ALICE. **Bob is missing**: MySQL never returns it, so the Java re-check can't add it |
| 7 | Scan with index `name = 'alice'` over alice, ALICE, alicé | 1 row | **3 rows.** The index value is not re-checked in Java. Compare #5: the same predicate gives a different answer depending on whether the column is indexed |
| 8 | Clustering keys `'b'` and `'B'` in one partition | Two records | `DB-CORE-20013`, so they can't coexist |

Items 3 and 4 are data-integrity bugs: a write addressed to one key modifies a different record.

### Observed on PostgreSQL 18 (database collation `ja_JP.UTF-8`)

PostgreSQL's default collations are deterministic, so equality is exact and keys are fine. Ordering and ranges follow the locale:
- **Range predicates:** `name < 'a'` returned 0 rows when evaluated by the database, but 24 under binary order (uppercase sorts before lowercase).
- **Ordering:** scans ordered by a TEXT clustering key, and ScanAll ordered by TEXT, come back in locale order. Code that merges or sorts in Java (the PostgreSQL frontend, or the planned scan read-your-writes merge) then disagrees with storage.

### Other backends (not tested here; vendor defaults)

| Backend | Typical default | Expected effect |
|---|---|---|
| MariaDB, TiDB | `*_ci` (MariaDB), `utf8mb4_bin` (TiDB) | MariaDB: same as MySQL. TiDB: binary, but PAD SPACE (see trailing spaces below). |
| SQL Server | `SQL_Latin1_General_CP1_CI_AS` | Same as MySQL. Equality also ignores trailing spaces under any collation. TEXT is `VARCHAR(8000)` (a code page, not Unicode) unless a `_UTF8` collation is used. |
| Oracle | Comparison follows `NLS_COMP`, ordering `NLS_SORT` (BINARY for English, linguistic for some `NLS_LANGUAGE`s) | Usually binary; can become linguistic depending on session settings. |
| Db2 | Fixed at `CREATE DATABASE` (`IDENTITY`, which is binary, by default) | Usually binary. |
| SQLite, Spanner | Binary | None. |

## Decision

**TEXT comparison in ScalarDB is binary: by Unicode code point, case-sensitive and accent-sensitive, with no padding.** It is the same on every storage.

Why binary:
- It's what core already evaluates in Java, and what Cassandra, DynamoDB and object storage already do.
- It's the only ordering every backend can provide.
- It gives keys an exact identity: a key is its code points, nothing else. Linguistic ordering depends on locale and library version (glibc/ICU upgrades silently reorder indexes); binary does not.
- Users who want locale ordering for presentation can sort in the application. The PostgreSQL frontend could support `ORDER BY name COLLATE "<locale>"` in memory with `java.text.Collator`.

### Java side

`TextColumn.compareTo` uses `String.compareTo`, which is UTF-16 code unit order. It differs from code point order (and from UTF-8 byte order) only when comparing supplementary characters (U+10000 and up, e.g. emoji) against U+E000–U+FFFF. Change it to compare by code point, so Java agrees with `C`, `_bin` and `BIN2` collations. `ScalarDbUtils` comparisons go through it.

## Design: JDBC adapter

### New tables

`JdbcAdmin` creates every TEXT column with a binary collation. This covers user columns, key columns, index columns and before-image columns. The before-image columns matter because `prepareScanForStorage` turns conditions into SQL on `before_*` columns, so they must compare the same way. It also covers the transaction metadata columns, for simplicity; they hold UUIDs and are unaffected.

The type strings come from `getDataTypeForEngine`, `getDataTypeForKey` and `getDataTypeForSecondaryIndex` (`JdbcAdmin.getVendorDbColumnType`). The collation clause belongs there, or in a new `RdbEngineStrategy` hook appended to TEXT column definitions:

| Engine | Clause | Notes |
|---|---|---|
| PostgreSQL | `COLLATE "C"` | Byte order = code point order for UTF8 databases. Deterministic. |
| MySQL 8.0+ | `CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin` | NO PAD. `utf8mb4_bin` is PAD SPACE (`'a' = 'a '`), so it is not the right choice. |
| MySQL 5.7, MariaDB | `utf8mb4_bin` (MariaDB 10.2+: `utf8mb4_nopad_bin`) | 5.7 has no NO PAD binary collation, so trailing-space equality remains there. |
| TiDB | `utf8mb4_0900_bin` (v7.4+), else `utf8mb4_bin` | Same PAD SPACE caveat. |
| SQL Server | `COLLATE Latin1_General_100_BIN2_UTF8` (2019+) | Also makes `VARCHAR` store UTF-8. Before 2019: `_BIN2` with `NVARCHAR` (a type change; out of scope). Trailing spaces are ignored in `=` regardless of collation. |
| Oracle | Session `NLS_COMP=BINARY`, `NLS_SORT=BINARY` at connection init | Column-level `COLLATE BINARY` needs `MAX_STRING_SIZE=EXTENDED`. The session setting is simpler and covers ORDER BY. |
| Db2 | None; check at startup | Collation is per database. Warn if it is not `IDENTITY`. |
| SQLite, Spanner | None | Already binary. |

### Existing tables

Changing the default only affects tables created afterwards. For existing tables:

1. **Detect.** When table metadata is loaded, or in `checkTable`/`repairTable`, read each TEXT column's collation:
   - PostgreSQL: `pg_attribute.attcollation`, falling back to `pg_database.datcollate` when it is the database default.
   - MySQL/MariaDB/TiDB: `information_schema.columns.collation_name`.
   - SQL Server: `sys.columns.collation_name`.
   
   Log a warning naming the table and columns when a key, index or ordered column is non-binary.
2. **Migrate on request.** `repairTable` (or an `upgrade` step) can rewrite the columns:
   - PostgreSQL: `ALTER COLUMN ... TYPE text COLLATE "C"` rebuilds the dependent indexes, with no table rewrite for `TEXT`.
   - MySQL: `MODIFY COLUMN ... COLLATE utf8mb4_0900_bin` rebuilds the table.
   
   Going from case-insensitive to binary only splits equivalence classes, never merges them, so it cannot create duplicate keys and is always safe for data. It does change query results for applications that relied on case-insensitive matching, which is why it isn't automatic.
3. **Imported tables** (`importTable`) belong to the user. Warn, don't alter.

### Opt-out

Do we need a setting to keep the database's collation, e.g. `scalar.db.jdbc.text_collation=binary|database`? Possible users are those who also query ScalarDB tables directly with SQL and rely on locale behavior. Proposal: no setting at first. Existing tables already behave as "database" until migrated, and a setting would make ScalarDB's semantics backend-dependent by choice. Add one only if someone asks.

## Residual differences after this change

- **Trailing spaces:** on SQL Server always, and on MySQL 5.7, MariaDB without `nopad` and TiDB before `0900_bin`, `'a'` and `'a '` are equal for `=` and for key uniqueness. Document this. Java re-checks keep condition results correct; keys differing only by trailing spaces still collide.
- **SQL Server before 2019:** `VARCHAR` is not Unicode. That is a separate, existing issue.

## Effects elsewhere

- **PostgreSQL frontend:** text pushdown becomes consistent with in-memory evaluation. Its in-memory comparison should also switch to code points. This resolves difftest bug #10.
- **Scan read-your-writes:** `Column` natural order becomes a valid storage-order comparator for TEXT on every adapter, which removes the TEXT fallback in `scan-read-your-writes-design.md`.
- **Index scans:** after this change, a Scan with index returns exactly the binary matches (#7 above). Re-checking the index value in Java is still worth adding as a defensive measure for unmigrated tables. It is cheap: one comparison per row.

## Tests

- **Integration, on every JDBC backend in CI and the non-JDBC adapters:** one table with a TEXT partition key, TEXT clustering key and indexed TEXT column, using the corpus `a`, `A`, `á`, `Á`, `b`, `B`, `ß`, `ss`, `a ` (trailing space), `😀` (U+1F600) and `ｱ` (U+FF71, which UTF-16 order puts after `😀` but code point order puts before), and `''` (empty). Assert:
  - all keys coexist (except the documented trailing-space cases);
  - Get, update and delete touch exactly one record;
  - equality, range, LIKE and index conditions give the binary answer;
  - partition scans and ScanAll with TEXT ordering return code-point order, identical across adapters.
- **Admin:** a table created with a non-binary collation is reported by the check, and `repairTable` converts it without losing rows.
- **Unit:** `TextColumn.compareTo` with supplementary characters.

## Open questions

1. Make binary the default for new tables in the next minor release, or behind a flag for one release first? It changes behavior only for new tables, so making it the default seems acceptable.
2. Should `repairTable` convert collations, or should a separate explicit command do it, given that it changes results for applications relying on case-insensitivity?
3. Oracle: is the session-level `NLS_*` setting acceptable, given that it also affects user SQL run on the same pooled connections? ScalarDB owns its pool, so probably yes.
