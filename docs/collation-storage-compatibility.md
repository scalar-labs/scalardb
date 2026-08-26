# Storage collation compatibility for `scalar.db.collation`

This document records how each ScalarDB-supported storage orders text, and whether the
`scalar.db.collation` setting (`BINARY` — ScalarDB's default when the property is absent — or
`ICU` with `scalar.db.collation.icu.{locale,strength,rules}`) can be configured to match it.

- **Supported-storage list:** ScalarDB 3.18
  ([requirements](https://scalardb.scalar-labs.com/docs/latest/requirements/#databases)).
- **Researched:** July 2026, against each vendor's official documentation (sources at the end).
- **ICU engine:** ScalarDB Core bundles **ICU4J 77.1** (fixed; see `build.gradle`). This version is
  central to the ICU-match discussion below.

## Where the setting applies (read this first)

`scalar.db.collation` governs **only the comparisons ScalarDB performs itself on the JVM** — never
ordering it delegates to the backend. That defines what "match the storage collation" means per
backend:

- **Relational / NewSQL (all JDBC-based):** scans, `ORDER BY`, and range filtering are pushed down
  to the backend as native SQL, governed by the backend's own collation — the setting does **not**
  touch them. The only in-memory comparison for these backends is the **Consensus Commit snapshot
  scan-after-write range check** (SERIALIZABLE). "Matching" here means keeping that overlap/validation
  check consistent with what the backend's SQL scan returns.
- **Cassandra / DynamoDB / Cosmos DB:** these reject cross-partition scans *with ordering*, so
  ScalarDB never sorts their rows in memory; the setting governs **in-memory conjunction range
  filtering** (`FilterableScanner`) plus the **snapshot** check. These storages accept only
  `BINARY` — an `ICU` configuration is rejected at startup (see the table below).
- **Object storage (S3 / Azure Blob / GCS):** ScalarDB orders records **entirely in memory** (scan
  sort + range filter + snapshot). There is no server-side record collation, and record identity
  is byte-exact (objects and records are keyed by the raw key text), so only `BINARY` is
  supported — an `ICU` configuration is rejected at startup.

## Compatibility matrix

Legend: ✅ exact match · 🟡 ICU best-effort (not byte-exact) · ⚠️ exact except an edge case · ❌ not matchable.

### Byte-order-native backends — `BINARY` is an exact match

| Storage | Native string order | Configurable? | Match |
|---|---|---|---|
| Apache Cassandra 5.0–3.0 | `UTF8Type` = unsigned UTF-8 bytes | No | `BINARY` ✅ |
| Amazon DynamoDB | UTF-8 bytes (documented) | No | `BINARY` ✅ |
| Azure Cosmos DB for NoSQL | byte / code-point (docs only guarantee "ascending") | No | `BINARY` ✅ (undocumented risk for astral/surrogate chars) |
| Google Cloud Spanner (PG dialect) | Unicode code-point (= UTF-8 byte order); `COLLATE` unsupported in PG dialect | No | `BINARY` ✅ |
| SQLite 3 | default `BINARY` (`memcmp` of UTF-8) | `NOCASE`/`RTRIM`/custom only | `BINARY` ✅ |

ScalarDB **rejects `scalar.db.collation=ICU` at startup** on these storages, since the storage type
alone proves the order (the Cassandra, DynamoDB, and Cosmos DB adapters; SQLite and Spanner through
JDBC engine detection). A PG-dialect Spanner reached through the PostgreSQL driver is
indistinguishable from PostgreSQL and is not rejected. See ADR-9 in
[collation-adr.md](collation-adr.md).

### Configurable relational backends — depends on the collation the DB uses

Each rating below describes how well a backend's collation can align with `BINARY` or `ICU`
ordering. It does not say whether the collation integration tests can run there: hosting them
also needs a way to apply a collation to the tables the tests create, which Db2 and YugabyteDB
lack despite their 🟡 ratings.

| Storage | Byte-order option → `BINARY` | UCA / linguistic option → `ICU` |
|---|---|---|
| MySQL 8.4/8.0, Aurora MySQL v3 | `utf8mb4_0900_bin` (NO PAD) → ✅ · legacy `utf8mb4_bin` (PAD SPACE) → ⚠️ trailing spaces | default `utf8mb4_0900_ai_ci` = UCA **9.0.0** + CLDR 30 → 🟡 |
| MariaDB 11.4 | `utf8mb4_nopad_bin` (NO PAD) → ✅ · `utf8mb4_bin` → ⚠️ trailing spaces | 11.4+ default `utf8mb4_uca1400_ai_ci` = UCA **14.0.0** → 🟡 (verified by the collation integration tests at ICU `PRIMARY`) |
| MariaDB 10.11, Aurora MySQL v2 | `utf8mb4_bin` → ⚠️ trailing spaces | default `utf8mb4_general_ci` = **non-UCA legacy** → ❌ |
| TiDB 8.5–6.5 | default `utf8mb4_bin` (new framework **trims** trailing spaces) → ⚠️ | `utf8mb4_0900_ai_ci` (7.4+) / `unicode_ci` if configured → 🟡 (TiDB rejects converting the collation of indexed columns, so a collation must be chosen at table creation — which the collation tests' namespace-default mechanism does; covered by the collation integration tests on 7.4+, which also guard against a cluster
bootstrapped with `new_collations_enabled_on_first_bootstrap=false`, where every collation
compares binary while `information_schema` still reports it correctly) |
| PostgreSQL 17–13, Aurora PG, AlloyDB | `C` / `POSIX` / builtin `C.UTF-8` → ✅ (AlloyDB default is `C.UTF-8`) | ICU provider → 🟡 (must match server's ICU version; a nondeterministic ICU collation at primary strength is verified by the collation integration tests on PostgreSQL 17 and AlloyDB) · **glibc** locale e.g. `en_US.UTF-8` → ❌ (glibc ≠ ICU, version drift) |
| YugabyteDB 2 (YSQL) | database collation **must be `C`** → ✅ | per-column ICU → 🟡 (**cannot host the collation tests**: YSQL supports only deterministic collations, databases can use only `C`, and column collation cannot be altered after creation, so a deterministic collation cannot deliver case-insensitive equality) |
| Oracle 23ai/21c/19c | `NLS_SORT=BINARY` (common default) → ✅ | `UCA1210_*` on 21c and later, `UCA0700_*` on 19c which offers no UCA1210 family → 🟡 (covered by the collation integration tests, which derive the name per version and need `MAX_STRING_SIZE=EXTENDED` on the database) · `GENERIC_M` / monolingual (non-UCA) → ❌ |
| SQL Server 2022–2017 | `*_BIN2` (pure code-point) → ✅ | Windows/`SQL_*` collations = proprietary NLS tables, non-UCA → ❌ in general · `*_CI_AI` (e.g. `Latin1_General_100_CI_AI`) is **practically compatible for basic-Latin data** with ICU `PRIMARY` → 🟡 (expansions such as ß/ss, ligatures, and non-Latin scripts still diverge and stay unaligned) |
| IBM Db2 12.1/11.5 | `IDENTITY` → ✅ | `CLDR2701`/`CLDR181` = **ICU/CLDR-based** → 🟡 (closest linguistic case — Db2 uses ICU internally; **cannot host the collation tests**: the collating sequence is fixed at `CREATE DATABASE ... COLLATE USING` with no `ALTER DATABASE` path, no per-column `COLLATE`, and no session override, and being database-wide it would change every existing Db2 test. Db2 also makes `LIKE` collation-aware, which is context for ADR-10) · `SYSTEM` language-aware → ❌ |

### Object storage — `BINARY` only

| Storage | Note |
|---|---|
| Amazon S3, Azure Blob Storage, Google Cloud Storage | No server-side record collation — ScalarDB defines the order in memory — but record identity is byte-exact: partition objects are named by the raw partition-key text and records are keyed by the raw concatenated key text, so the adapter cannot provide the one-record-per-collation-class uniqueness and collation-aware point reads that `ICU` key identity requires. Only `BINARY` is supported; `ICU` is rejected at startup (ADR-9 in [collation-adr.md](collation-adr.md)). |

## Why the ICU version matters (and when it doesn't)

The ICU/CLDR version is the **dominant factor** in how closely `ICU` mode matches a storage — **but
only for the `ICU`-vs-UCA-collation case (the 🟡 rows).** It is irrelevant everywhere else.

- **`BINARY` mode does not use ICU at all.** Every ✅ row is a pure unsigned-UTF-8-byte comparison, so
  the match is exact and completely independent of any ICU version. This is the recommended path for
  the majority of deployments.
- **UCA-based storage collations (🟡) pin a specific UCA/CLDR snapshot:** MySQL `utf8mb4_0900_*` = UCA
  **9.0.0** (Unicode 9.0), MariaDB `uca1400` = UCA **14.0.0** (Unicode 14.0), older MySQL/TiDB
  `unicode_ci` = UCA 4.0.0, `unicode_520_ci` = UCA 5.2.0. ScalarDB's ICU4J **77.1** ships a much newer
  snapshot (Unicode 16 era). Because DUCET weights, script reordering, and locale tailorings change
  between UCA/CLDR versions, ICU4J 77 will differ subtly from these frozen versions even at the same
  locale and strength. So `ICU` is **best-effort, not byte-exact** — closer when the versions are
  closer, and never guaranteed identical. PostgreSQL's ICU provider carries the same warning: its
  ordering "may change if PostgreSQL is built with a different version of ICU," so matching it exactly
  would require the JVM and the server to use the *same* ICU version.
- **Non-UCA linguistic collations (❌) cannot be matched at any ICU version:** glibc locales
  (`en_US.UTF-8`), Oracle `GENERIC_M`, SQL Server Windows/`SQL_*` collations, and MySQL/MariaDB
  `general_ci` use proprietary or libc tables that are not the Unicode Collation Algorithm. No ICU
  version reproduces them; ICU only crudely approximates.

**Practical consequence:** ScalarDB pins one ICU4J version for everyone, so an operator cannot freely
choose an ICU version to line up with, say, MySQL's UCA 9.0.0. Upgrading ScalarDB's bundled ICU4J
changes the ICU-mode ordering and could move it closer to or further from a given backend. The exact
match is therefore only reliably available through `BINARY` against a byte-order storage collation;
`ICU` is an approximation whose quality tracks the UCA/CLDR-version distance.

## Caveats

- **PAD SPACE / trailing spaces.** Legacy `utf8mb4_bin` (MySQL/MariaDB) is PAD SPACE and TiDB's default
  `utf8mb4_bin` trims trailing spaces before comparison; ScalarDB `BINARY` does neither. These match
  except when values differ only by trailing spaces.
- **Equality and key identity follow the collation.** A configured `scalar.db.collation` makes
  ScalarDB's own in-memory `=`/`!=` collation-aware, and under `ICU` the Consensus Commit
  transaction layer's key identity is collation-canonical as well (same best-effort/version caveats
  as the ordering match in this matrix — `BINARY` is byte-exact, `ICU` is best-effort). Physical
  keying is never changed: stored bytes stay as written, and uniqueness remains the backend's job —
  which is why `ICU` key identity requires a backend whose key collation matches, covering both
  uniqueness and recovery point-read resolution (see the key-identity section in `collation.md`).
- **Runtime validation is minimal.** ScalarDB rejects `ICU` at startup only on the structurally
  binary-only storages above. For every configurable backend it does not check the collation
  against the backend; matching it is the operator's responsibility. The one per-operation
  rejection is `LIKE`/`NOT_LIKE` inside a transaction under `ICU`, which no collation can make
  agree with the backend (ADR-10 in [collation-adr.md](collation-adr.md)).
- **Pattern matching is not part of the match.** The rows in this matrix describe ordering and
  equality. A backend's collation may also govern its own `LIKE` — MySQL and MariaDB defaults,
  SQL Server defaults, TiDB, and Oracle under `NLS_COMP=LINGUISTIC` all do — while `BINARY` keeps
  ScalarDB's own pattern matching byte-exact, so a ✅ row does not imply pattern-matching
  agreement. SQLite is the sharpest case: `BINARY` matches its ordering exactly, but its `LIKE` is
  ASCII-case-insensitive by default regardless of column collation.

## Recommendations

1. **Prefer `BINARY`, and set the storage to a byte-order collation** where you control it
   (Oracle `BINARY`, SQL Server `*_BIN2`, Db2 `IDENTITY`, SQLite `BINARY`, MySQL/MariaDB `*_bin` /
   `*_0900_bin`, PostgreSQL/YugabyteDB/AlloyDB `C`/`C.UTF-8`). This yields an exact, version-stable
   match across the entire supported matrix and needs no ICU.
2. **Use `ICU` only when the storage must keep a UCA-based collation** (MySQL `0900_ai_ci`, MariaDB
   `uca1400`, Db2 CLDR, Oracle `UCA1210` or `UCA0700` on 19c, PostgreSQL ICU provider). Configure `.icu.locale` and
   `.icu.strength` to approximate it, and treat the result as best-effort — verify ordering on your
   data, and expect drift if ScalarDB's ICU4J version and the backend's UCA/CLDR version differ.
3. **For non-UCA/proprietary collations** (glibc locales, Oracle `GENERIC_M`, SQL Server Windows/`SQL_*`,
   MySQL/MariaDB `general_ci`): no setting matches them. Migrate the column/DB to a byte-order or UCA
   collation, or accept the documented residual risk.

## Sources

Databases (official docs):
- MySQL: [Unicode character sets](https://dev.mysql.com/doc/refman/8.4/en/charset-unicode-sets.html),
  [collation naming](https://dev.mysql.com/doc/refman/8.4/en/charset-collation-names.html)
- MariaDB: [character set & collation overview](https://mariadb.com/docs/server/reference/data-types/string-data-types/character-sets/character-set-and-collation-overview),
  [MDEV-25829](https://jira.mariadb.org/browse/MDEV-25829), [MDEV-27009](https://jira.mariadb.org/browse/MDEV-27009)
- TiDB: [character set and collation](https://docs.pingcap.com/tidb/stable/character-set-and-collation),
  [collation design doc](https://github.com/pingcap/tidb/blob/master/docs/design/2020-01-24-collations.md)
- PostgreSQL: [collation support](https://www.postgresql.org/docs/current/collation.html),
  [locale support](https://www.postgresql.org/docs/current/locale.html)
- Aurora PostgreSQL: [collations](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/PostgreSQL-Collations.html)
- AlloyDB: [migration overview](https://docs.cloud.google.com/alloydb/docs/migration-overview)
- Spanner: [collation concepts](https://docs.cloud.google.com/spanner/docs/reference/standard-sql/collation-concepts),
  [dialect differences](https://docs.cloud.google.com/spanner/docs/reference/dialect-differences)
- YugabyteDB: [YSQL collations](https://docs.yugabyte.com/preview/explore/ysql-language-features/advanced-features/collations/)
- Oracle: [NLS_SORT](https://docs.oracle.com/en/database/oracle/oracle-database/18/refrn/NLS_SORT.html),
  [linguistic sorting](https://docs.oracle.com/en/database/oracle/oracle-database/26/nlspg/linguistic-sorting-and-matching.html)
- SQL Server: [collation and Unicode support](https://learn.microsoft.com/en-us/sql/relational-databases/collations/collation-and-unicode-support)
- Db2: [UCA-based collations](https://www.ibm.com/docs/en/db2/11.5.x?topic=collation-unicode-algorithm-based-collations),
  [IDENTITY collation](https://www.ibm.com/docs/en/db2/11.5.x?topic=database-identity-collation)
- SQLite: [datatypes / collating sequences](https://www.sqlite.org/datatype3.html)
- Cassandra: [UTF8Type](https://javadoc.io/static/org.apache.cassandra/cassandra-all/2.0.2/org/apache/cassandra/db/marshal/UTF8Type.html)
- DynamoDB: [Query ordering](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html)
- Cosmos DB: [ORDER BY](https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/order-by),
  [indexing overview](https://learn.microsoft.com/en-us/azure/cosmos-db/index-overview)
- Object storage: [S3 ListObjectsV2](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html),
  [Azure List Blobs](https://learn.microsoft.com/en-us/rest/api/storageservices/list-blobs),
  [GCS list objects](https://docs.cloud.google.com/storage/docs/listing-objects)
