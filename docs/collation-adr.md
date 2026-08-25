# Architectural Decision Records: storage-aligned collation

Decision log for the `scalar.db.collation` feature. Each record captures one decision, the
context that forced it, the alternatives considered, and the consequences accepted. The
user-facing description of the feature is in [collation.md](collation.md); the per-backend
research backing these decisions is in
[collation-storage-compatibility.md](collation-storage-compatibility.md),
[collation-equality-backend-behavior.md](collation-equality-backend-behavior.md), and
[collation-phase2-consensus-commit-feasibility.md](collation-phase2-consensus-commit-feasibility.md).

| # | Decision | Status |
|---|---|---|
| [ADR-1](#adr-1) | One global, JVM-side-only collation setting | Accepted |
| [ADR-2](#adr-2) | `BINARY` (unsigned UTF-8 byte order) is the default | Accepted |
| [ADR-3](#adr-3) | ICU4J as the collation engine, version-frozen | Accepted |
| [ADR-4](#adr-4) | Equality follows the collation (no `deterministic` flag) | Accepted (supersedes an implemented flag) |
| [ADR-5](#adr-5) | Collation-canonical key identity in Consensus Commit, implicit under `ICU` | Accepted |
| [ADR-6](#adr-6) | No runtime collation-vs-storage validation | Accepted (narrowed by ADR-9) |
| [ADR-7](#adr-7) | Invalid ICU configuration fails at startup | Accepted |
| [ADR-8](#adr-8) | Integration tests apply the backend collation via namespace defaults and a required-mode capability gate | Accepted |
| [ADR-9](#adr-9) | `ICU` rejected at startup on structurally binary-only storages | Accepted |
| [ADR-10](#adr-10) | `LIKE` rejected in transactions under `ICU` | Accepted |

---

## ADR-1: One global, JVM-side-only collation setting <a name="adr-1"></a>

**Status:** Accepted

### Context

ScalarDB compares text on the JVM with Java's natural `String` order (UTF-16 code-unit
order) in several places it evaluates itself: object-storage scan sort and range filtering,
in-memory conjunction/cross-partition filtering, the Consensus Commit snapshot's
scan-after-write check, and in-memory conditional-mutation predicates. No supported backend
agrees with that order. Byte-order backends (Cassandra, DynamoDB) compare UTF-8 bytes,
which diverges from UTF-16 order above U+FFFF, and UCA-collated backends (MySQL 8 defaults,
PostgreSQL ICU) diverge much further. A range or overlap decision ScalarDB makes in memory
can then contradict what the backend's own scan returns.

### Decision

Introduce one global setting, `scalar.db.collation` (`Collation.BINARY` or
`Collation.ICU`, parsed in `DatabaseConfig`), that governs **only the comparisons ScalarDB
performs itself on the JVM**. Comparisons delegated to the backend (native SQL `ORDER BY`,
pushed-down predicates) are untouched, ScalarDB never emits `COLLATE` or charset clauses in
DDL, and stored bytes are never rewritten. The storage's collation stays the source of
truth, and the setting exists to make ScalarDB's in-memory decisions agree with it.

### Alternatives considered

- **Per-column or per-table collation.** Matches SQL engines' per-column model, but multiplies
  the alignment surface and the operator burden; deployments mixing collations across
  columns are explicitly out of scope for this version.
- **Emitting `COLLATE` DDL so the backend follows ScalarDB.** Inverts the authority model,
  is unavailable on non-SQL backends, and would make ScalarDB responsible for collation
  semantics it cannot control (PAD SPACE, index behavior).

### Consequences

- One `CollationComparator` (in `com.scalar.db.io`), built once from the config, is
  the comparison authority for every in-memory text comparison site.
- The setting is uniform across backends in a multi-storage deployment; a deployment whose
  storages need different collations is not fully served (documented limitation).
- Public API identity (`Key`/`Column` `equals()`/`hashCode()`) is unchanged (see ADR-5 for
  the one localized exception).

---

## ADR-2: `BINARY` (unsigned UTF-8 byte order) is the default <a name="adr-2"></a>

**Status:** Accepted

### Context

With the setting introduced, "no collation configured" needed a meaning. Keeping the legacy
Java UTF-16 order as the default would preserve bug-for-bug compatibility but perpetuate the
misalignment the feature exists to fix: no supported backend orders text in UTF-16 code-unit
order, while the byte-order backends (Cassandra, DynamoDB, Cosmos DB, PostgreSQL `C`,
MySQL `*_bin`, object storage as ScalarDB orders it) all agree with unsigned UTF-8 byte
order exactly and version-independently.

### Decision

Default `scalar.db.collation` to **`BINARY`** (unsigned UTF-8 byte order) and make the
comparator always present rather than optional. `BINARY` equality is exact `String`
equality, byte-identical to ScalarDB's historical equality behavior.

### Alternatives considered

- **Absent setting keeps legacy Java UTF-16 ordering.** Zero behavior change on upgrade,
  but the default would remain wrong for every backend, and the codebase would carry a
  three-way mode (legacy / BINARY / ICU) forever.
- **Default `ICU` (root locale).** Linguistically nicer ordering, but wrong for the
  byte-order backends that constitute most of the supported matrix, and it would put ICU4J
  on the hot path for everyone.

### Consequences

- Upgrade note: in-memory **ordering** of supplementary-plane text (above U+FFFF: emoji,
  rare CJK) changes from UTF-16 code-unit order to UTF-8 byte order on the paths ScalarDB
  evaluates itself. Equality and key identity remain byte-exact under the default.
- On byte-order backends the default is an exact, version-stable match with no
  configuration action.

---

## ADR-3: ICU4J as the collation engine, version-frozen <a name="adr-3"></a>

**Status:** Accepted

### Context

Matching UCA-based backend collations (MySQL `utf8mb4_0900_*`, MariaDB `uca1400`,
PostgreSQL's ICU provider) requires a Unicode Collation Algorithm implementation on the
JVM. ICU4J is the reference implementation. But collation tables change between ICU
releases, including maintenance releases, and the backends pin frozen UCA snapshots
(MySQL 0900 = UCA 9.0.0, MariaDB uca1400 = UCA 14.0.0), so the bundled ICU version directly
determines how closely `ICU` mode aligns with a backend, and any bump silently moves
that alignment.

### Decision

Bundle ICU4J (77.1) as a runtime dependency of ScalarDB Core and treat its version as
frozen behavior, not a routine dependency:

- the version is bumped **manually only, and only at a ScalarDB major version** (policy
  recorded at the `icu4jVersion` declaration in `build.gradle`);
- Dependabot is configured to ignore `com.ibm.icu:icu4j` (`.github/dependabot.yml`);
- the verified version is stamped into `scalardb-collation.properties` at build time, and
  `CollationComparator` logs a warning when the ICU4J actually on the runtime classpath
  differs from it (e.g. substituted by an embedding application's dependency resolution).

### Alternatives considered

- **Make ICU4J optional (separate module or `compileOnly`).** Avoids ~13 MB on the
  classpath for `BINARY`-only consumers, but makes `ICU` mode fail at runtime depending on
  packaging, and splits the tested surface.
- **Let the operator choose the ICU version.** Would allow lining up with a backend's UCA
  snapshot, but ScalarDB cannot test a matrix of ICU versions, and collation behavior would
  vary per deployment in ways that support cannot reproduce.
- **Track ICU releases normally (Dependabot).** A patch-level bump could reorder text and
  change ICU equality classes: a silent, data-visible behavior change in a minor ScalarDB
  release.

### Consequences

- `ICU` alignment is explicitly **best-effort**: ICU4J 77.1 bundles a much newer UCA/CLDR
  snapshot than the backends' frozen ones, so equality classes and orderings can disagree
  for characters whose weights changed in between. This residual risk is accepted and
  documented per backend in the compatibility matrix.
- All consumers carry ICU4J even when running `BINARY`; a downstream application pinning a
  different ICU4J major can hit Gradle conflict resolution (the runtime warning makes the
  substitution visible).
- Every bundled-ICU bump is a behavior change requiring re-verification of backend
  alignment, by policy confined to major versions.

---

## ADR-4: Equality follows the collation (no `deterministic` flag) <a name="adr-4"></a>

**Status:** Accepted. Supersedes the interim `scalar.db.collation.deterministic` flag,
which was implemented and then removed on this branch before release.

### Context

The feature initially modeled PostgreSQL: a `scalar.db.collation.deterministic` flag
decoupled ordering (always collation-aware) from equality (byte-exact unless
`deterministic=false`). Empirical verification across every supported RDBMS with a
case-insensitive collation ([collation-equality-backend-behavior.md](collation-equality-backend-behavior.md))
showed PostgreSQL is the **only** backend with that distinction: on MySQL, MariaDB,
SQL Server, Oracle, and Db2, a CI collation makes `=`, `WHERE`, and `UNIQUE`/keys
insensitive as one inseparable unit. A ScalarDB flag that models one backend's exception
would leave the common case (all the others) misaligned by default and force operators to
understand a distinction their engine doesn't have.

### Decision

Remove the flag. The configured collation governs ScalarDB's in-memory equality (`=`/`!=`)
as well as its ordering: the collation flavor is the determinism. `BINARY` equality is
byte-exact (unchanged behavior); `ICU` equality follows the configured locale/strength. To
get case-sensitive equality, configure a case-sensitive collation.
`IS_NULL`/`IS_NOT_NULL`/`LIKE`, non-text equality, and null-text comparisons always stay
byte-exact.

### Alternatives considered

- **Keep the PostgreSQL-style flag.** Maximum flexibility, but defaults would mismatch
  every non-PostgreSQL CI backend, and the flag interacts badly with key identity (ADR-5):
  on those engines, collation-aware equality without collation-aware key identity is not a
  state the backend can be in.

### Consequences

- One less setting. The mental model matches how the majority of backends actually behave.
- PostgreSQL's deterministic-CI niche (CI ordering with byte-exact equality) is
  intentionally not replicable in ScalarDB.

---

## ADR-5: Collation-canonical key identity in Consensus Commit, implicit under `ICU` <a name="adr-5"></a>

**Status:** Accepted

### Context

On a backend with a CI key collation, the same logical row reaches the Consensus Commit
snapshot under two spellings: the bytes the application requested (`'apple'`) and the bytes
the backend physically stores and returns from scans (`'Apple'`). With byte-exact key
identity, every site that looks up a request-keyed entry with a storage-returned key
misses. The feasibility investigation
([collation-phase2-consensus-commit-feasibility.md](collation-phase2-consensus-commit-feasibility.md))
catalogued the failures: some fail safe (spurious aborts at prepare/validation), but
read-your-own-writes returns silently stale values, scan-after-write overlap detection is
silently skipped, and secondary-index results are silently filtered out. Byte-exact
identity on a CI backend is already broken today, so the change *repairs* existing
behavior rather than merely extending the feature.

### Decision

Under `ICU`, make the Consensus Commit transaction layer's key identity
**collation-canonical**: two collate-equal keys are one logical key across the snapshot's
read/write/delete bookkeeping, read-your-own-writes merging, the prepare-time before-image
join, write-write and scan-after-write conflict detection, and mutation grouping
(`Snapshot`, `CrudHandler`, `MutationsGrouper` all key through the canonical form exposed
by `CollationComparator`). The change is **localized**: `Key`/`Column`
`equals()`/`hashCode()` and all other public API semantics are untouched, stored key bytes
are never rewritten or normalized, and physical uniqueness remains the backend's job. Under
`BINARY` (the default), key identity stays byte-exact, unchanged.

Key identity comes **implicitly with `ICU`**: there is no separate opt-in flag. The
feasibility report initially argued for one, but ADR-4's finding removed the case for it:
on every backend where a CI collation is achievable except PostgreSQL, predicate equality
and key identity are inseparable, so a configuration expressing one without the other would
describe a backend state that cannot exist. The safety boundary is instead the documented
**aligned-backend contract**: the backend must enforce one row per collation class on keys
(CI PK/unique index) and must resolve point reads by the same collation (so lazy recovery
finds a write-set key recorded as `'apple'` against a record stored as `'Apple'`).

### Alternatives considered

- **Global collation-aware `Key`/`Column` equality.** Simpler to reason about, but changes
  public API semantics for every consumer and every backend, including ones where it is
  unsafe.
- **Normalizing stored key bytes to a canonical spelling.** Would collapse the two
  provenances at the source, but rewriting user data is out of the question and case
  mappings are not stable across collations.
- **A separate opt-in flag for key identity.** Rejected per the inseparability finding
  above; it would also leave the silent-wrong-answer paths in place for `ICU` users who
  didn't discover the flag.

### Consequences

- Deployments already running `scalar.db.collation=ICU` (e.g. adopted for ordering alone)
  get collation-canonical key identity on upgrade with no configuration action. They must
  assess backend alignment or switch to `BINARY` (documented upgrade note).
- Misaligned backends (byte-order stores, or an aligned backend on text hitting
  ICU-vs-backend version skew per ADR-3) are unsafe under `ICU` key identity: distinct rows
  can collapse or writes silently merge. ScalarDB does not detect this (ADR-6); the
  documentation directs such deployments to `BINARY`.
- The alignment contract for key identity is strictly stronger than for ordering/equality
  (it adds the uniqueness and recovery point-read requirements), and the documentation
  calls that out explicitly.

---

## ADR-6: No runtime collation-vs-storage validation <a name="adr-6"></a>

**Status:** Accepted. Narrowed by [ADR-9](#adr-9): storages that are binary-only *by
construction* reject `ICU` at startup, and this record still governs every configurable
backend.

### Context

Misconfiguring the collation relative to the backend ranges from harmless to unsafe
(ADR-5). It is tempting to have ScalarDB detect the backend's collation and reject or warn
on a mismatch.

### Decision

ScalarDB applies the configured collation uniformly and performs **no**
compatibility validation against the backend: it neither rejects nor warns. Matching the
collation to the storage is the operator's responsibility, supported by the per-backend
compatibility matrix and recommendations in
[collation-storage-compatibility.md](collation-storage-compatibility.md).

### Alternatives considered

- **Probe the backend's collation at startup and reject mismatches.** There is no reliable,
  uniform way to do this: collation is per-column on most RDBMS (and ScalarDB does not own
  the DDL), non-SQL backends expose nothing to probe, and "match" is not binary: the
  ICU-vs-UCA relationship is best-effort by construction (ADR-3), so a checker would either
  block legitimate configurations or bless unsafe ones.
- **Warn heuristically.** Same detection problem with lower stakes; a warning that is
  frequently wrong in both directions trains operators to ignore it.

### Consequences

- Misalignment is the operator's responsibility. The documentation states this
  prominently and gives concrete per-backend guidance (`BINARY` for byte-order stores,
  `ICU` only against UCA-based collations).
- Configuration errors ScalarDB *can* judge locally are still rejected (see ADR-7).

---

## ADR-7: Invalid ICU configuration fails at startup <a name="adr-7"></a>

**Status:** Accepted

### Context

ICU's default behavior is forgiving: an unrecognized locale silently falls back to the root
collation. For this feature, silent fallback is the worst failure mode: the operator
configured `ja` expecting Japanese ordering, mistyped it, and gets root ordering with no
signal, producing exactly the ScalarDB-vs-backend disagreement the setting exists to
prevent, but now undetectable.

### Decision

Validate the collation configuration at startup and fail fast with an
`IllegalArgumentException` (via `CoreError`) on: an invalid `scalar.db.collation` value, an
invalid `scalar.db.collation.icu.strength`, a malformed `scalar.db.collation.icu.rules`
string, and a locale ICU has no collation data for. A recognized language with an unknown
region (e.g. `en_XX`) is accepted and uses the language's collation, matching ICU's
resolution semantics. Custom `rules` compose *on top of* the configured locale's collation
rather than replacing it, so tailoring and locale selection stay orthogonal.

### Consequences

- Misconfiguration surfaces at deployment time, not as silently wrong ordering in
  production.
- One known gap is tracked as an open item ([collation-open-questions.md](collation-open-questions.md) V1):
  the locale string is parsed as a legacy ICU locale ID, so a BCP-47 `-u-` collation
  keyword (e.g. `ja-u-co-unihan`) passes validation but silently drops the keyword.

---

## ADR-8: Integration tests apply the backend collation via namespace defaults and a required-mode capability gate <a name="adr-8"></a>

**Status:** Accepted

### Context

The `ICU` mode's value claim (ScalarDB's in-memory decisions agree with a CI-collated
backend) can only be proven against real backends with real CI collations. But ScalarDB
never emits `COLLATE` DDL (ADR-1), so the tests must arrange the backend collation
out-of-band; backends differ in where collation can be set (MySQL/MariaDB: schema default;
PostgreSQL/SQL Server: per column; TiDB: rejects altering the collation of indexed
columns); and a capability probe that silently skips on an unreachable backend would let CI
rot into a no-op.

### Decision

Two integration-test suites, `DistributedStorageCollationIntegrationTestBase` (storage
layer) and `ConsensusCommitCollationIntegrationTestBase` (key identity), exercise `ICU` at
`PRIMARY` strength inside the existing CI jobs, with the backend collation applied through
test-only `AdminTestUtils` hooks:

- **MySQL/MariaDB:** set the CI collation as the **namespace (database) default** before
  table creation so tables inherit it at creation time, plus a read-back verification of
  the created columns' collations that fails fast on a stale collation left by a leaked
  table (`JdbcCollationVerificationIntegrationTest`). Choosing the collation at creation
  rather than altering afterward is also what removes TiDB's historical hard blocker.
- **PostgreSQL/SQL Server** (no namespace-level collation): alter the created table's
  character columns.
- **Capability gate:** other JDBC backends skip via
  `JdbcCollationTestUtils.isCollationTestSupported`; on CI-covered backends the gate runs
  with `scalardb.jdbc.collation_test=required`, which turns an *inconclusive* probe (e.g. a
  connection failure) into a hard error so a half-configured backend can never silently
  skip, while a *definitive* incapability verdict (MySQL 5.7, TiDB pending, a PostgreSQL
  build without ICU) still skips with the reason logged.
- **Default-mode fidelity:** the CI jobs' default collations are pinned to exact `BINARY`
  matches (`utf8mb4_0900_bin`, `utf8mb4_nopad_bin`, PostgreSQL `--locale=C`,
  `Japanese_BIN2`, AlloyDB posix, per `ci/tests-config.yaml`) so the rest of the test
  matrix exercises the default mode against byte-ordered backends.

### Consequences

- The aligned-backend contract of ADR-5 is exercised end-to-end on MySQL 8.x, MariaDB
  10.10+, PostgreSQL/AlloyDB, and SQL Server; object storage runs neither suite because it
  rejects `ICU` at startup (ADR-9), which is unit-tested instead.
- TiDB remains gate-excluded pending a version-check-to-usage-probe change and a CI
  environment prerequisite (`tidb_enable_noop_functions=1` for read-only metadata
  connections), though both suites passed manually on TiDB 8.5.
- Test coverage tracks the bundled ICU version: alignment must be re-verified when it
  changes (ADR-3).

---

## ADR-9: `ICU` rejected at startup on structurally binary-only storages <a name="adr-9"></a>

**Status:** Accepted

### Context

ADR-6 rules out probing the backend for its collation: detection is unreliable, and
"match" is not a binary property. But a subset of storages doesn't need a probe, because
the storage *kind* alone proves `ICU` can never match:

- **Cassandra, DynamoDB, Cosmos DB:** the native string order is fixed to UTF-8 bytes /
  code points and is not configurable
  ([collation-storage-compatibility.md](collation-storage-compatibility.md)). An `ICU`
  configuration always disagrees with the backend, and under ADR-5 key identity it is
  documented as unsafe (physically distinct collate-equal rows collapse).
- **SQLite and Cloud Spanner (PostgreSQL dialect), via JDBC:** the same fixed byte /
  code-point order (SQLite compares with `memcmp`; the Spanner PG dialect does not support
  `COLLATE`), and both are identifiable from the JDBC engine.
- **Object storage (S3, Azure Blob Storage, GCS):** ScalarDB's own adapter is the storage
  engine, and its record identity is byte-exact: partition objects are named by the raw
  partition-key text and records are keyed by the raw concatenated key text, so point
  reads resolve byte-exactly and nothing enforces one record per collation class. The
  ADR-5 aligned-backend contract is structurally unavailable there. Making the adapter
  satisfy the contract (canonical ICU-sort-key object names and record ids) was researched
  and set aside: ICU sort keys are not stable across ICU4J versions or collation settings,
  so the collation configuration would become a persistent storage-format parameter,
  requiring a data re-keying migration on any `scalar.db.collation.icu.*` change or ICU4J
  upgrade.

Leaving these storages as documentation-only guidance (pure ADR-6) keeps a
silent-wrong-answer path open for exactly the deployments the documentation says must not
use `ICU`.

### Decision

Reject `scalar.db.collation=ICU` at startup with an `IllegalArgumentException` (via
`CoreError`) on storages whose binary-only text order is knowable from the storage type
alone: the Cassandra, DynamoDB, Cosmos DB, and object-storage adapters check in their
constructors, and the JDBC adapter delegates to
`RdbEngineStrategy.throwIfCollationNotSupported`, overridden by the SQLite and Spanner
engines. This is local configuration judgment in the sense of ADR-7, not backend probing:
no connection is made and no backend state is inspected.

ADR-6 still governs every configurable backend: MySQL, MariaDB, PostgreSQL, Oracle,
SQL Server, Db2, YugabyteDB, and TiDB accept `ICU` unvalidated, because whether it matches
depends on the collation the operator configured on the database, including byte-order
*configurations* such as PostgreSQL `C`, which remain the operator's responsibility.

In multi-storage, the guard runs inside each sub-storage's constructor while the
transaction layer compares with the top-level collation, so a per-storage collation
override could desynchronize the two: `storages.<name>.collation=BINARY` would slip an
ICU-keyed transaction layer past a binary-only sub-storage's guard, exactly the
silently-misaligned state this record exists to prevent. So `MultiStorageConfig`
rejects any per-storage `scalar.db.collation` / `scalar.db.collation.icu.*` property.
Collation is one global setting (ADR-1).

### Alternatives considered

- **Keep documentation-only guidance (pure ADR-6).** Rejected: for these storages the
  misconfiguration is provable locally, and ADR-7 already established that locally
  judgeable configuration errors fail fast.
- **Implement object-storage `ICU` key identity instead of rejecting.** Deferred: the
  adapter change is moderate, but canonical keying makes the collation configuration part
  of the on-disk format (a persisted collation fingerprint, a hard rather than
  warning-level ICU4J version check, and a re-keying migration on every collation or ICU4J
  change), which is disproportionate to current demand for `ICU` on object storage.

### Consequences

- Object storage supports only `BINARY`. It no longer runs the storage-layer `ICU`
  integration suite; the constructor rejection is unit-tested instead.
- A deployment that had configured `ICU` on one of these storages fails at startup after
  upgrading, instead of continuing with silently misaligned comparisons.
- Spanner is recognized only through its JDBC URL (`jdbc:cloudspanner:` /
  `jdbc:spanner:`); a PG-dialect Spanner reached through the PostgreSQL driver (PGAdapter)
  is indistinguishable from PostgreSQL and stays under ADR-6.

## ADR-10: `LIKE` rejected in transactions under `ICU` <a name="adr-10"></a>

**Status:** Accepted

### Context

`scalar.db.collation` governs ordering, equality (ADR-4), and key identity (ADR-5), but it
has never governed pattern matching: `LIKE` and `NOT_LIKE` are evaluated as a Java regex
over exact code points at any collation. That exclusion is the last remaining instance of
the divergence class the setting was introduced to close (ADR-1).

The Consensus Commit transaction layer re-evaluates a selection's conjunctions in memory
after the backend has already applied them — when merging the transaction's own writes,
when re-checking records whose before images matched, and when detecting scan-after-write
conflicts. On a backend whose own collation governs `LIKE` (MySQL and MariaDB defaults,
SQL Server defaults, TiDB, Oracle under `NLS_COMP=LINGUISTIC`) the backend returns rows
that this byte-exact re-check then discards, a case-differing own write stays hidden from
the transaction that made it, and a pending write the backend would treat as overlapping a
scan goes unseen. The last of these is the unsafe direction: a missed conflict rather than
an extra abort.

Making the pattern collation-aware requires defining what `_` and `%` mean against
contractions, expansions, and strength-ignorable characters. No two backends answer that
the same way: MySQL carries a per-collation wildcard comparator, and PostgreSQL refuses
pattern matching under a nondeterministic collation outright ("The pattern matching
operators of all three kinds do not support nondeterministic collations"). An in-memory
answer would be a third dialect matching neither the backend nor the operator's
expectation, and — unlike ordering, where ADR-3 accepts a documented best-effort gap —
a wrong row set is not a near miss.

### Decision

Reject a `LIKE` or `NOT_LIKE` conjunction with an `IllegalArgumentException` (via
`CoreError`) when `scalar.db.collation=ICU`, in `ConsensusCommitOperationChecker`, for both
one-phase and two-phase transactions. The check is unconditional on isolation level: only
the missed scan-after-write conflict is specific to `SERIALIZABLE`, while the discarded
rows and the hidden own writes occur under `SNAPSHOT` too.

The rejection is scoped to the transaction layer because that is the only place `ICU` can
reach where ScalarDB evaluates the pattern itself. The storage API, the JDBC transaction
manager, and single-CRUD transaction mode push the pattern down and are untouched. The
storage-side in-memory conjunction filter (`FilterableScanner`) is used only by Cassandra,
DynamoDB, Cosmos DB, and object storage — exactly the storages ADR-9 rejects `ICU` on at
startup — so it is unreachable under `ICU` and needs no guard. Should ADR-9 ever admit
`ICU` on one of them, that path needs this check too.

Under `BINARY`, `LIKE` behaves exactly as before.

### Alternatives considered

- **Collation-aware pattern matching (ICU `StringSearch` over the pattern's literal
  segments).** Requires inventing wildcard semantics against contractions and expansions
  that match no specific backend, so it would replace a known divergence with an unknown
  one at much higher cost.
- **Case/accent folding both sides, keeping the regex.** Handles the common Latin-text case
  but is wrong wherever the fold is not length-preserving, and it introduces a second notion
  of collation equality alongside `textEquals`, contradicting ADR-4's one-collation
  determinism.
- **Making only the conflict check conservative.** Treating a `LIKE` conjunction as
  possibly-overlapping would close the unsafe direction without rejecting anything, but
  leaves the discarded-rows and hidden-own-write cases returning silently wrong results.

### Consequences

- An application that combined `ICU` with a `LIKE` conjunction inside a transaction now
  fails fast instead of receiving a row set neither ScalarDB nor the backend agrees on.
- The same divergence remains under `BINARY`, which is the default: MySQL's default
  collation and SQLite both match patterns insensitively while the transaction layer
  re-checks byte-exactly. This ADR does not close that; it is tracked separately and is
  the reason ADR-10 must not be read as closing the whole `LIKE` gap.
- A PostgreSQL deployment using a nondeterministic ICU collation — the configuration
  [collation-storage-compatibility.md](collation-storage-compatibility.md) recommends for
  `ICU` — already fails its pushed-down `LIKE` in the backend, independently of this guard.
