# Storage-aligned collation (`scalar.db.collation`)

ScalarDB historically compared text on the JVM with Java's natural `String` order (UTF-16
code-unit order). The supported backends do not agree with that order or with each other:
MySQL 8's default `utf8mb4_0900_ai_ci` is case- and accent-insensitive, PostgreSQL orders
per its configured collation, Cassandra and DynamoDB compare UTF-8 bytes, and object
storage is ordered entirely by ScalarDB in memory. Even byte order diverges from Java
UTF-16 order above U+FFFF (emoji, rare CJK).

The `scalar.db.collation` setting aligns ScalarDB's **in-memory string comparison** with
the collation configured on the underlying storage. It defaults to **`BINARY`** (unsigned
UTF-8 byte order — what byte-order backends actually do), so ScalarDB's own comparisons
agree with those backends out of the box.

> **Note:** This is a ScalarDB Core repository. The canonical, user-facing configuration
> reference lives at <https://scalardb.scalar-labs.com/docs/latest/configurations>. This
> document describes the setting so the content can be ported there.

## Configuration

| Property | Description |
|---|---|
| `scalar.db.collation` | Collation mode: `BINARY` (the default when absent) or `ICU`. Governs **ordering**, **equality**, and — in the Consensus Commit transaction layer — **key identity** (see "Collation-aware equality" and "Collation-aware key identity" below). |
| `scalar.db.collation.icu.locale` | *(ICU only)* Locale that selects the collation rules (for example `en`, `en_US`, `ja`). When absent, ICU's root locale is used. |
| `scalar.db.collation.icu.strength` | *(ICU only)* One of `PRIMARY`, `SECONDARY`, `TERTIARY`, `QUATERNARY`, `IDENTICAL`. Controls how much detail ordering distinguishes: `PRIMARY` is case- and accent-insensitive; `SECONDARY` adds accent sensitivity; `TERTIARY` adds case sensitivity. When absent, ICU's default strength applies. |
| `scalar.db.collation.icu.rules` | *(ICU only)* An optional custom ICU tailoring-rule string that fine-tunes ordering *on top of* the configured `locale` (its rules extend the locale's collation, or the root collation when no locale is set) and `strength`. A malformed rule string is rejected at startup. |

### Values

- **`BINARY`** *(default)* — orders text by unsigned UTF-8 byte sequence. Equality is
  byte-exact, identical to how ScalarDB has always compared text for equality; only the
  in-memory *ordering* of supplementary-plane text differs from the pre-collation Java
  order (see the upgrade note under Limitations).
- **`ICU`** — orders text according to the Unicode Collation Algorithm (UCA) as implemented
  by [ICU4J](https://unicode-org.github.io/icu/userguide/collation/), configured by locale,
  strength, and optional tailoring rules. MySQL 8 defaults and PostgreSQL's ICU collation
  provider are built on the UCA, so ICU mode aligns closely with them.

### Example

```properties
# Align with a case-insensitive MySQL 8 collation
scalar.db.collation=ICU
scalar.db.collation.icu.locale=en_US
scalar.db.collation.icu.strength=PRIMARY
```

```properties
# Byte-order backends (Cassandra, DynamoDB, PostgreSQL C, MySQL *_bin)
scalar.db.collation=BINARY
```

## Scope — what the setting governs

`scalar.db.collation` governs **ordering**, **equality**, and — under `ICU`, within the Consensus
Commit transaction layer — **key identity**, and only the comparisons
ScalarDB performs itself on the JVM:

- object-storage scan sort and range filtering;
- ScalarDB's own in-memory cross-partition / conjunction range **and** equality filtering (`>`,
  `>=`, `<`, `<=`, `=`, `!=`);
- the Consensus Commit snapshot's scan-after-write range- **and** equality-membership check; and
- in-memory conditional-mutation range and equality predicates (`putIf`/`deleteIf`/`updateIf`
  with `>`, `>=`, `<`, `<=`, `=`, `!=` on a text column) that ScalarDB evaluates itself — under
  Consensus Commit and for object storage. (Conditional mutations that other backends push down
  to storage are evaluated by the backend's own collation.)

Equality follows the collation whenever one is set (see "Collation-aware equality" below):
byte-exact for `BINARY`, collation-aware for `ICU`. `IS_NULL`/`IS_NOT_NULL`/`LIKE` and null-text
comparisons always stay byte-exact.

It does **not** affect:

- **Comparisons delegated to the backend** — single-partition JDBC scans use native SQL
  `ORDER BY`, and backends that reject cross-partition scans with ordering (DynamoDB,
  Cosmos DB, Cassandra) keep their native semantics. ScalarDB never emits `COLLATE` or
  charset clauses in DDL; the storage's collation stays the source of truth.
- **Public API identity and stored bytes** — `Key`/`Column` `equals()`/`hashCode()` are
  unchanged, and ScalarDB never rewrites or normalizes stored key bytes. The collation-canonical
  key identity described under "Collation-aware key identity" below is localized to the Consensus
  Commit transaction layer's own bookkeeping.

## Storage recommendation (guidance only)

ScalarDB does **not** validate collation-vs-storage compatibility — it neither rejects nor
warns, and applies the collation uniformly regardless of backend. Matching the collation to
the storage is the operator's responsibility.

- Use **`BINARY`** for byte-order backends: Cassandra, DynamoDB, PostgreSQL with the `C`
  collation, and MySQL `*_bin` collations.
- Use **`ICU`** (with a matching locale/strength) for UCA-based collations: MySQL 8 defaults
  and PostgreSQL's ICU collation provider.

See [Storage collation compatibility](collation-storage-compatibility.md) for a per-storage
breakdown of which collations each supported backend offers and whether `BINARY` or `ICU` can
match them (including why the bundled ICU version bounds how closely `ICU` mode can align).

## Collation-aware equality

The configured collation governs ScalarDB's own in-memory **equality** (`=`/`!=`) as well as its
ordering — the collation flavor *is* the determinism, matching how MySQL, MariaDB, SQL Server, and
Oracle collations behave (a case-/accent-insensitive collation makes `=`, `WHERE`, and uniqueness
insensitive as one unit). There is no separate toggle: whenever a collation is set, equality
follows it. This differs from PostgreSQL's ability to decouple the two with a *deterministic*
collation flag, which ScalarDB intentionally does not replicate — to get case-sensitive equality,
configure a case-sensitive collation.

The collation governs `=`/`!=` on text at the paths ScalarDB evaluates itself: in-memory
conjunction/scan filtering, conditional-mutation `EQ`/`NE` (`putIf`/`deleteIf`/`updateIf`, under
Consensus Commit and object storage), and the Consensus Commit snapshot's equality-based overlap
check — so with a case-insensitive `ICU` collation a `WHERE textcol = 'apple'` predicate matches a
stored `'Apple'`, and an `=`-predicate scan-after-write detects a case-differing pending write (the
equality analog of the range behavior). `IS_NULL`/`IS_NOT_NULL`, `LIKE`, non-text equality, and
null-text comparisons always stay byte-exact.

- **`BINARY`** — equality is byte-exact (`'Apple' != 'apple'`), identical to ScalarDB's current
  behavior.
- **`ICU`** — equality follows the collation: at a case-/accent-insensitive strength `'Apple'`
  equals `'apple'`; at a case-sensitive strength it distinguishes them.

Backend equality match has the same best-effort caveats as ordering (see below and
[Storage collation compatibility](collation-storage-compatibility.md)).

## Collation-aware key identity (Consensus Commit)

Under `ICU`, the Consensus Commit transaction layer's **key identity** is collation-canonical:
two collate-equal keys (for example the `'apple'` your application typed and the `'Apple'` the
backend physically stores and returns from scans) are **one logical key** across the snapshot's
read/write/delete bookkeeping, read-your-own-writes merging, the prepare-time before-image join,
write-write and scan-after-write conflict detection (including collisions on partition/clustering
keys and secondary-index values), and mutation grouping. This matches what a CI-collated backend
itself enforces — one physical row per collation class, with collation-aware `=`, uniqueness, and
read-your-own-writes (verified empirically against MySQL `utf8mb4_0900_ai_ci`). Without it, a
byte-exact snapshot on such a backend silently returns stale reads of its own writes, aborts valid
read-modify-writes at prepare, splits one row's writes in two, and misses scan-after-write
conflicts. Under `BINARY` (the default), key identity stays byte-exact, unchanged.

**What key identity does *not* change:** stored key bytes are never rewritten or normalized (the
row keeps whatever spelling created it); `Key`/`Column` `equals()`/`hashCode()` and other public
API semantics are untouched; physical uniqueness remains entirely the backend's job.

**The aligned-backend contract (stricter than for ordering/equality):** collation-aware key
identity assumes the backend's key collation matches the configured `ICU` collation, in two ways:

1. **Uniqueness:** the backend must enforce one row per collation class on keys (a CI PK/unique
   index), so the transaction layer's one-logical-key view cannot collapse two real rows.
2. **Point-read resolution (recovery):** the backend must resolve point reads by the same
   collation — a write-set key recorded as `'apple'` must find a record physically stored as
   `'Apple'` during lazy recovery, or recovery silently leaves records `PREPARED`.

"Matches" is bounded by collation-version skew: MySQL's `utf8mb4_0900_*` implements UCA 9.0.0
while the bundled ICU4J implements a much newer Unicode version, so equality classes can disagree
for characters whose collation weights changed in between. The alignment premise holds only for
text whose equality classes agree between the backend collation and the bundled ICU — restrict
key text to a stable repertoire, and re-verify alignment when the bundled ICU version changes.

**Misaligned backends are unsafe for `ICU` key identity** (byte-order backends such as Cassandra,
DynamoDB, Cosmos DB, PostgreSQL `C` — or an "aligned" backend on text hitting the version skew
above). ScalarDB applies the setting without validation, so the operator owns the consequences:
two physically distinct collate-equal rows collapse into one entry in the transaction layer's
result maps (a row silently dropped, limit counts change); writes to physically distinct rows are
canonically merged (one write silently dropped); legitimate scan-after-write patterns on genuinely
distinct keys abort as false conflicts; and where the backend equates strings ICU distinguishes,
the split-identity defects resurface for those strings. Use `BINARY` on such backends.

**Upgrade note:** a deployment already running `scalar.db.collation=ICU` (for example adopted for
ordering alone on an earlier version) gets collation-canonical key identity on upgrade with no
configuration action. Assess backend alignment per the contract above, or switch to `BINARY`.

## Automated test coverage

Two integration-test suites exercise the ICU mode end-to-end (at `PRIMARY` strength) inside the
existing CI jobs; the tests create their tables through normal ScalarDB DDL and then alter them
to the target backend collation via a test-only `AdminTestUtils` hook:

- **Storage-layer suite** (`DistributedStorageCollationIntegrationTestBase`): scan ordering,
  conditional-mutation `EQ`/`NE` across collate-equal spellings, range operators across case
  boundaries, and cross-partition filtering. Runs on MySQL 8.x (`utf8mb4_0900_ai_ci`), MariaDB
  10.10+ (`utf8mb4_uca1400_ai_ci`), PostgreSQL and AlloyDB (a nondeterministic ICU collation at
  primary strength the tests create), SQL Server (`Latin1_General_100_CI_AI`, basic-Latin data
  only), and object storage (no backend collation — ScalarDB's in-memory comparisons alone are
  under test).
- **Consensus Commit key-identity suite** (`ConsensusCommitCollationIntegrationTestBase`): the
  aligned-backend scenarios above (read-modify-write across spellings, read-your-own-writes,
  write-write convergence to one physical row, scan-after-delete detection) through real
  transactions. Runs on the same JDBC backends as the storage-layer suite (object storage is
  excluded: its record identity is byte-exact, so the aligned-backend contract is structurally
  unavailable there). Other JDBC backends skip via a capability gate
  (`JdbcCollationTestUtils.isCollationTestSupported`); on the CI-covered backends the gate is
  enforced with `scalardb.jdbc.collation_test=required`, which turns an inconclusive probe (e.g. a
  connection failure) into a hard error so an unreachable or half-configured backend can never
  silently skip. A definitive incapability verdict (MySQL 5.7, TiDB, a PostgreSQL build without
  ICU) still skips even in required mode, with the reason logged at WARN. TiDB stays excluded even though it supports
  `utf8mb4_0900_ai_ci` expressions: it rejects converting the collation of indexed columns
  ("Unsupported converting collation ... when index is defined on it"), and every ScalarDB key
  column is indexed, so the tests cannot produce a collated table there.

The CI jobs' **default** collations are pinned to exact `BINARY` matches so the rest of the test
matrix exercises the default mode faithfully: MySQL 8.x `utf8mb4_0900_bin` (5.7 keeps legacy
`utf8mb4_bin` — its only option, PAD SPACE gap documented), MariaDB `utf8mb4_nopad_bin`,
PostgreSQL `--locale=C` with UTF8 encoding, SQL Server `Japanese_BIN2`, AlloyDB posix ICU locale
(see `ci/tests-config.yaml`).

## Limitations
- **Upgrade note (default ordering change).** Compared to releases without the collation
  feature, the `BINARY` default changes ScalarDB's **in-memory ordering** of
  supplementary-plane text (characters above U+FFFF, e.g. emoji and rare CJK) from Java
  UTF-16 code-unit order to unsigned UTF-8 byte order on the paths ScalarDB evaluates
  itself. Equality and key identity remain byte-exact, unchanged. On byte-order backends
  this aligns in-memory decisions with the storage; no configuration action is needed.
- **ICU alignment is best-effort, not byte-exact.** ICU matches UCA-based collations closely,
  but cannot exactly reproduce libc-based PostgreSQL ordering (which itself drifts across
  glibc versions), legacy non-UCA MySQL collations (for example `utf8_general_ci`), or SQL
  Server Windows collations. Any residual divergence — even a single codepoint — can produce
  a scan or snapshot range decision that disagrees with the backend. This residual risk is
  accepted.
- **A single, uniform collation is assumed.** `scalar.db.collation` is global, not per-column
  or per-table. Deployments that mix collations across columns are not fully served by this
  version.
- **ICU4J is added to the runtime classpath.** ICU mode requires ICU4J (~13 MB), added to
  ScalarDB Core as a runtime dependency for all consumers (including `BINARY`-only ones). A
  downstream application pinning a different ICU4J major version can hit Gradle conflict
  resolution, and ICU version differences carry collation-table changes.

- **An unrecognized `scalar.db.collation.icu.locale` is rejected at startup.** Like an invalid
  `scalar.db.collation`, `scalar.db.collation.icu.strength`, or a malformed
  `scalar.db.collation.icu.rules`, a locale ICU has no collation data for fails fast with an
  `IllegalArgumentException` rather than silently falling back to root-collation ordering.
  Configure a valid ICU locale (for example `en`, `en_US`, `ja`). Note that a recognized language
  with an unknown region (for example `en_XX`) is accepted and uses the language's collation.
