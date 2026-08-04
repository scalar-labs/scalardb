# Storage-aligned collation (`scalar.db.collation`)

ScalarDB runs on the JVM and, by default, compares text with Java's natural `String`
order (UTF-16 code-unit order). The supported backends do not agree with that order or
with each other: MySQL 8's default `utf8mb4_0900_ai_ci` is case- and accent-insensitive,
PostgreSQL orders per its configured collation, Cassandra and DynamoDB compare UTF-8
bytes, and object storage is ordered entirely by ScalarDB in memory. Even byte order
diverges from Java UTF-16 order above U+FFFF (emoji, rare CJK).

The `scalar.db.collation` setting aligns ScalarDB's **in-memory string ordering** with the
collation configured on the underlying storage. It is opt-in: when unset, ScalarDB keeps
its current comparison behavior and upgrading has no behavioral impact.

> **Note:** This is a ScalarDB Core repository. The canonical, user-facing configuration
> reference lives at <https://scalardb.scalar-labs.com/docs/latest/configurations>. This
> document describes the setting so the content can be ported there.

## Configuration

| Property | Description |
|---|---|
| `scalar.db.collation` | Collation mode: `BINARY` or `ICU`. When absent, ScalarDB uses its current comparison behavior (Java UTF-16 code-unit order). Governs both **ordering** and **equality** (see "Collation-aware equality" below). |
| `scalar.db.collation.icu.locale` | *(ICU only)* Locale that selects the collation rules (for example `en`, `en_US`, `ja`). When absent, ICU's root locale is used. |
| `scalar.db.collation.icu.strength` | *(ICU only)* One of `PRIMARY`, `SECONDARY`, `TERTIARY`, `QUATERNARY`, `IDENTICAL`. Controls how much detail ordering distinguishes: `PRIMARY` is case- and accent-insensitive; `SECONDARY` adds accent sensitivity; `TERTIARY` adds case sensitivity. When absent, ICU's default strength applies. |
| `scalar.db.collation.icu.rules` | *(ICU only)* An optional custom ICU tailoring-rule string that fine-tunes ordering *on top of* the configured `locale` (its rules extend the locale's collation, or the root collation when no locale is set) and `strength`. A malformed rule string is rejected at startup. |

### Values

- **`BINARY`** — orders text by unsigned UTF-8 byte sequence. This is the value most
  deployments should set (see the recommendation below), though it is **not** the default,
  because the default must preserve existing behavior.
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

`scalar.db.collation` governs both **ordering** and **equality**, and only the comparisons
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
- **Key identity** — `Key`/`Column` `equals()`/`hashCode()`, snapshot map keying, deduplication
  keying, read/write-set membership, and delete-set overlap stay byte-exact **regardless of the
  setting**. Collation-aware equality changes only *predicate/read* `=`/`!=` evaluation, never key
  identity — treating distinct keys as one is a transaction-correctness change reserved for a later
  phase (see "Collation-aware equality").

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

**What it does *not* change (the identity boundary):** `Key`/`Column` `equals()`/`hashCode()`,
the snapshot map keying (`readSet`/`writeSet`/`deleteSet`), deduplication keying, and physical
storage record keying stay **byte-exact**. Two values that collate-equal but differ in bytes (for
example `'Apple'` and `'apple'`) remain **distinct keys and distinct stored rows**. So this version
matches **predicate/read equality**, not key-level uniqueness — matching the backend's *uniqueness*
on keys (which would require collation-aware key identity across the snapshot, deduplication, and
mutation grouping, and a stricter backend invariant) is a **reserved future phase**, deliberately
not built here.

A concrete consequence of that boundary: a Consensus Commit scan-after-write conflict whose
overlap depends on a collation-matching **partition or clustering key** — for example a blind
insert to key `'Apple'` followed by a scan of key `'apple'` under a case-insensitive collation —
is **not** detected in this version, because partition/clustering keys are still compared
byte-exact. Only conflicts that depend on a **non-key predicate column** (a `WHERE`/conditional
`=` on a value column) are collation-aware. Detecting key-collision conflicts is part of the
reserved key-identity phase.

Backend equality match has the same best-effort caveats as ordering (see below and
[Storage collation compatibility](collation-storage-compatibility.md)).

## Limitations
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
  ScalarDB Core as a runtime dependency for all consumers (including `BINARY`-only and unset
  ones). A downstream application pinning a different ICU4J major version can hit Gradle
  conflict resolution, and ICU version differences carry collation-table changes.

- **An unrecognized `scalar.db.collation.icu.locale` is rejected at startup.** Like an invalid
  `scalar.db.collation`, `scalar.db.collation.icu.strength`, or a malformed
  `scalar.db.collation.icu.rules`, a locale ICU has no collation data for fails fast with an
  `IllegalArgumentException` rather than silently falling back to root-collation ordering.
  Configure a valid ICU locale (for example `en`, `en_US`, `ja`). Note that a recognized language
  with an unknown region (for example `en_XX`) is accepted and uses the language's collation.
