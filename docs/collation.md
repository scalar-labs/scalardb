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
| `scalar.db.collation` | Collation mode: `BINARY` or `ICU`. When absent, ScalarDB uses its current comparison behavior (Java UTF-16 code-unit order). |
| `scalar.db.collation.locale` | *(ICU only)* Locale that selects the collation rules (for example `en`, `en_US`, `ja`). When absent, ICU's root locale is used. |
| `scalar.db.collation.strength` | *(ICU only)* One of `PRIMARY`, `SECONDARY`, `TERTIARY`, `QUATERNARY`, `IDENTICAL`. Controls how much detail ordering distinguishes: `PRIMARY` is case- and accent-insensitive; `SECONDARY` adds accent sensitivity; `TERTIARY` adds case sensitivity. When absent, ICU's default strength applies. |
| `scalar.db.collation.rules` | *(ICU only)* An optional custom ICU tailoring-rule string to fine-tune ordering beyond locale and strength. A malformed rule string is rejected at startup. |

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
scalar.db.collation.locale=en_US
scalar.db.collation.strength=PRIMARY
```

```properties
# Byte-order backends (Cassandra, DynamoDB, PostgreSQL C, MySQL *_bin)
scalar.db.collation=BINARY
```

## Scope — what the setting governs

`scalar.db.collation` governs **ordering only**, and only the comparisons ScalarDB performs
itself on the JVM:

- object-storage scan sort and range filtering;
- ScalarDB's own in-memory cross-partition / conjunction range filtering (`>`, `>=`, `<`,
  `<=`); and
- the Consensus Commit snapshot's scan-after-write range-membership check.

It does **not** affect:

- **Comparisons delegated to the backend** — single-partition JDBC scans use native SQL
  `ORDER BY`, and backends that reject cross-partition scans with ordering (DynamoDB,
  Cosmos DB, Cassandra) keep their native semantics. ScalarDB never emits `COLLATE` or
  charset clauses in DDL; the storage's collation stays the source of truth.
- **Equality and identity** — these stay byte-exact regardless of the setting: `Key`/`Column`
  `equals()`/`hashCode()`, snapshot map keying, deduplication, read/write-set membership, and
  delete-set overlap. This keeps case-/accent-insensitive collation away from key identity,
  where treating distinct keys as equal would be a transaction-correctness hazard.

## Storage recommendation (guidance only)

ScalarDB does **not** validate collation-vs-storage compatibility — it neither rejects nor
warns, and applies the collation uniformly regardless of backend. Matching the collation to
the storage is the operator's responsibility.

- Use **`BINARY`** for byte-order backends: Cassandra, DynamoDB, PostgreSQL with the `C`
  collation, and MySQL `*_bin` collations.
- Use **`ICU`** (with a matching locale/strength) for UCA-based collations: MySQL 8 defaults
  and PostgreSQL's ICU collation provider.

## Limitations

- **Equality/uniqueness is not collation-aware.** On case-insensitive collations
  (MySQL/SQL Server/Oracle), the backend's `=` and uniqueness are also collation-aware; this
  version matches **ordering** only. Two values that the backend would treat as equal (for
  example `'Apple'` and `'apple'` under a case-insensitive collation) remain distinct keys in
  ScalarDB. In particular, a Consensus Commit scan-after-write whose overlap depends only on
  an **equality** predicate (`WHERE textcol = x`) stays byte-exact and can miss a
  backend-visible overlap under a non-deterministic collation. This is an intentional,
  documented bound. (The clean model to adopt if collation-aware equality is built later is
  PostgreSQL's deterministic vs. nondeterministic collation distinction; it is out of scope
  here because it would change key identity across the snapshot, deduplication, and storage
  keying.)
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

- **An unrecognized `scalar.db.collation.locale` is rejected at startup.** Like an invalid
  `scalar.db.collation`, `scalar.db.collation.strength`, or a malformed
  `scalar.db.collation.rules`, a locale ICU has no collation data for fails fast with an
  `IllegalArgumentException` rather than silently falling back to root-collation ordering.
  Configure a valid ICU locale (for example `en`, `en_US`, `ja`). Note that a recognized language
  with an unknown region (for example `en_XX`) is accepted and uses the language's collation.
