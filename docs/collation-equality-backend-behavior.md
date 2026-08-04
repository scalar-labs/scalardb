# Collation-aware equality: per-backend behavior (empirically verified)

Companion to the PostgreSQL determinism test (`collation-postgres-determinism-test.sql`). Verifies,
on each ScalarDB-supported RDBMS that has a case-insensitive (CI) collation, whether that collation
makes **`=`/`WHERE` (predicate equality)** and **`UNIQUE`/key** comparisons case-insensitive — and
whether the backend has PostgreSQL's **deterministic/nondeterministic** distinction (CI *ordering*
without CI *equality*).

Run via `dbt up <db>`; each row was executed against the version shown (August 2026).

## Results

| Backend | Version | CI collation used | CI `=` / `WHERE` | CI `UNIQUE` / key | `deterministic` toggle (CI ordering *without* CI equality)? |
|---|---|---|---|---|---|
| **PostgreSQL** | 17.9 | `icu … ks-level1` | **only if `deterministic=false`** (deterministic → byte-exact) | **only if `deterministic=false`** | **YES — unique among these** |
| **MySQL** | 8.4.8 | `utf8mb4_0900_ai_ci` | yes | yes (2nd insert → dup) | no |
| **MariaDB** | 11.4 | `utf8mb4_general_ci`, `utf8mb4_uca1400_ai_ci` | yes | yes (2nd insert → dup) | no |
| **SQL Server** | 2022 | `SQL_Latin1_General_CP1_CI_AS` | yes | yes (2nd insert → dup) | no |
| **Oracle** | 23c Free | `BINARY_CI` / `BINARY_AI` (`COLLATE` operator) | yes | yes¹ | no |
| **IBM Db2** | 11.5 / 12.1 | UCA `CLDR…` primary strength (DB-level `COLLATE USING`) | yes (documented²) | yes (documented²) | no |

¹ Oracle column-level collation (for a `UNIQUE`/key column) requires `MAX_STRING_SIZE=EXTENDED` (a
DB-restart parameter); the `COLLATE BINARY_CI/AI` operator confirms predicate equality without it.
² Db2 sets collation at database creation (`COLLATE USING`), not per query, and has no `COLLATE`
operator — its UCA (CLDR/ICU-based) collations at primary strength are case-/accent-insensitive for
both `=` and uniqueness; not re-run here (would require creating a CI-collated database). See
`collation-storage-compatibility.md`.

## Key finding

**PostgreSQL is the only supported backend with the deterministic/nondeterministic distinction.** On
MySQL, MariaDB, SQL Server, Oracle (and Db2), a CI collation is *all-or-nothing*: it makes `=`,
`WHERE`, **and** `UNIQUE`/keys case-insensitive together — there is no "case-insensitive ordering but
byte-exact equality" mode. So:

- ScalarDB's `scalar.db.collation.deterministic` flag models **PostgreSQL** specifically (deterministic
  = ordering-aware, equality byte-exact; nondeterministic = equality-aware).
- On the other RDBMS, if an operator configures a CI collation to get case-insensitive `=`, that same
  collation *also* makes the PK/unique index case-insensitive. There is no way to have one without the
  other. So matching such a backend requires **both** ScalarDB's predicate equality
  (`deterministic=false`) **and** phase-2 key identity — the two are inseparable on those engines.
- The NewSQL/cloud variants inherit their base engine: TiDB / Aurora MySQL behave like MySQL; Aurora
  PostgreSQL / AlloyDB / YugabyteDB behave like PostgreSQL; Cloud Spanner is Unicode-code-point
  (byte) order with no CI collation. Cassandra, DynamoDB, and Cosmos DB have no collation concept
  (always binary) and are out of scope for collation-aware equality.

## SQL used per backend

**PostgreSQL** — see `collation-postgres-determinism-test.sql`.

**MySQL / MariaDB** (`mysql` client):
```sql
SELECT ('Apple'='apple' COLLATE utf8mb4_0900_ai_ci) AS eq_ci,   -- 1 (MySQL)
       ('Apple'='apple' COLLATE utf8mb4_bin)          AS eq_bin;  -- 0
-- MariaDB: utf8mb4_general_ci / utf8mb4_uca1400_ai_ci both -> 1
CREATE TABLE u_ci (v varchar(50) COLLATE utf8mb4_0900_ai_ci UNIQUE);
INSERT INTO u_ci VALUES ('Apple');
INSERT INTO u_ci VALUES ('apple');   -- ERROR 1062 duplicate
```

**SQL Server** (`sqlcmd`):
```sql
SELECT IIF('Apple'='apple' COLLATE SQL_Latin1_General_CP1_CI_AS,'EQUAL','ne');  -- EQUAL
SELECT IIF('Apple'='apple' COLLATE Latin1_General_BIN2,'EQUAL','ne');           -- ne
CREATE TABLE #u_ci (v varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS UNIQUE);
INSERT INTO #u_ci VALUES ('Apple');
INSERT INTO #u_ci VALUES ('apple');   -- Msg 2627 unique violation
```

**Oracle** (`sqlplus`, `COLLATE` operator; column collation needs `MAX_STRING_SIZE=EXTENDED`):
```sql
SELECT CASE WHEN 'Apple'='apple' COLLATE BINARY_CI THEN 'EQUAL' ELSE 'ne' END FROM dual;  -- EQUAL
SELECT CASE WHEN 'Apple'='apple' COLLATE BINARY    THEN 'EQUAL' ELSE 'ne' END FROM dual;  -- ne
SELECT COUNT(*) FROM (SELECT 'Apple' v FROM dual UNION ALL SELECT 'apple' FROM dual)
  WHERE v='apple' COLLATE BINARY_CI;   -- 2
```
