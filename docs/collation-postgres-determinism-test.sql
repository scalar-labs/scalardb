-- Verify PostgreSQL's deterministic-vs-nondeterministic collation semantics that ScalarDB's
-- scalar.db.collation.deterministic setting is modeled on.
--
-- Question: does a CASE-INSENSITIVE ICU collation created with deterministic = true affect
-- predicate evaluation and equality (=), or only ordering?
--
-- Expected (PostgreSQL 15+ with ICU): deterministic = true  -> ordering is case-insensitive, but
-- =, WHERE, and UNIQUE stay BYTE-EXACT ('Apple' != 'apple'); deterministic = false -> =, WHERE,
-- and UNIQUE become case-insensitive.
--
-- Run against a scratch database; each block prints what it proves.

\echo '== PostgreSQL version =='
SELECT version();

-- Two case-insensitive (primary-strength / level1) ICU collations: one deterministic, one not.
DROP COLLATION IF EXISTS ci_deterministic;
DROP COLLATION IF EXISTS ci_nondeterministic;
CREATE COLLATION ci_deterministic    (provider = icu, locale = 'und-u-ks-level1', deterministic = true);
CREATE COLLATION ci_nondeterministic (provider = icu, locale = 'und-u-ks-level1', deterministic = false);

\echo ''
\echo '== 1) Direct predicate equality with explicit COLLATE =='
\echo '   expected: eq_deterministic = false, eq_nondeterministic = true'
SELECT ('Apple' = 'apple' COLLATE ci_deterministic)    AS eq_deterministic,
       ('Apple' = 'apple' COLLATE ci_nondeterministic) AS eq_nondeterministic;

\echo ''
\echo '== 2) WHERE = over a column of each collation =='
\echo '   expected: det_matches = 1 (only ''apple''), nondet_matches = 2 (''Apple'' and ''apple'')'
DROP TABLE IF EXISTS t_det, t_nondet;
CREATE TABLE t_det    (v text COLLATE ci_deterministic);
CREATE TABLE t_nondet (v text COLLATE ci_nondeterministic);
INSERT INTO t_det    VALUES ('Apple'), ('apple');
INSERT INTO t_nondet VALUES ('Apple'), ('apple');
SELECT (SELECT count(*) FROM t_det    WHERE v = 'apple') AS det_matches,
       (SELECT count(*) FROM t_nondet WHERE v = 'apple') AS nondet_matches;

\echo ''
\echo '== 3) UNIQUE: does the collation collapse ''Apple'' and ''apple'' as one key? =='
\echo '   expected: deterministic UNIQUE accepts both (2 rows); nondeterministic UNIQUE rejects the 2nd'
DROP TABLE IF EXISTS u_det, u_nondet;
CREATE TABLE u_det    (v text COLLATE ci_deterministic UNIQUE);
CREATE TABLE u_nondet (v text COLLATE ci_nondeterministic UNIQUE);

INSERT INTO u_det VALUES ('Apple');
INSERT INTO u_det VALUES ('apple');   -- deterministic: byte-distinct => SUCCEEDS
SELECT count(*) AS u_det_rows FROM u_det;  -- expected: 2

INSERT INTO u_nondet VALUES ('Apple');
\echo '   next INSERT is expected to FAIL with a unique_violation:'
INSERT INTO u_nondet VALUES ('apple');   -- nondeterministic: equal => unique violation

\echo ''
\echo '== 4) ORDERING under the deterministic CI collation (should still group case-insensitively) =='
\echo '   expected order: Apple, apple, BANANA, banana (case-insensitive grouping, byte tie-break)'
SELECT v FROM (VALUES ('banana'), ('Apple'), ('apple'), ('BANANA')) AS x(v)
ORDER BY v COLLATE ci_deterministic;
