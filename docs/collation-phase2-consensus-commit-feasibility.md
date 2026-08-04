# Phase 2 (collation-aware key identity) — Consensus Commit feasibility

Investigation of whether a **case-insensitive nondeterministic collation** (collation governs both
ordering *and* equality/identity) can be supported in the Consensus Commit layer, under the
**"alignment + localized canonical key"** design:

- ScalarDB's in-memory logical key (`Snapshot.Key`, dedup, map keying) becomes **collation-canonical**
  via `com.scalar.db.io.CollationComparator`; `Key`/`Column` `equals()`/`hashCode()` are **not**
  changed globally.
- Stored record key bytes are **preserved** (no normalization).
- Physical PK uniqueness is enforced by the **backend's matching CI collation** — RDBMS only
  (MySQL/PostgreSQL/SQL Server/Oracle CI); Cassandra/DynamoDB/Cosmos are excluded (no CI collation).

Based on a three-part read of `Snapshot`, the commit/coordinator/recovery/serialization path, and
`CrudHandler`/`MutationsGrouper`/2PC. Line references are indicative and may drift.

## Verdict

A CI nondeterministic collation **breaks the Consensus Commit layer as written**, but is **feasible
under the alignment design** as a substantial, delicate change. On the intended RDBMS+CI backends,
most current failures fail **safe** (transaction aborts); a handful are genuinely **silent**
wrong-answer paths. The canonical-key change *repairs* today's behavior — byte-exact identity on a
CI backend already causes spurious aborts.

## The core hazard: split key provenance

`Snapshot.Key`/`io.Key` `equals`/`hashCode` are byte-exact (`Snapshot.java` ~1039-1057 →
`TextColumn.equals`). The snapshot maps receive keys from **two provenances** that differ on a CI
backend for the *same* logical row:

- **request/mutation bytes** — the app's Get/Put/Delete key (`'apple'`): `CrudHandler.java` ~108, 583, 607.
- **storage-returned bytes** — what's physically stored (`'Apple'`): scan/index-get results,
  `CrudHandler.java` ~220, 264, 441.

Every site that looks up a request-keyed entry with a storage-returned key (or vice-versa) misses.
That is the whole bug.

## Concrete breaks

| Site | Scenario | Failure | Mode |
|---|---|---|---|
| `Snapshot.to()` before-image join | `get('Apple')` → `put('apple')` | before-image not found → insert branch → `putIfNotExists` hits existing CI row → `PreparationConflictException` | fail-safe abort |
| `putIntoWriteSet` / `putIntoDeleteSet` merge + guards | `put('Apple')` + `put('apple')` in one tx | two writeSet entries; merge & insert-guard bypassed → two mutations on one row | fail-safe (self-conflict) |
| `mergeResult` (read-your-own-writes) | write `'apple'`, read `'Apple'` | returns **stale** pre-write value, no error | **SILENT** |
| SERIALIZABLE `validateScanResults` | tx wrote `'apple'`, re-scan returns `'Apple'` | self-written row misclassified as changed-by-another → spurious `ValidationConflictException` | fail-safe (wrong abort) |
| Overlap `results.containsKey` for `ScanWithIndex`/`ScanAll` | scan-after-write collision | overlap **missed** → `SCANNING_ALREADY_WRITTEN` guard skipped → inconsistent scan | **SILENT** |
| Secondary-index `resultMatchesIndexKey` (`CrudHandler`) | index query `'apple'`, stored `'Apple'` | row filtered out (`indexKeyFilteredOut`) → wrong results | **SILENT** |

## Two independent keying families must adopt the canonical key in lockstep

1. **`Snapshot.Key.equals/hashCode`** — all five maps (`readSet`/`writeSet`/`deleteSet`/`getSet`/`scanSet`)
   + merge/dedup + the before-image join in `to()`.
2. **`MutationsGrouper.MutationGroup.equals/hashCode`** — *independent*. Even with a canonical
   `Snapshot.Key`, two collation-colliding mutations still land in separate storage batches → separate
   ops on one CI row.

Two things a `Snapshot.Key` canonicalization does **not** fix on its own: `getSet` is keyed by the
whole **`Get` object** (byte-exact columns), and the **secondary-index** value comparison. In total
**~20 membership sites** span `Snapshot` + `CrudHandler` + `MutationsGrouper`; any missed one silently
reintroduces the split for whatever op-mix reaches it.

## What is safe

- **Coordinator** — keyed by `tx_id` only; record keys live inside an opaque write-set BLOB.
- **Recovery** — matches writers by `tx_id`, rebuilds keys from stored bytes; the decoded write-set
  key is used only as a `storage.get` argument.
- **Write-set serialization** — byte-exact round-trip preserved; consumer is CI-tolerant.
- **2PC** — collisions cannot span participants (same partition = one participant).
- **Commit conditions** — on `ID`+`STATE`, not key bytes.
- **Ordering paths** — already collation-aware (the shipped ordering feature).

## A stricter backend invariant this surfaces

Under alignment, the backend collation must cover **recovery point-reads** (`storage.get` resolution),
not merely PK uniqueness. A partially-CI or mismatched RDBMS would **silently break recovery** — a
write-set key stored as `'apple'` fails to resolve a record stored as `'Apple'`, leaving it
`PREPARED`. The operator requirement is therefore stricter than "matching PK collation": the same CI
collation must govern point-get resolution too.

## Bottom line

- Feasible under alignment, and it *repairs* today's spurious-abort behavior on CI backends.
- But materially bigger and riskier than the predicate-equality feature: one canonical-key function
  threaded through **two keying families + ~20 sites in lockstep**, with `getSet`/secondary-index
  needing separate handling, genuine **silent-wrong-answer** paths if done partially, and a stricter
  backend invariant.
- This risk profile argues for a **separate, explicit opt-in** for key identity (off by default,
  ideally usable only on validated CI-RDBMS backends), so the safe predicate-equality feature
  (`scalar.db.collation.deterministic=false`) does not silently drag it in.
