# Consensus Commit: Read-Your-Writes for Scans

Status: draft, 2026-10-02. Nothing implemented yet.

## Problem

Within a Consensus Commit transaction, a scan that touches a record the same transaction has already written or deleted fails:

```
DB-CORE-10106: Scanning data already-written or already-deleted by the same transaction is not allowed.
```

A Get in the same situation works. It returns the stored record with the transaction's own write applied. A scan instead refuses to run. This is the main gap between Consensus Commit and the read-your-writes behavior SQL users expect. Through the PostgreSQL frontend, ordinary sequences like these fail inside `BEGIN`:

```sql
UPDATE emp SET bonus = bonus * 2 WHERE dept_id = 3;
SELECT id, bonus FROM emp WHERE dept_id = 3;        -- DB-CORE-10106

INSERT INTO dept VALUES (100, 'new', ...);
SELECT * FROM dept WHERE id >= 100;                 -- DB-CORE-10106

DELETE FROM emp WHERE id = 80;
SELECT count(*) FROM emp;                           -- DB-CORE-10106
```

In the differential test against native PostgreSQL (`postgres-frontend/difftest`), 11 of the 31 write cases inside a transaction hit this error. The frontend's own UPDATE/DELETE with a non-key WHERE clause is a scan followed by writes. It therefore also fails when an earlier statement in the transaction wrote a row in that range.

TPC-C (BenchBase) is mostly unaffected: its transactions scan before they write. ORMs that flush and then query, and hand-written multi-statement transactions, are affected.

## Current behavior

| Piece | Where | What it does |
|---|---|---|
| Get merge | `Snapshot.mergeResult` (`Snapshot.java:324`) | Drops a deleted key. For a written key, overlays the Put on the read-set record (`MergedResult`), then re-checks the Get's conjunctions. |
| Overlap check | `Snapshot.verifyNoOverlap` / `getKeyOverlappedWithWriteSetOrDeleteSet` (`Snapshot.java:430`) | Rejects a scan if any written or deleted key is in its results (cases 1 and 2). It also rejects a scan if a Put could make a record match the scan's range or conditions (case 3). |
| Batch scan | `CrudHandler.scan` → `scanInternal` → `verifyNoOverlap` (`CrudHandler.java:409`) | Collects all rows, then checks overlap with an empty exempt set. |
| Streaming scan | `ConsensusCommitStorageScanner` / `ConsensusCommitSnapshotScanner` (`CrudHandler.java:970`, `:1147`) | Checks overlap at close. Keys the scanner returned before they were written are exempt (#3850), so scan-then-write works and write-then-scan does not. |
| Storage scan | `ConsensusCommitUtils.prepareScanForStorage` | Already runs **without a limit**, because recovery and conjunction filtering can drop rows. The limit is applied in `CrudHandler` after filtering. |
| Validation | `Snapshot.validateScanResults` (`Snapshot.java:721`) | Re-scans and compares with the recorded results. It already skips records this transaction wrote (`tx_id == id`), and keys in the write or delete set. |
| Scan cache | `Snapshot.getResults(scan)` | A repeated identical scan returns the recorded results without reading storage. |

The Javadoc on `verifyNoOverlap` gives the reason for the restriction. Merging writes into a scan is subtle once limits and ordering are involved, so ScalarDB "currently avoids such overlaps instead of attempting to handle more intricate scenarios".

## Goal and semantics

A scan in transaction T returns what the same scan would return over T's snapshot with T's buffered writes applied:

> result(scan) = scan(apply(writeSet ∪ deleteSet, snapshot read by T))

Concretely:
- Records T deleted are absent.
- Records T wrote appear with T's values. They are included or excluded according to the scan's range, index value and conjunctions **evaluated on the merged image**.
- Records T inserted appear if they match.
- Order and limit apply to the merged set.

**Streaming scanners.** A scanner sees the write set **as of the moment it was opened**. A write issued while the scanner is open does not change what that scanner returns. This matches PostgreSQL cursor behavior and the reasoning behind #3850's exempt keys ("run the whole scan, then apply the write"). A new scan or scanner opened after the write does see it.

**Non-goals:**
- Changing what other transactions see.
- Changing commit or validation semantics.
- Merging for read-only or one-operation transactions. They have no writes, and `isOverlapVerificationRequired` is already false for them.

## Design

### 1. Own rows for a scan

When a scan or scanner opens, compute **own rows** O from the write set and delete set entries on the scan's table:
- **Deleted keys:** recorded as "drop" and never emitted.
- **Written keys:** a cheap key-range pre-filter comes first:
  - **Partition `Scan`:** the partition key must equal the scan's, and the clustering key must fall within the start/end range. Keys are known, so no image is needed.
  - **`ScanWithIndex` and `ScanAll`:** no key pre-filter is possible; every written key on the table is a candidate.
- **The image of each candidate** is `MergedResult(base, put)`. The base is chosen as follows:
  - The read-set record, if present. This is the version T will overwrite, and the version Get already uses.
  - Empty, if the Put is in insert mode, or if it came from a delete-then-put (`putIntoWriteSet` already nulls the unspecified columns).
  - Otherwise, a Get through the normal read path. This records the key in the read set and the get set, so validation and commit treat it like any other read. With implicit pre-read, the default, this Get would have happened at commit anyway (`readIfImplicitPreReadEnabled`). This just moves it earlier.
- **Keep the candidate** if the image matches the scan:
  - the index column value for `ScanWithIndex`;
  - `ScalarDbUtils.columnsMatchAnyOfConjunctions` on the conjunctions, which supports LIKE.

### 2. Merge with the storage stream

The storage stream is unlimited, recovered and filtered exactly as today. Rows whose key is in the write or delete set are **skipped**. Their merged version, if it still matches, is in O. Every other row passes through unchanged.

**The ordered merge depends on the scan type:**

| Scan | Order | Placing own rows |
|---|---|---|
| Partition `Scan` | Clustering key, with the scan's orderings applied over the clustering order. | Comparator on clustering key columns. An updated row cannot move because keys are immutable. It lands where the storage row would have been. |
| `ScanAll` with orderings | Ordering columns, with ties in an undefined order. | Comparator on the ordering columns of the merged image. An updated ordering column moves the row, which is correct because the stale storage copy was skipped. |
| `ScanAll` without orderings, `ScanWithIndex` | Undefined | Emit O after the stream, or before it; either is valid. |

**Streaming:** before emitting stream row r, emit every own row that sorts before r. At stream end, emit the rest. The batch path is the same loop collected into a list.

**Comparator:** `Column` natural order, the same as `objectstorage.ClusteringKeyComparator`. See the next section for when this is safe.

### 3. Ordering of TEXT

The merge is only correct if the comparator agrees with the order in which storage returns rows. For numeric, boolean, date/time and BLOB columns, natural order is unambiguous. For TEXT it depends on the backend:
- **JDBC:** ScalarDB sets no collation (there is no `COLLATE` in `storage/jdbc`). PostgreSQL uses the database collation, which was `ja_JP.UTF-8` in the frontend test environment. MySQL uses the column collation, and so on.
- **Cassandra, DynamoDB, object storage:** binary or code-point order. Java's `String.compareTo` is UTF-16 order, which differs from UTF-8 byte order only for supplementary characters versus U+E000–U+FFFF.

**Rule:** if an own row must be **placed** by a comparator on a TEXT column, and the storage does not guarantee binary TEXT order, keep throwing the existing DB-CORE-10106 with a message naming the reason. This happens only for:
- inserts into a TEXT-clustered partition scan;
- `ScanAll` orderings on TEXT.

Rows that keep their position, and unordered scans, never need the comparator. See open question 1 for a way to remove this restriction.

### 4. Limit

The storage scan is already unlimited. The merge loop stops when it has emitted `limit` rows. No extra fetching logic is needed.

### 5. Validation (SERIALIZABLE)

The scan set and scanner set keep recording the **raw** storage rows consumed, as today, not the merged output. Validation already ignores T's own records in both the recorded and the latest results.

One change is needed. Today's early exit `scan.getLimit() != 0 && results.size() == scan.getLimit()` assumes the recorded rows are exactly the first `limit` rows. With merging, a limited scan can stop after consuming more or fewer raw rows than `limit`. The recorded entry therefore needs a flag saying the stream was not consumed to the end. Validation then treats it as `notFullyScannedScanner = true`: it compares the consumed prefix and does not check beyond it. Rows beyond the prefix were never observed, so a phantom there is not an anti-dependency.

Before-index checks (`validateBeforeIndex`) are unchanged.

### 6. Scan cache

`snapshot.getResults(scan)` keeps returning the raw cached rows. The merge runs on top of them, the same as on a fresh storage stream. A repeated identical scan after further writes therefore reflects those writes. Today that case is an error.

### 7. What gets removed

- `verifyNoOverlap` and `getKeyOverlappedWithWriteSet*` are reduced to the TEXT-placement fallback in section 3.
- The exempt-key tracking (#3850) is no longer needed. The scanner merges the write set as of open, and later writes are invisible to it by design.

## Edge cases

| Case | Behavior |
|---|---|
| Put, then delete of the same key | Already moved to the delete set: dropped. Deleting a key inserted in the same transaction is already rejected (`DELETING_ALREADY_INSERTED`). |
| Delete, then put | `putIntoWriteSet` nulls the unspecified columns and clears insert mode; the base is empty. |
| A Put that makes a row stop matching | The storage row is skipped, and the image fails the conjunctions, so the row is absent. |
| A Put that makes a row start matching (old case 3) | The image matches, so the row is in O and placed by the comparator. |
| Projections | The full image is merged first, then `FilteredResult` projects it, so the Put does not need to carry the projected columns. |
| Index scan where the Put changes the index column | Matched on the image's index value. A row moved out is dropped, and a row moved in is included. |
| Blind write, implicit pre-read disabled, not yet read | An image is needed only if the key passes the key pre-filter. That costs one Get, or the existing error if we prefer not to read for a blind write (open question 2). |
| Concurrent change to a row T updated | T merges onto its read-set version, the same as Get. The conflict surfaces at commit, as today. |

## Cost

- **Per scan:** one pass over T's write and delete entries for that table. Today's overlap check already passes over the whole write set on every scan; indexing the write set by table would make it cheaper than today.
- **Extra Gets:** only for written keys that pass the pre-filter, are not in the read set, and are not inserts. With implicit pre-read those Gets would have happened at commit anyway.
- **Hot path:** no change for scans with no own writes on the table, which is the common case including TPC-C.

## Plan

1. **Phase 1: no ordering reconstruction.**
   - Partition scans where own rows only stay in place or disappear (updates and deletes of existing rows).
   - All unordered `ScanAll` / `ScanWithIndex` cases.
   - Validation flag (section 5) and scan-cache merge (section 6).
   - Inserts and newly matching rows into a partition scan are also allowed when the clustering key types are non-TEXT.
   - **Covers:** in the difftest, every failing case except inserts into a TEXT-ordered result.
2. **Phase 2: comparator placement for `ScanAll` orderings, and the TEXT rule.** Whatever open question 1 decides.
3. **Remove the exempt-key machinery** once both scanner types merge.

## Tests

- **Unit (`SnapshotTest`, `CrudHandlerTest`):** the merge loop for each scan type with these inputs:
  - deletes;
  - in-place updates;
  - updates that stop matching;
  - updates that start matching;
  - inserts before, between and after storage rows;
  - limits that cut inside own rows and inside storage rows;
  - repeated scans hitting the scan cache;
  - a scanner opened before a write, which must not see it.
- **Validation:** limited scans with own rows. A phantom inserted by another transaction inside the consumed prefix must still conflict; a phantom beyond the prefix must not.
- **Integration (`ConsensusCommitSpecificIntegrationTestBase`, `ConsensusCommitCrossPartitionScanIntegrationTestBase`):** the existing `scan_*OverlappingPut*_ShouldThrowIllegalArgumentException` tests (e.g. lines 6439, 6503, 6526, 6733, and the cross-partition ones) flip to asserting merged results. Keep one test for the TEXT fallback.
- **End to end:** rerun `postgres-frontend/difftest` (`cases_write.sql`); the DB-CORE-10106 cases must match PostgreSQL.

## Open questions

1. **TEXT ordering guarantee.** Options:
   - (a) Keep the fallback error, so behavior stays as today where it matters.
   - (b) Let each storage declare whether its TEXT order is binary. JDBC would report "binary" only when it can verify the collation.
   - (c) Create JDBC key and ordering columns with a binary collation (`COLLATE "C"` on PostgreSQL, `utf8mb4_bin` on MySQL). This gives one ordering across adapters, but changes the schema and needs a migration story.
   
   (c) also fixes the frontend's inconsistency between pushed-down and in-memory text comparison.
2. **Blind writes with implicit pre-read disabled.** Read the base at scan time, which costs a Get the user opted out of, or keep the error for that narrow case?
3. **Who calls the Get.** Should the image Get go through `read()`, which adds the key to the get set and validates it under SERIALIZABLE, or through a storage read that only fills the read set? The former is simpler and certainly correct.

## Alternative considered: merging in the frontend

The PostgreSQL frontend could keep its own per-transaction write log and overlay it on scan results. It would need to duplicate the write set, the read-set base images, condition evaluation and the ordering rules. It would still be unable to fix the validation and limit interaction, which lives in `Snapshot`. Core already has every piece, and the fix benefits every ScalarDB user, so core is the better place.
