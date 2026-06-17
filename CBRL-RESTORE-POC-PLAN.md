# CBRL Restore — PoC Plan

Plan only (no code yet). Scope is the **restore replay core**, integrated into the `core`
module and using real ScalarDB types, fed by **synthetic** redo-log input. Builds on
`CBRL-RESTORE-DESIGN.md`, `REDO-LOGGING-IMPL-FINDINGS.md`, and the handoff `CBRL-HANDOFF.md`.
SSR (`~/src/scalardb-cluster/replication`) is reference only.

---

## 0. The decisive finding (read first)

This branch already ships the coordinator-side redo-log substrate, but it does **not** match
what the chain-replay design assumes. Confirmed by reading the code:

- `core/src/main/proto/consensus_commit.proto` — `WriteSet → repeated EntryGroup → repeated Entry`.
  An `Entry` carries: `entry_type` (`WRITE`/`DELETE`), `namespace`, `table`, `partition_key`,
  optional `clustering_key`, and `repeated Column columns`.
- `Coordinator.java` — the coordinator `State` row persists this `WriteSet` in the
  `tx_write_set` BLOB column (`Attribute.WRITE_SET`); `State.getWriteSet()` returns
  `Optional.empty()` for NULL (pre-feature / lazy-recovery-abort rows).
- `WriteSetEncoder.java` — `includeColumns=false` records **primary keys only**;
  `includeColumns=true` records partial after-image columns with tx-meta columns
  (`tx_state`, `tx_version`, `before_*`) filtered out.
- **Wiring (verified in code):** the commit path *already* populates `tx_write_set` on every
  write commit — `CommitHandler.commit` → `commitState` (`CommitHandler.java:188`, `:365`) and
  the group-commit emitter both call the encoder. **But every call site hardcodes
  `includeColumns=false`** (`CommitHandler.java:365`/`:431`, `CommitHandlerWithGroupCommit.java:171`/`:190`)
  — so today it's **keys-only, always-on, with no config flag and no backup-window gate**.
  Reaching `includeColumns=true` (full values) needs a **code change**, not config. Two-phase
  commit does **not** log (`TwoPhaseConsensusCommit.java:236` → `commitStateWithoutWriteSet`) —
  a CBRL + 2PC gap.

**Gap vs. the design.** The replay primitive in `CBRL-RESTORE-DESIGN.md` §"Replay primitive"
orders strictly by the per-record **`prev_tx_id → tx_id` chain**. The `tx_write_set` `Entry`
records **neither `prev_tx_id` nor `tx_id`**, and by default **no columns**. So:

| Replay needs | `tx_write_set` provides today | Action for PoC |
|---|---|---|
| `tx_id` of the writing txn | the enclosing coordinator row id (parent) + `EntryGroup.child_id` | derivable — fold into PoC model |
| `prev_tx_id` (the cursor link) | **nothing** | **must be added** (see Decision D1) |
| partial after-image columns | only when `includeColumns=true` | PoC assumes `includeColumns=true` |
| commit membership / consistency point | coordinator `state == COMMITTED` in snapshot | use as-is |

This gap is the single most important thing for the owner to decide, and it is why this is a
plan-for-review, not code. See **§7 Decisions**. The rest of the plan is written so the replay
core is validated **independently** of how D1 is resolved.

---

## 1. Goal & non-goals

**Primary goal — build the integration test first (§6.1).** A real end-to-end test, modeled on
`replication:e2e`'s `E2ETest`, that drives the whole backup/restore lifecycle on a live
ScalarDB (JDBC/Postgres) and proves the restored tables are correct:

> start a write workload → back up the user DB (a *torn* copy, taken while writes run) → back
> up the coordinator DB **last** (= the consistency point) → stop the workload → restore into a separate
> target by replaying the coordinator's `tx_write_set` window onto the torn copy → **compare
> the target tables to the expected state at the consistency point.**

This test is the executable spec: the replay core below exists to make it pass. It exercises
the real `tx_write_set` path, so it forces the D1 decision (§7) into the open instead of hiding
behind synthetic data.

**Supporting goal — the offline restore replay core** the restore step calls:

1. **Pass 1 (shuffle):** read the window redo log; append each write op to bucket
   `hash(key) % N`. All ops for a key land in one bucket ⇒ single owner per key.
2. **Pass 2 (replay):** thread pool `M ≤ N`; each thread owns whole buckets. Per key,
   cursor-driven chain replay; append order within a bucket irrelevant.
3. **Integrity check:** chain connectivity — every non-null `prev_tx_id` resolves to the
   current record or to a logged op; dangling ⇒ fail-loud. (Not "every op applied" — net-zero
   delete-terminated runs are legitimately unapplied.)
4. **Property tests:** confluence (random write set + random apply order ⇒ same final state),
   idempotency (re-run ⇒ same state).

**Non-goals (stubbed/synthetic for the spike).** Real window logging, real coordinator
snapshotting, the fail-closed window flag (concern #2), logical-delete-mode wiring into the
live read path (concern #1), and any production commit-path change. The PoC consumes a
generated `Stream<RedoOp>` (§3, §4.1) so none of these block it.

---

## 2. Where it lives & test tooling

Three source sets, because the integration test (`src/integration-test`) and the unit/property
tests (`src/test`) must both call the replay core — so the core goes in `src/main`:

- **Replay core →** `core/src/main/java/com/scalar/db/transaction/consensuscommit/cbrl/`.
  It's the real (offline) restore tool and must be visible to both test source sets; a
  `src/test`-only package would not compile against `src/integration-test`. This is *not* the
  commit path — adding the package is safe; only D1 (§7) touches the commit path, and that
  stays a gated decision.
- **Unit + property tests →** `core/src/test/java/.../cbrl/` (the §6.2 replay-core tests +
  `RedoLogGenerator`/`ReferenceApplier`).
- **Integration test →** `core/src/integration-test/java/.../cbrl/` (the §6.1 e2e test).
  **Build prerequisite:** core's integration-test source sets whitelist packages with `include`
  globs, so the `integrationTestJdbc` source set in `core/build.gradle` must gain
  `include '**/com/scalar/db/transaction/consensuscommit/cbrl/*.java'` — otherwise the class is
  never compiled or run. Add it to `integrationTestJdbc` only (Postgres backend for the spike).
- **Tooling:** JUnit 5 + AssertJ (already in `core/build.gradle`). **No** property-testing
  library is present (no jqwik/quickcheck); the "property tests" are hand-rolled randomized
  loops over **fixed seeds** (deterministic, per the project's determinism rule). Do not add a
  dependency for the spike.
- **Env:** the integration test needs **PostgreSQL on localhost:5432** (the project's standard
  IT backend); a self-contained IT class wiring storage directly via `StorageFactory` /
  `TransactionFactory` (like `E2ETest`), not the per-backend abstract-base pattern — lighter
  for a spike, promote to the base-class pattern later.
- **Run:** unit/property `./gradlew :core:test --tests '*.cbrl.*'`; integration
  `./gradlew :core:integrationTestJdbc --tests '*.cbrl.*'`.

---

## 3. PoC data model

Reuse the real proto for record/column encoding; add a thin chain wrapper that supplies the
ordering metadata `tx_write_set` lacks (so the replay core is exercised regardless of D1).

- **Reused as-is:** `com.scalar.db.transaction.consensuscommit.proto.v1.{Entry, Column, Key}`
  for `(namespace, table, partitionKey, clusteringKey, entryType, columns)`.
- **`RedoOp` (the replay unit):** `{ txId, prevTxId (nullable), createdAtMillis, Entry entry }`.
  `prevTxId == null` ⇔ INSERT root. The replay code depends only on `RedoOp`, never on how it
  was sourced — the integration test builds them from real coordinator `tx_write_set` rows, the
  property tests from `RedoLogGenerator`. `prevTxId` is whatever D1 (§7) lands on.
- **`RecordKey`:** value type over `(namespace, table, partitionKey, clusteringKey)` with
  `equals`/`hashCode` — the bucketing and single-owner key. (Encode keys deterministically;
  reuse the proto `Key` bytes for hashing.)
- **`RecordState`:** the replayed per-key state — `{ present, txId, mergedColumns (name→Column),
  deleted, insertTxIds }`. `insertTxIds` is the set of applied INSERT tx ids (SSR's `insertTxIds`),
  carried so a re-run can dedup INSERT roots (§6.2 P2 idempotency). The pass-2 output; the
  integration test writes it back to the restore-target tables.

---

## 4. Components

### 4.1 `RedoLogGenerator` (the input)
- Seeded; produces a `Stream<RedoOp>` for the window — random but **chain-consistent**. The
  expected final state is computed separately by the §6.2 reference applier, not here.
- No `RedoLogSource` interface: pass 1 consumes a plain `Stream<RedoOp>`. Whatever produces it
  — the generator now, the deferred Q1 read (coordinator scan vs. dedicated log) later — just
  yields a `Stream<RedoOp>`, so a one-method source interface today is speculative.

### 4.2 Pass 1 — `RecordShuffler`
- Consumes the `Stream<RedoOp>`. For each op, compute `bucket = Math.floorMod(hash(recordKey),
  N)`; append to that bucket. PoC backs buckets with in-memory `List<RedoOp>[N]` (file-backed
  is a scaling TODO, not spike work).
- Invariant asserted: all ops sharing a `RecordKey` land in exactly one bucket.

### 4.3 Pass 2 — `RecordApplier`
- Thread pool `M ≤ N`; each worker owns whole buckets (no intra-key concurrency ⇒ no CAS, no
  locks, physical delete allowed — the design's core simplification vs. SSR).
- Per key within a bucket: `divideWriteOperations` into `insertOperations` / `nonInsertOperations`,
  then walk the chain (§5).
- **Checkpoint:** record per-bucket completion so a crashed re-run skips done buckets
  (idempotency requirement). PoC: an in-memory `Set<Integer> completedBuckets` + a re-run test.

### 4.4 `IntegrityChecker`
- Per key: every `RedoOp` with non-null `prevTxId` must have its `prevTxId` equal to the
  current record's `txId` (from §4.5 `RestoredRecordReader`) **or** to some other op's `txId` on the
  same key. Any dangling link ⇒ throw fail-loud (`CbrlReplayException`). INSERT roots
  (`prevTxId == null`) apply only when the current record is absent or deleted (the loop polls
  `insertOperations` exactly when `currentTxId == null || deleted`).

### 4.5 `RestoredRecordReader` (C4 seam)
- `RecordState get(RecordKey)` → the key's current `RecordState` in the **database being
  restored** (the loaded primary backup image) — the replay cursor's origin and merge target,
  one state per key. PoC supplies synthetic states; in production this read happens **after**
  PREPARED records are resolved via the consistency point + `before_*` (concern #4a). Seam only, not implemented
  in the spike.
- "Restored" is load-bearing: this reads the *user-table* record in the DB being restored — not
  the *coordinator* rows that the redo-op stream is read from (Q1), and not core's
  `consensuscommit.Snapshot`. Both of those are also "reading records"; the name must not blur
  into them.

---

## 5. Replay primitive (mirrors SSR `RecordApplyService`)

Names and control flow follow the battle-tested `RecordApplyService.findWriteOperationsToApply` /
`divideWriteOperations`. Per key, given the record's current state in the database being restored
— `currentTxId` (`null` if absent) and `deleted` — from §4.5 `RestoredRecordReader`, and that
key's ops:

1. **`divideWriteOperations`** — split the key's ops by type. Type is derived (decision A): a
   `WRITE` entry with `prev_tx_id == null` is an INSERT, with `prev_tx_id != null` an UPDATE; a
   `DELETE` entry is a delete. No new enum/oneof — this rests on the encoder invariant that
   `prev_tx_id` is set iff a before-image exists (true at the single `WriteSetEncoder` site).
   - `insertOperations` — a `Deque<RedoOp>` of INSERTs, sorted oldest-first (by `createdAtMillis`).
   - `nonInsertOperations` — a `Map<String, RedoOp>` of UPDATE/DELETE ops **keyed by their
     `prevTxId`**, so the chain is traversed by looking the current tx id up in the map.
2. **Walk the chain** (`while (true)`):
   - if `currentTxId == null || deleted`: `op = insertOperations.poll()`; if empty, stop. Skip an
     INSERT whose txId is already applied to this record (idempotent re-run — see `insertTxIds`).
   - else: `op = nonInsertOperations.remove(currentTxId)`; if none, stop.
   - apply `op`: INSERT/UPDATE ⇒ merge `op.entry.columns` (INSERT also fills unset columns null),
     `deleted = false`; DELETE ⇒ clear columns + fill null, `deleted = true` (logical tombstone;
     physical removal is the post-replay step).
   - `currentTxId = op.txId`.
3. Result = the working columns + `deleted` + the last applied `txId`.

Why it's safe regardless of input order or re-runs:
- **Order-independent within a key:** the chain is followed by `prevTxId → txId`, so
  `nonInsertOperations` can be built in any order; INSERTs are sorted oldest-first only to match
  the likely commit order. A re-INSERT after a DELETE is a **new INSERT root**, reached via the
  `deleted` flag — not chained to the delete (SSR's loop only polls inserts once `deleted`).
- **Idempotent:** re-running from the produced state is a no-op — UPDATE/DELETE are already past
  the cursor, and INSERT roots are deduped via a per-record applied-insert-txId set
  (`insertTxIds`), the only way to dedup roots since they have no inbound chain link. CBRL replay
  carries the same `insertTxIds` for the §6.2 P2 idempotency property.

Notes: `tx_version` is **not** used for ordering — only carried on the final record so
post-restore core stays valid (concern #6, left as a TODO seam for the exact `tx_id`/`tx_version`
core needs to resume).

---

## 6. Tests

### 6.1 Integration test — `CbrlBackupRestoreIntegrationTest`

Modeled on `replication:e2e` `E2ETest`/`E2ETestEnv`: type-rich source tables (reuse E2ETest's
TABLE1 no-CK / TABLE2 multi-PK / TABLE3 with-CK schemas), a `withRetry(manager, tx -> …)`
helper, and a worker thread pool. Differences from SSR's e2e: **no backup site, no
`LogApplier`, no `transaction_groups`, no repl-record tables** — CBRL replays the coordinator's
own `tx_write_set`.

**Fixtures.** One ScalarDB primary on JDBC/Postgres with the Coordinator, window logging on and
`includeColumns=true` (D1′). Source tables in namespace `cbrl_src`; restore-target tables
(identical schema) in `cbrl_restore`.

**Flow** (the requested steps):
1. **start workload** — background threads run `upsert`/`delete` transactions over `cbrl_src`;
   each commit logs its write set to the Coordinator's `tx_write_set`.
2. **back up the user DB** — while writes continue, copy every `cbrl_src` row → `cbrl_restore`
   via `storage.scan`+`put` (cf. `copyAllRecordFromPrimaryToBackupSite`). Intentionally
   **torn**: taken mid-flight, so some keys are stale or missing.
3. **back up the coordinator DB** — snapshot the Coordinator table (id, state, `tx_write_set`)
   into an in-test copy, taken **after** step 2 ⇒ this defines **the consistency point** (C3).
4. **stop workload** — quiesce; run lazy recovery so PREPARED records resolve.
5. **restore** — for each `COMMITTED` row in the coordinator backup, explode its `WriteSet`
   `EntryGroup`s into `RedoOp`s and replay them onto `cbrl_restore` via the §5 core
   (`RecordShuffler` → `RecordApplier`), with `RestoredRecordReader` reading `cbrl_restore`.
6. **compare** — assert `cbrl_restore` equals the expected state at the consistency point, per-key `Get` on
   both sides (cf. `assertPrimaryAndBackupDbTables`).

**The comparison oracle.** "Expected state at the consistency point" needs a deterministic reference. Take the
coordinator backup *after* the workload quiesces (consistency point = everything committed), then **compare
`cbrl_restore` to the live `cbrl_src`**: the torn user-DB copy is repaired by chain replay up
to the consistency point, so it must equal the fully-committed source. Workload is unconstrained
(multi-write keys, delete→re-insert, like E2ETest), so this exercises the real chain (needs
D1 + D1′).

(A mid-flight consistency point — coordinator backup taken *before* stop, so the live primary later diverges
from the consistency point — is deferred: it needs an independent oracle for "state at a prefix," which is
its own mini-project. Not in the first test.)

### 6.2 Replay-core unit & property tests

**Generator** (`RedoLogGenerator`, deterministic, seeded): for `K` keys, emit a random legal
history per key — sequences of INSERT → UPDATE* → DELETE → (re-INSERT as a **new root**) — with
unique `txId`s; UPDATE/DELETE carry the `prevTxId` of the preceding op on that key, INSERTs carry
none (delete→re-insert continuity is via the `deleted` flag + `insertOperations`, validated by the
applied-insert-txId dedup — not a chain link). `ReferenceApplier` computes the **expected final
state** sequentially (the oracle).

**Property tests** (loop over a fixed seed list; assert per the project's determinism rule):
- **P1 Confluence:** shuffle the op list into random order (and across random `N`, `M`) ⇒
  replayed state per key equals the reference state.
- **P2 Idempotency:** run replay twice (second run from the first run's output as the current state) ⇒
  identical state; also re-run with `completedBuckets` pre-seeded ⇒ no change.
- **P3 Connectivity fail-loud:** drop one mid-chain op ⇒ `IntegrityChecker` throws; assert the
  message names the dangling `prevTxId` and key.
- **P4 Single-owner:** assert every key's ops occupy exactly one bucket across random `N`.

**Unit tests** (boundary values per the project's testing rule):
- empty window; single INSERT root; INSERT→DELETE net-zero (unapplied, key absent); partial
  column merge across two UPDATEs; delete→re-insert chain continuity; record-present vs.
  record-absent roots; `N=1`, `N=K`, `M=1`, `M=N`.

### 6.3 Performance evaluation — write-set logging overhead

The viability gate for the `coordinator.tx_write_set` approach: quantify the **commit-time cost
on regular service operations** of populating the write-set, before investing in replay. The
slides scope logging to a window precisely because of this cost; concern #9 flags it. Reuses the
§6.1 harness + Postgres.

Compare three commit configurations under the same workload:
1. **Off (baseline)** — plain consensus commit, no write-set. This is both today's behavior and
   what happens *outside* a backup window.
2. **Keys-only** — `tx_write_set` populated, `includeColumns=false`.
3. **Full columns** — `includeColumns=true` (the mode CBRL restore actually needs).

Per config, measure: commit latency p50/p95/p99, throughput (TPS), coordinator payload/row size
and table growth per txn (and optionally CPU). Vary write-set size (records/txn, column count,
blob sizes) since the BLOB grows with it.

Framing:
- The overhead is **window-scoped** — outside the window config 1 applies (≈ zero extra), so the
  headline is *in-window* overhead and whether it's acceptable for a short window.
- The write-set rides in the **same** coordinator `Put` (no extra round-trip), so the cost is
  serialization + a larger BLOB write + storage growth, not an added operation.
- Levers if it's too high: GZIP the BLOB (SSR compresses its log; `tx_write_set` today does
  not), shorten the window, or reconsider `includeColumns`. This is the empirical input to the
  column-vs-separate-table and `includeColumns` choices.

Method: throughput/latency load runs (not JMH micro — the cost is I/O + payload, not a hot
loop); repeat N times; report deltas vs. baseline as ranges.

---

## 7. Decisions to resolve while prototyping (do not guess silently)

**D1 — how replay gets `prev_tx_id` (gates the §6.1 integration test + production; the §6.2
property tests feed `prevTxId` synthetically and don't need it).** `tx_write_set` has no
`prev_tx_id`. Options:
- **D1a (recommended): add `prev_tx_id` (and `tx_id`) to `Entry`** in the proto + populate in
  `WriteSetEncoder` from the read-set result id (exactly how SSR captures it —
  `result.getId()` at prepare time). Additive, back-compatible (new optional fields). Makes
  `tx_write_set` a true chain log.
- **D1b: reconstruct from before-image** — rejected: the snapshot holds only the latest
  committed version per key, so intra-window ordering can't be recovered from `before_tx_id`
  alone.
- **D1c: order by commit time** — rejected: `created_at`/group commit give no reliable
  per-key total order; the design explicitly relies on the chain, not timestamps.
The replay core depends only on `RedoOp` (D1-agnostic); the plan recommends D1a.

**Decided (A — op type modeling):** keep `ENTRY_TYPE_WRITE` and derive INSERT vs UPDATE from
`prev_tx_id == null`. The `oneof` form (SSR's `WriteOperation`) is the type-safest but **rejected**:
`Entry` is the released, shared proto for keys-only active-recovery too, so the oneof's benefit
requires relocating existing fields — wire-incompatible, breaks reading persisted `tx_write_set`.
An additive `INSERT`/`UPDATE` enum was the fallback but adds nothing for correctness given the
encoder invariant. Reserve the oneof for a future dedicated CBRL log message (no released shape to
preserve).

**D1′ — `includeColumns` must be `true` for the backup window.** Chain replay merges partial
columns, so the window must persist them; keys-only is insufficient. **Verified:** every commit
call site hardcodes `includeColumns=false`, with no flag — so this is a **code change** at the
call sites, not a config toggle, and there's no backup-window gate to hang it on yet (today
logging is always-on keys-only). The spike must add the `includeColumns=true` path and decide
whether to gate it by a window flag now or leave it always-on.

**Q1 — window-log read path.** `coordinator.state` is hash-partitioned by `tx_id` with no time
clustering, so "read every op from window start" is a full scan (concern #5). In the PoC the
shuffle just consumes a `Stream<RedoOp>`; the production choice (full coordinator scan vs.
dedicated range-scannable log) stays open and is fed synthetic ops until decided.

**C3 — the consistency point.** Coordinator snapshot must be atomic, strictly last, and group-atomic
(all `child_ids` of a row in or out). Out of PoC scope (no real snapshotting) but the replay
must treat an `EntryGroup` set as all-or-nothing per coordinator row (concern #8); the PoC
models a coordinator row as `{ txId, state, List<EntryGroup> }` and explodes children into
`RedoOp`s only when `state == COMMITTED`.

**C4 — record recovery before replay.** Records PREPARED in the backup image resolved via
the consistency point + `before_*` to a clean committed state **before** chain replay anchors. Modeled as the
§4.5 `RestoredRecordReader` seam; confirm `before_*` is present in the backup image. (SSR's
`BackupDbTableRepository.rollforward`/
`rollback` via `CommitMutationComposer`/`RollbackMutationComposer` is the concrete reference
pattern.)

**TODO (deferred) — backup-mode flag (the "is CBRL window open?" switch).** Slides 4/6–8: a
durable, runtime-readable flag every embedded-Core process observes before DB backups start
(Cluster can push it like pause). Open points, not decided here:
- **One flag gating both** mutation-logging and logical-delete mode (they must flip together —
  concern #2), stored with a **TTL/expiry** so a crashed backup process can't pin the window
  open (slide 10). Possibly a window epoch/id.
- **The hard part is fail-closed visibility, not the storage:** the slides' cache-poll + wait is
  a heuristic a GC pause/partition defeats; concern #2 requires a node that can't confirm
  window mode to not commit in a chain-breaking way. The table is necessary, not sufficient.
- Where it lives (Core table vs Cluster push; one shared source of truth?) — TBD.
This is on the logging side (§10 out of scope for this spike); listed so it isn't lost.

---

## 8. File layout (proposed)

```
core/src/main/java/com/scalar/db/transaction/consensuscommit/cbrl/   // the replay core
  RedoOp.java                  // unit: txId, prevTxId, createdAtMillis, Entry
  RecordKey.java               // (ns, table, pk, ck) value type
  RecordState.java             // present, txId, mergedColumns, deleted, insertTxIds
  RestoredRecordReader.java    // C4 seam: RecordState get(RecordKey)
  RecordShuffler.java          // pass 1
  RecordApplier.java           // pass 2 (thread-per-bucket) + checkpoint
  IntegrityChecker.java        // chain-connectivity fail-loud
  CbrlReplayException.java

core/src/test/java/com/scalar/db/transaction/consensuscommit/cbrl/    // replay-core tests
  RedoLogGenerator.java        // seeded: Stream<RedoOp>, chain-consistent
  ReferenceApplier.java        // trivial sequential applier → expected final state (oracle)
  ReplayCoreTest.java          // unit tests (§6.2)
  ReplayPropertyTest.java      // P1–P4 (§6.2)

core/src/integration-test/java/com/scalar/db/transaction/consensuscommit/cbrl/
  CbrlBackupRestoreIntegrationTest.java   // §6.1 e2e (Postgres)
```

## 9. Milestones (integration test first)

1. **Harness + `includeColumns=true` path** — `E2ETest`-style env on Postgres (source +
   restore-target tables, workload, torn user-DB copy, coordinator-DB copy, per-key compare).
   `tx_write_set` is already logged keys-only on every commit; this milestone adds the
   `includeColumns=true` code path at the commit call sites (D1′ — a code change, no flag
   exists), optionally behind a window gate.
2. **Performance evaluation (§6.3)** — commit latency/TPS/storage for off vs keys-only vs
   full-columns. **Viability gate for the `tx_write_set` approach** — run before investing in
   replay; if in-window overhead is unacceptable, revisit compression/window/`includeColumns`
   first.
3. **Chain replay, end-to-end (§6.2 + §6.1 green)** — replay core (`RecordShuffler`,
   `RecordApplier`, `IntegrityChecker`, `RestoredRecordReader`) plus the chain metadata it orders
   by (`prev_tx_id`/`tx_id` on `Entry`, populated in `WriteSetEncoder` from the read-set result —
   D1a, **already implemented on this branch**); `RedoLogGenerator`/`ReferenceApplier` + property
   tests P1/P2/P4 + connectivity P3 + unit boundary list (§6.2); then the §6.1 e2e green —
   unconstrained multi-write workload, chain replay, compare-to-quiesced-`cbrl_src`. The three
   are one indivisible slice: nothing here independently delivers "restore works."
4. **C3/C4 + restart** — coordinator-last consistency point handling, PREPARED resolution in
   `RestoredRecordReader`, per-bucket checkpoint/idempotent re-run; write D1/Q1/C3/C4 findings
   back into the design docs.

## 10. Explicitly out of scope (spike)

The real window read over the coordinator; window flag / fail-closed boundary (concern #2);
logical-delete-mode on the live read path (concern #1); writing replayed state back to a real
primary; the exact post-restore `tx_id`/`tx_version` stamping for core resume (concern #6 —
left as a TODO seam).

**Known gap — CBRL + two-phase commit.** `TwoPhaseConsensusCommit` commits via
`commitStateWithoutWriteSet`, so 2PC transactions log no write-set and would be invisible to
replay. The spike targets single/normal + group commit only; covering 2PC is a separate
follow-up.
