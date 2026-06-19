# CBRL Restore — PoC Plan

Plan only (no code yet). Scope is the **restore replay core**, integrated into the `core`
module and using real ScalarDB types, fed by **synthetic** redo-log input. Builds on
`CBRL-RESTORE-DESIGN.md`, `REDO-LOGGING-IMPL-FINDINGS.md`, and the handoff `CBRL-HANDOFF.md`.
SSR (`~/src/scalardb-cluster/replication`) is reference only.

> **CRITICAL RULES (highest priority — these override every other section):**
>
> 1. **Restore MUST NOT use `tx_version` or `created_at`** for ordering or for any replay decision.
>    `created_at` is wall-clock and clock-skew-prone (CBRL is multi-primary — different processes
>    commit the same key with unsynchronized clocks), and `tx_version` resets on delete→re-insert;
>    neither is a reliable per-key order. Restore is ordered **only** by the `prev_tx_id → tx_id`
>    chain. Both fields may be *carried* on the redo so a restored record stays valid for
>    post-restore core, but the replay must never read them to decide what, or in what order, to
>    apply. Any code path that consults `created_at`/`tx_version` to order or gate replay is a bug.
> 2. **Use precise, authorized terminology — no jargon or ambiguous coinages.** Do not write "cut"
>    or "torn". Use the established unambiguous term: a backup taken by a live, non-atomic scan is a
>    **non-snapshot-consistent** copy (never "torn"); the committed-transaction boundary is the
>    **consistency point** (never "cut"). Likewise, never write bare **"window"** — it conflates the
>    logging side with the restore side: say **"backup window"** (the logging-side period when
>    mutation logging is enabled) for the logging phase, and on the restore side say **"the backup
>    window's redo"** or **"restore replay"**, never just "window". Prefer the vocabulary the source
>    documents already use and negate or extend it precisely.
> 3. **Keep the non-test (impl) code as simple as the passing IT allows, and change it only to make
>    a *failing* integration test pass — never from code reasoning alone.** If a code path *looks*
>    wrong, first write the IT that fails without the fix; if no IT fails, leave the impl unchanged.
>    Speculative impl changes — modifying logic with no IT failure demonstrating the need — are
>    **prohibited** (this PoC repeatedly changed impl on the strength of reasoning that later proved
>    wrong or unexercised; let the test, not the argument, justify every impl change).

---

## Codes used in this plan (decode table)

Every shorthand used below, spelled out, so none of it is a "magic code":

- **D1** — how restore obtains each record's `prev_tx_id` (the chain link). **D1a** (recommended, shipped): add `prev_tx_id`/`tx_version` to the `Entry` proto and populate them in `WriteSetEncoder`. **D1b** (rejected): reconstruct from the before-image. **D1c** (rejected): order by commit time.
- **D1′** — during the backup window, `includeColumns` must be `true` (log full column values, not keys-only).
- **Decision A** — op-type modeling: keep `ENTRY_TYPE_WRITE` and derive INSERT vs UPDATE from `prev_tx_id == null` (no new enum/oneof).
- **Q1** — the window-log read path: full `coordinator.state` scan vs a dedicated range-scannable log (deferred).
- **C3** — the consistency point: the coordinator snapshot must be atomic, strictly last, and group-atomic (a true consistent prefix).
- **C4: PREPARED-record recovery** — resolve records left PREPARED in the backup to a clean committed/absent state before replay.
- **concern #N** — the severity-ordered concerns in `REDO-LOGGING-IMPL-FINDINGS.md` §5: **#1** logical-delete must also suppress lazy recovery's physical delete; **#2** one fail-closed window-mode boundary; **#3** the consistency point must be a true consistent prefix; **#4** replay correctness (recovered baseline, cursor, completeness); **#5** window-log access path (= Q1); **#6** restored-record metadata for core resume; **#8** group commit; **#9** window-time cost.
- **R-risk-1** — snapshot/coordinator-backup alignment: the point-in-time oracle and the coordinator backup must reflect the same committed-transaction set.
- **§N** — a section of *this* plan (e.g., §5 = the replay primitive, §6.1 = the integration test, §7 = decisions).
- **Milestone N / Mn** — a numbered milestone in §9 (e.g., Milestone 2 = the performance viability gate).
- **SSR** — Semi-Synchronous Replication, the `scalardb-cluster/replication` module used as the reference implementation (its `RecordApplyService`, `insertTxIds`, e2e test). Reference only — CBRL has no backup site.
- **2PC** — two-phase commit (`TwoPhaseConsensusCommit`), which logs no write-set and is therefore out of scope here.

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
core is validated **independently** of how the D1 decision is resolved.

---

## 1. Goal & non-goals

**Primary goal — build the integration test first (§6.1).** A real end-to-end test, modeled on
`replication:e2e`'s `E2ETest`, that drives the whole backup/restore lifecycle on a live
ScalarDB (JDBC/Postgres) and proves the restored tables are correct:

> start a write workload → back up the user DB (a *non-snapshot-consistent* copy, taken while writes run) → back
> up the coordinator DB **last** (= the consistency point) → stop the workload → restore into a separate
> target by replaying the coordinator's `tx_write_set` window onto the non-snapshot-consistent copy → **compare
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
  for a spike, promote to the base-class pattern later. **But the with/without-group-commit axis
  must be config-transparent — the *same* scenarios under different config, not a bespoke
  group-commit manager or a group-commit-only test** (see the group-commit open question for the
  required rework). `E2ETest` does this with an abstract base plus two thin subclasses overriding a
  `withCoordinatorGroupCommit()` hook (no `@EnabledIf`, no GC-specific test); the in-repo
  `ConsensusCommitSpecificIntegrationTestBase` does it via `isGroupCommitEnabled()` (reads
  `ConsensusCommitConfig`) + `ConsensusCommitTestUtils.loadConsensusCommitProperties` +
  `@EnabledIf("isGroupCommitEnabled")` for GC-only assertions.
- **Run:** unit/property `./gradlew :core:test --tests '*.cbrl.*'`; integration
  `./gradlew :core:integrationTestJdbc --tests '*.cbrl.*'`.

---

## 3. PoC data model

Reuse the real proto for record/column encoding; add a thin chain wrapper that supplies the
ordering metadata `tx_write_set` lacks (so the replay core is exercised regardless of the D1 decision).

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

### 4.5 `RestoredRecordReader` (seam for C4: PREPARED-record recovery)
- `RecordState get(RecordKey)` → the key's current `RecordState` in the **database being
  restored** (the loaded primary backup image) — the replay cursor's origin and merge target,
  one state per key. The §6.2 unit tests supply states directly; the §6.1 IT supplies the **real
  recovered copy** state (`readCopyState`), read **after** the copy's PREPARED records are resolved
  against the reloaded backup coordinator + `before_*` (concern #4a — recovered baseline; see §6.1).
- "Restored" is load-bearing: this reads the *user-table* record in the DB being restored — not
  the *coordinator* rows that the redo-op stream is read from (the Q1 read-path decision), and not core's
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
   - `insertOperations` — a `Deque<RedoOp>` of INSERT roots (`prev_tx_id == null`). Order is
     irrelevant — the chain converges to the same final version — so they are **not** sorted by
     `created_at` (highest rule).
   - `nonInsertOperations` — a `Map<String, RedoOp>` of UPDATE/DELETE ops **keyed by their
     `prevTxId`**, so the chain is traversed by looking the current tx id up in the map.
2. **Walk the chain** (`while (true)`):
   - if `currentTxId == null || deleted`: poll the next INSERT root, skipping any whose txId is
     already applied (idempotent re-run — see `insertTxIds`) **or** already reflected in the base
     (a chain-ancestor of the base's current version, found by walking `prev_tx_id` back from the
     cursor — this replaces the old, incorrect `created_at` skip). If none remain, stop.
   - else: `op = nonInsertOperations.remove(currentTxId)`; if none, stop.
   - apply `op`: INSERT/UPDATE ⇒ merge `op.entry.columns` (INSERT also fills unset columns null),
     `deleted = false`; DELETE ⇒ clear columns + fill null, `deleted = true` (logical tombstone;
     physical removal is the post-replay step).
   - `currentTxId = op.txId`.
3. Result = the working columns + `deleted` + the last applied `txId`.

Why it's safe regardless of input order or re-runs:
- **Order-independent within a key:** the chain is followed by `prevTxId → txId`, so
  `nonInsertOperations` can be built in any order, and INSERT roots are applied in any order (the
  chain converges to the same final version — there is no `created_at` ordering). A re-INSERT after
  a DELETE is a **new INSERT root**, reached via the `deleted` flag — not chained to the delete
  (SSR's loop only polls inserts once `deleted`); a stale root already reflected in the base is
  skipped by walking the chain back from the cursor, never by timestamp.
- **Idempotent:** re-running from the produced state is a no-op — UPDATE/DELETE are already past
  the cursor, and INSERT roots are deduped via a per-record applied-insert-txId set
  (`insertTxIds`), the only way to dedup roots since they have no inbound chain link. CBRL replay
  carries the same `insertTxIds` for the §6.2 P2 idempotency property.

Notes: per the highest rule, **neither `tx_version` nor `created_at` is used for ordering or any
replay decision** — both are only *carried* on the final record so post-restore core stays valid
(concern #6 — restored-record metadata for core resume; left as a TODO seam for the exact `tx_id`/`tx_version` core needs).

---

## 6. Tests

### 6.1 Integration test — `CbrlBackupRestoreIntegrationTest`

Modeled on `replication:e2e` `E2ETest`/`E2ETestEnv` for its ergonomics: two type-rich source tables
(`table_a` no clustering key, `table_b` with one), a `withRetry(manager, tx -> …)` helper, and a
worker thread pool. Differences from SSR's e2e: **no backup site, no
`LogApplier`, no `transaction_groups`, no repl-record tables** — CBRL replays the coordinator's
own `tx_write_set`.

**Structure — group commit is a config axis, not a separate test.** Mirror `E2ETest`, which is an
**abstract** base holding every scenario once (no `@EnabledIf`, no group-commit-only test) plus two
~12-line subclasses (`E2EWith…`/`E2EWithoutCoordinatorGroupCommit…Test`) that override a
`withCoordinatorGroupCommit()` hook (`E2ETestEnv` maps it to `COORDINATOR_GROUP_COMMIT_ENABLED`). So
`CbrlBackupRestoreIntegrationTest` is abstract; a `withCoordinatorGroupCommit()` hook feeds the
**single** `manager`'s properties; two thin subclasses select with/without group commit. The same
scenarios run under **both** configs — no second manager, no GC-only test.

**Fixtures.** One ScalarDB primary on JDBC/Postgres with the Coordinator, window logging on and
`includeColumns=true` (D1′). Source tables in namespace `cbrl_src`; restore-target tables
(identical schema) in `cbrl_restore`.

**Flow** (the requested steps):
1. **start workload** — background threads run `upsert`/`delete` transactions over `cbrl_src`;
   each commit logs its write set to the Coordinator's `tx_write_set`.
2. **back up the user DB** — while writes continue, copy every `cbrl_src` row → `cbrl_restore`
   via `storage.scan`+`put` (cf. `copyAllRecordFromPrimaryToBackupSite`). Intentionally
   **non-snapshot-consistent**: a live scan taken mid-flight, so some keys are stale or missing.
3. **back up the coordinator DB** — snapshot the Coordinator table (id, state, `tx_write_set`)
   into an in-test copy, taken **after** step 2 ⇒ this defines **the consistency point** (C3).
4. **stop workload** — quiesce; run lazy recovery so PREPARED records resolve.
5. **restore** — for each `COMMITTED` row in the coordinator backup, explode its `WriteSet`
   `EntryGroup`s into `RedoOp`s and replay them onto `cbrl_restore` via the §5 core
   (`RecordShuffler` → `RecordApplier`), with `RestoredRecordReader` reading `cbrl_restore`.
6. **compare** — assert `cbrl_restore` equals the expected state at the consistency point, per-key `Get` on
   both sides (cf. `assertPrimaryAndBackupDbTables`).

**The comparison oracle.** "Expected state at the consistency point" needs a deterministic reference
independent of the replay under test. Per the Flow the coordinator backup is taken **while the
workload runs** (step 3, before the stop in step 4), so the consistency point is the committed set
that backup captured — the live `cbrl_src` keeps diverging past it and is *not* the reference; the
oracle must describe *that prefix*. Two independent oracles cover it:

- **Per-key prefix (disjoint-owner test).** Each key is written by a single worker with a
  monotonically increasing token, and every column value is a deterministic function of the writing
  token. The test records each key's op history tagged with its `txId`; the expected state is that
  history applied up to the last op whose `txId` is in the backup — a clean committed prefix, since
  one owner commits a key sequentially. Every restored column must equal the value derived from its
  last-in-prefix writer, which also catches older-overwrites-newer and partial-column-merge
  regressions, and (via keys untouched in-window) that the copy is load-bearing. This is the
  independent "state at a prefix" reference a non-pausing backup needs.
- **Conservation invariant (shared-account test).** An unconstrained workload of balance-preserving
  transfers on shared accounts (multi-writer keys, real contention, like E2ETest). The restored
  image must conserve the total balance and keep both tables equal per account — a property true of
  *any* consistent cut, so it needs no per-key reference and tolerates the fuzzy live boundary. The
  contention is also what exercises both directions of lazy recovery. Needs the D1 + D1′ decisions.

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
slides scope logging to a window precisely because of this cost; concern #9 (window-time cost) flags it. Reuses the
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
clustering, so "read every op from window start" is a full scan (concern #5 — window-log access path). In the PoC the
shuffle just consumes a `Stream<RedoOp>`; the production choice (full coordinator scan vs.
dedicated range-scannable log) stays open and is fed synthetic ops until decided.

**C3 — the consistency point.** Coordinator snapshot must be atomic, strictly last, and group-atomic
(all `child_ids` of a row in or out). Out of PoC scope (no real snapshotting) but the replay
must treat an `EntryGroup` set as all-or-nothing per coordinator row (concern #8 — group commit); the PoC
models a coordinator row as `{ txId, state, List<EntryGroup> }` and explodes children into
`RedoOp`s only when `state == COMMITTED`.

**C4: PREPARED-record recovery, before replay.** Records PREPARED in the backup image resolved via
the consistency point + `before_*` to a clean committed state **before** chain replay anchors. Modeled as the
§4.5 `RestoredRecordReader` seam; confirm `before_*` is present in the backup image. (SSR's
`BackupDbTableRepository.rollforward`/
`rollback` via `CommitMutationComposer`/`RollbackMutationComposer` is the concrete reference
pattern.)

**TODO (deferred) — backup-mode flag (the "is CBRL window open?" switch).** Slides 4/6–8: a
durable, runtime-readable flag every embedded-Core process observes before DB backups start
(Cluster can push it like pause). Open points, not decided here:
- **One flag gating both** mutation-logging and logical-delete mode (they must flip together —
  concern #2, one fail-closed window-mode boundary), stored with a **TTL/expiry** so a crashed backup process can't pin the window
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
  RestoredRecordReader.java    // C4: PREPARED-record recovery seam: RecordState get(RecordKey)
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
   restore-target tables, workload, non-snapshot-consistent user-DB copy, coordinator-DB copy, per-key compare).
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
4. **C3 / C4: PREPARED-record recovery + restart** — coordinator-last consistency point handling, PREPARED resolution in
   `RestoredRecordReader`, per-bucket checkpoint/idempotent re-run; write the §7 decision findings (D1, Q1, C3, C4: PREPARED-record recovery)
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

## Deferred / Open Questions

### From 2026-06-18 review

Findings from a multi-persona document review (coherence, feasibility, scope-guardian,
adversarial, product-lens), the source-grounded pass reading the original slide deck
(`_Coordinator-Based Redo Logging in ScalarDB.pdf`), `CBRL-RESTORE-DESIGN.md`,
`REDO-LOGGING-IMPL-FINDINGS.md`, and `CBRL-HANDOFF.md`. Captured for the owner's judgment.
**✅ marks a finding resolved since this review** (each carries a dated note); unmarked findings
remain open.

**Divergence from the source documents:**

- ✅ **RESOLVED — Restore ordering is chain-only (`created_at`/`tx_version` removed)** — §5 (coherence, adversarial)

  *Resolved 2026-06-18 (commit `50055f41b`):* the replay core briefly ordered disconnected re-insert roots by `created_at` — droppable on a millisecond/group-commit tie or under multi-primary clock skew, the commit-time ordering D1c rejected. Removed: restore now skips stale roots by walking the `prev_tx_id` chain back from the base, and the **highest rule** (top of this plan) forbids `created_at`/`tx_version` in restore. Offline single-owner replay under physical-delete is chain-only and clock-independent (verified by §6.2 + §6.1). This also resolves two sub-findings with the same root cause: the **group-commit `created_at` tie** (`nextInsert` no longer compares `created_at`) and the **oracle sharing the `created_at` tie-break** (restore no longer uses `created_at`, so the oracle's `created_at` order is now an independent cross-check, not a shared assumption).

- ✅ **RESOLVED (non-issue) — logical-delete is NOT load-bearing for the restored state** — `REDO-LOGGING-IMPL-FINDINGS` §4 + #1 (coherence, adversarial)

  *Resolved 2026-06-18:* carried from the source as "physical delete breaks the chain for a re-insert," but there is no failing case for the restored value. Even when a re-insert is logged as a disconnected root (`prev_tx_id = null`), offline replay converges to the **last** re-insert on the key's timeline, so the restored value is correct regardless of which version the copy captured — `delete K → get K → put K = X` yields `K = X` in plain ScalarDB and under CBRL restore alike. Logical-delete would only preserve a gap-detection *signal* (a missing op masked by a disconnected root), which is moot here since the connectivity check is not used (SSR-tolerant replay). No action.

- ✅ **WON'T FIX (out of scope by design) — backup-mode entry/exit is not this PoC's concern** — §1/§10 (product-lens)

  *Decided 2026-06-18:* the source flags two "CRITICAL" mechanisms — logical-delete (#1) and the fail-closed backup-mode flag (#2). #1 is a non-issue for the restored state (see the resolved entry above). #2 — how every process enters/exits backup mode (the cache-wait / fail-closed window flag) — is deliberately **out of scope here**: this PoC exists to establish the **correctness of the CBRL restore protocol** (the replay core), not the operational machinery for turning logging on/off across processes. Validating the protocol on a controlled window is exactly the intended scope; entering/exiting backup mode is a separate workstream. No action.

- ✅ **DONE — viability gate cleared; proto change justified** — §7 D1a / §9 Milestone 2 (product-lens)

  *Resolved 2026-06-18:* the §6.3 / Milestone-2 performance gate has run and **passed** — full-column write-set logging costs ~10% in-window overhead, deemed acceptable. The `tx_write_set` approach is validated, so the D1a additions (`prev_tx_id`, `tx_version` on `Entry`) are justified rather than premature. (Independent of the gate, the fields are optional/additive, so the original "changed ahead of the gate" worry carried little weight anyway.) No action.

- ✅ **WON'T FIX (out of scope) — backup-sourcing mechanics, not the CBRL protocol** — §6.1 vs slides slide 10 / `REDO-LOGGING-IMPL-FINDINGS` #3 (coherence, adversarial)

  *Decided 2026-06-18:* "snapshot-consistent vs non-snapshot-consistent copy" is about *how production sources the base backup* (native snapshot / `pg_dump --single-transaction`), which is operational/deployment, not the CBRL restore-protocol correctness this PoC validates. The only protocol-relevant part is the base **precondition** — each record a committed version no newer than the consistency point — and the IT already satisfies it: copy taken live, coordinator backed up **last** (bounds every record ≤ the consistency point), PREPARED rows cleaned by C4: PREPARED-record recovery. The non-snapshot-consistent copy is just a deliberately harder test input proving the protocol tolerates it. No action.

**Plan vs. implemented branch (the plan has been overtaken by code):**

- ✅ **OUT OF SCOPE (process) — plan re-baselining is doc-maintenance** — Header / §9 (feasibility)

  Updating the plan to match the as-built branch is documentation upkeep, not a CBRL design/impl defect (the code is correct; the plan text lags). Set aside for the design/impl review.

- ✅ **OUT OF SCOPE (doc-staleness) — D1a wording lags the code** — §7 D1a / §0 (feasibility, adversarial)

  The shipped design — derive the writing-txn id from the coordinator row id + `EntryGroup.child_id` rather than a per-`Entry` `tx_id` field — is sound; only §7's wording is stale. The real design question it gestures at (group-commit child-id chain-linking) is the item below.

- **Group commit: chain-linking + config-transparent IT done; in-doubt-child recovery is ScalarDB's own concern (no CBRL bug), exercised under the GC subclass** — restore core / §6.1 (design/impl + IT test)

  **Landed (2026-06-18) — keep.** The redo→`RedoOp` explosion derives the writing transaction's **full id** — `keyManipulator.fullKey(parentRowId, childId)` for a normal group-commit child (row keyed by the parent, `child_id` set), else the row key itself (non-group-commit, or a delayed group commit keyed by the full id) — so `RedoOp.txId` matches the full id records store and other ops' `prev_tx_id` chains to. `closeOverChain` resolves a child's full `prev_tx_id` to its parent-keyed row; the `#1` reload preserves a parent row's `child_ids`. This chain-linking fix is correct.

  ✅ **RESOLVED (2026-06-19, commit `9eef2a872`) — config-transparent IT structure.** With/without group commit is now the **same** scenarios under different config, not a bespoke manager + a dedicated test. Reference patterns (read from latest main): `replication:e2e` is an abstract `E2ETest` holding the test methods (no `@EnabledIf`, no GC-specific test) plus two ~12-line subclasses (`E2EWith…`/`E2EWithoutCoordinatorGroupCommit…Test`) that override only `withCoordinatorGroupCommit()`/`getCompressionType()`, which `E2ETestEnv` maps to `COORDINATOR_GROUP_COMMIT_ENABLED`; the in-repo `ConsensusCommitSpecificIntegrationTestBase` keys `isGroupCommitEnabled()` off `ConsensusCommitConfig`, loads `scalardb.consensus_commit.coordinator.group_commit.*` via `ConsensusCommitTestUtils.loadConsensusCommitProperties`, and gates GC-only assertions with `@EnabledIf("isGroupCommitEnabled")`. **Done:** `CbrlBackupRestoreIntegrationTest` is now abstract with a `withCoordinatorGroupCommit()` hook feeding the single `manager`; two thin subclasses `CbrlBackupRestoreWith[out]GroupCommitIntegrationTest` run the same scenarios with GC off and on; the bespoke `groupCommitManager`/`groupCommitProperties`/`commitGroupBatch`/`hasGroupCommitChild` and the dedicated `groupCommit_*` test were deleted. Full-child-id chain-linking and delete→re-insert across group rows are exercised transparently under the GC-on subclass — both subclasses pass.

  **CORRECTED (2026-06-19) — not a CBRL bug; this is ScalarDB's recovery layer, not ours.** On restore the coordinator is reloaded from the backup and ScalarDB's lazy recovery resolves the copy's in-doubt records. Whether recovery correctly resolves an in-doubt *group-commit child* is ScalarDB's own correctness, covered by ScalarDB's tests — not something CBRL needs to reason about or re-verify. My earlier claim that recovery would never resolve such a child and would throw reached down into ScalarDB internals to assert a bug there, with no test — the wrong layer, and unverified. The CBRL IT already drives recovery for in-doubt records, including group-commit children under the GC subclass, and passes. No CBRL impl change.

- ✅ **OUT OF SCOPE (process) — milestone sequencing** — §1 / §9 (scope-guardian)

  IT-first vs replay-core-first, and where the perf gate sits, is PoC planning/sequencing, not a CBRL design/impl issue. Set aside.

- ✅ **RESOLVED — Backup-window-start ≤ copy alignment, deterministically exercised (R-risk-1)** — §6.1 (design + IT test, P1)

  **Precondition (now stated):** restore is correct only if the backup window opens **no later than** the copy point. Replay anchors each key on its copied version and walks the redo forward to the consistency point; every op on that path committed at or after the copy, so it is logged **iff** the window was already open when the copy was taken. If the window opened after the copy, a key whose copied version differs from its consistency-point value would have a gap the redo can't cover.

  *Resolved 2026-06-18:* the IT now exercises this deterministically instead of relying on the random workload. A dedicated **alignment** key partition is seeded pre-window, updated in-window **before** the copy (so the copy captures an in-window version, strictly earlier than the consistency point), and updated again **after** the copy. `assertAlignmentRepairedForward` checks the copied version is exactly that in-window pre-copy version (not the pre-window seed), that its redo carries both updates with no insert root, and that restore repairs it forward to the post-copy value. The assertion has teeth: had the window opened after the copy (copied version unlogged) or the redo not covered the gap, the copied-version or repaired-forward check would fail.

- ✅ **OUT OF SCOPE (scope) — 2PC** — §0 / §10 (coherence)

  2PC's "scope choice vs known gap" framing is a scope/wording question; the underlying limitation (2PC commits log no write-set → invisible to replay) is already an acknowledged out-of-scope gap in §10. Not an open design/impl item for this PoC.

- ✅ **RESOLVED — C4: PREPARED-record recovery is now self-contained (recovers against the backup, not the live coordinator)** — §4.5 / §6.1 (design/impl + IT test, P1)

  *Resolved 2026-06-18:* the IT previously recovered the copy by reading the **live** coordinator, so it proved only replay-onto-a-recovered-base. The restore now reloads the coordinator **table** from the backup before recovery (`reloadCoordinatorFromBackup`): every backed-up transaction as COMMITTED, and every transaction still PREPARED/DELETED in the copy but absent from the backup as ABORTED — the restore's decision that an in-flight-at-copy transaction which never reached the consistency point is discarded (a fast, faithful stand-in for the hardcoded 15-second `TRANSACTION_LIFETIME_MILLIS` expiry recovery would otherwise wait out). ScalarDB's own recovery then resolves the copy against **that** coordinator, and `awaitCopyRecovered` drives it to quiescence so the raw replay base reads resolved values. The live coordinator — diverged past the consistency point by the post-backup updates — is truncated before recovery, so the restore provably uses only the backup; the existing point-in-time + correctness assertions now carry that stronger meaning. Verified green across two runs (different random schedules).

- ✅ **OUT OF SCOPE (process; gate has run) — viability threshold** — §6.3 / §9 Milestone 2 (scope-guardian)

  Whether the plan states a numeric pass/fail threshold is process; and the gate has now run and passed (~10% in-window overhead, accepted). Not a design/impl issue.

### Process note — assistant reliability (2026-06-18)

The central failure in this session was epistemic. The assistant (Claude Code) repeatedly claimed, with confidence, that it understood ScalarDB's lazy-recovery behavior, and built a design concern on top of that claimed understanding. Its understanding was in fact wrong — for example, it described an asynchronous recovery path as "eager," attributed a record's physical deletion to lazy recovery rather than to the committing transaction's own rollforward, and treated a delete-then-re-insert "chain break" as a real correctness problem when it is not. It then insisted on these incorrect ideas across many turns — elaborating the concern, proposing code-level mitigations, and offering to "strengthen" it in the plan — instead of verifying. It abandoned the wrong position only after the user explicitly told it to read the source code and then walked it through a minimal example showing there was no problem at all. The cost came not from one isolated error but from the pattern: asserting understanding it did not have, and persisting in it rather than checking.
