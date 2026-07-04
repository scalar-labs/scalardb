# CBRL — Design: backup and restore

Status: **PoC.** The restore/replay engine and the *enter/exit* half of the embedded-Core backup-mode
machinery are implemented and exercised end-to-end by a containerized demo (single-Postgres +
single-MySQL). The lifecycle's cancel and restore-side states, and several production preconditions
below, are **designed but not yet built or enforced** — see *Assumptions & preconditions* and *Known
gaps* before relying on this beyond the demo. This is an overall design; it describes the concept, not
every implementation detail.

A plain physical backup of ScalarDB's databases, taken online while transactions keep running, is
**not** transactionally consistent: a record may be caught mid-commit, stale, or missing, and
different records reflect different points in time. Coordinator-Based Redo Logging (CBRL) makes such a
backup consistent **after the fact** — during a backup window it logs each commit's write operations
into the coordinator, and at restore it replays that redo onto the copy to reconstruct the exact
committed state as of a **consistency point**. It is the same record-level redo idea as ScalarDB
Cluster's Semi-Synchronous Replication (SSR), applied in place to a backup rather than shipped to a
remote site. Correctness rests on the preconditions in *Assumptions & preconditions* — when any of
them is violated the restore is silently wrong, not erroring.

```mermaid
sequenceDiagram
    participant App as App (Consensus Commit)
    participant Co as coordinator (tx_write_set)
    participant DB as user tables
    participant R as Restore
    Note over App,DB: backup window open
    App->>Co: each commit logs its write ops (keys, columns, prev_tx_id + version)
    Note over DB: online physical backup copies the user tables (non-snapshot)
    Note over Co: coordinator snapshotted last and atomically -> the consistency point
    R->>Co: read the redo for the backup label
    R->>DB: replay each record's chain forward onto the copy
    Note over DB: now the committed state as of the consistency point
```

## Backup phase

- **Open a backup window.** While it is open, every Consensus Commit transaction logs its **write
  operations** into the coordinator's own `tx_write_set` column. Capture rides on the normal commit —
  it only stamps the write set into the coordinator row the commit already writes, so there is no
  extra transaction and no extra round trip.
- **Each write operation** records, for one record: its keys, the new column values (a DELETE carries
  no columns), and a **chain link** — the previous committed version's transaction id (`prev_tx_id`)
  and this write's version (`tx_version`). The record's own transaction id is the enclosing
  coordinator row's id (parent + child for group commit), not stored per operation. That link is what
  lets restore order writes per record without trusting wall-clock time.
- **Copy the user tables** with an ordinary **online physical backup** (`pg_dump`/`mysqldump` or a
  storage snapshot) taken while the database keeps serving — a **non-snapshot-consistent copy**.
  Replay tolerates this because it walks each record forward.
- **Snapshot the coordinator table last, and atomically.** Its set of committed transactions *defines*
  the consistency point, so — unlike the user tables — the coordinator copy is not just data to be
  repaired; it must be a single-point-in-time, read-from-closed snapshot, and every user-table
  snapshot must be no newer than it (see *Assumptions & preconditions*). Because the redo lives in
  `tx_write_set` and each commit time in `tx_created_at`, an atomic backup of the coordinator table
  captures both; there is no separate redo log or timestamp export to manage.

### Backup mode — entering and quitting (embedded Core)

The backup window must be turned on and off across **every app process** (each embedding ScalarDB
Core). Embedded Core has no control plane to push a flag to every process, so the flag lives in a
coordinator-DB table that each process reads on a periodic, cached poll. (In ScalarDB Cluster the flag
can be pushed directly, like pause, so none of this machinery is needed — the concern below is
specifically embedded Core.)

**The flag table `<coordinator-ns>.backup`.** One row per backup. The **partition key is a fixed
constant** so every row lives in one partition, and the **`label` is the clustering key** — so the
daemon lists all backups with an ordinary **single-partition scan** (not a cross-partition one),
returned sorted by label. Created alongside `coordinator.state`; timestamps are `BIGINT` epoch-millis.

| column | type | notes |
|---|---|---|
| `id` | TEXT (PK) | fixed constant (e.g. `"backup"`) — co-locates every row in one partition |
| `label` | TEXT (CK) | the backup label, usually its timestamp; also the redo's `EntryGroup.backup_label` and the `CbrlRestore` argument |
| `state` | TEXT | lifecycle (below); a process logs redo **iff** `state = BACKING_UP` |
| `created_at`, `updated_at` | BIGINT | backup started / last transition |
| `updated_by` | TEXT | operator id (observability) |

**Lifecycle.** The intended state machine is below. **As built, only `enterBackupMode`
(`→ BACKING_UP`) and `exitBackupMode` (`BACKING_UP → BACKED_UP`) exist**; the cancel and restore-side
states are declared but not yet written by any code, and `CbrlRestore` does not touch this table (it
takes a bare label). Each *implemented* transition is a `putIf` on the current `state`; re-opening an
already-`BACKING_UP` label is idempotent success, and opening a label that has already transitioned is
rejected.

```mermaid
stateDiagram-v2
    [*] --> BACKING_UP: enableRedoLogging(label)
    BACKING_UP --> BACKED_UP: disableRedoLogging()
    BACKING_UP --> BACKUP_CANCELED: cancel (planned)
    BACKED_UP --> RESTORING: start restore (planned)
    RESTORING --> RESTORED: (planned)
    RESTORING --> RESTORE_CANCELED: cancel (planned)
```

**Protocol.** Each app process runs a daemon that polls the flag table every `check-interval` and
caches the set of `BACKING_UP` labels; `begin()` reads the cache (not the DB) to decide logging.

```mermaid
sequenceDiagram
    participant Op as Operator
    participant T as backup table
    participant P as App process
    Op->>T: putIfNotExists row for label, state BACKING_UP
    loop every check-interval, 5s
        P->>T: scan the partition
        T-->>P: BACKING_UP rows, cache the labels and log redo
    end
    Note over Op,P: cache wait spans interval + staleness-bound + tx-timeout + skew
    Note over Op,P: every process sees the flag and pre-flag txns drain
    Note over Op: consistency point valid, snapshot the coordinator
    Op->>T: putIf state BACKING_UP to BACKED_UP
    P->>T: next scan, label not BACKING_UP, stop logging
```

**Keeping the backup complete.** Because the poll is periodic, three mechanisms *aim* to ensure no
window-time commit goes unlogged. They reduce the risk to a high-probability mitigation, **not an
absolute guarantee** (see *Known gaps*):

- **Cache wait** — after opening the window, wait until every process has seen the flag and pre-flag
  transactions have drained before trusting the consistency point. This is a manual, unverified sleep;
  there is no readiness probe.
- **`begin()` freshness** — `begin()` uses the cache but forces a synchronous scan if the cache is
  older than the staleness bound; once the flag has been read at least once, if it still can't be
  confirmed `begin()` **fails closed** (refuses), so a process resuming from a stall — or during a
  coordinator partition — can't begin under a stale flag.
- **Transaction timeout** — a transaction self-aborts (retryable) if it runs longer than
  `transaction-timeout`, killing one frozen across the transition. **This is global and always-on**
  (default `3 × check-interval`), applied to *every* Consensus Commit transaction whether or not a
  backup is in progress — not a backup-scoped knob. See *Known gaps* for its blast radius and why the
  check (once, before prepare) does not bound the whole commit protocol.

```mermaid
sequenceDiagram
    participant P as App process
    participant Tx as Transaction
    P->>Tx: begin, stamp beginAt
    Note over Tx: does work, or is GC-paused across the flag transition
    P->>Tx: commit
    Note over Tx: check elapsed since beginAt (before prepare)
    alt elapsed exceeds transaction-timeout
        Tx-->>P: abort, retryable
        P->>Tx: begin a fresh txn under the current flag
    else within the timeout
        Tx-->>P: commit succeeds
    end
```

## Restore phase

Restore replays the logged write operations onto the copy, walking each record forward to the
consistency point. The result is a **consistent prefix** — the committed state as of the consistency
point — **provided the capture was complete and the preconditions held** (replay cannot detect an
incomplete capture; see *Known gaps*). Restore uses the **Storage API only**: no transaction of its
own, and no extra coordinator bookkeeping.

The operator physically restores the coordinator and user tables first, then runs restore for a chosen
**backup label** — multiple labels can accumulate in the long-lived redo over time, and only the
matching one is replayed. For each record, restore recovers the copy's in-flight version, reads it as
the base, replays the chain forward, and writes the result back with the Storage API. Records are
routed by the namespace and table carried in their redo, so one run spans every namespace the window
touched. Each reconstructed record is stamped with its **original** commit time (the coordinator's
`tx_created_at`, not the restore-time clock), so `tx_committed_at` stays faithful and valid for later
Consensus Commit time-based logic; restore trusts the coordinator backup's time over the record's own
column, which record recovery may have re-stamped.

**Resolving order — the tx-id chain.** Order is resolved **per record** by the chain, exactly as in
SSR. Suppose three transactions commit in order T0 < T1 < T2:

- T0 inserts K-A, K-B, K-C
- T1 updates K-A, K-B
- T2 deletes K-A, K-C

The write operations, each carrying `previous txn ID -> current txn ID`, grouped per record:

- **K-A:** INSERT (null -> T0), UPDATE (T0 -> T1), DELETE (T1 -> T2)
- **K-B:** INSERT (null -> T0), UPDATE (T0 -> T1)
- **K-C:** INSERT (null -> T0), DELETE (T0 -> T2)

Each operation's `previous txn ID` must match the `current txn ID` of the operation before it, so
K-A's only valid order is INSERT(->T0), UPDATE(T0->T1), DELETE(T1->T2) — whatever order the operations
happen to be read in. The *ordering* of the ops a record has is thus fixed by the causal chain, never
by a timestamp, so it is immune to clock skew. (This is about ordering, not completeness — see the
caveat under *Known gaps*.)

Replay starts from whatever version the copy caught for a record and follows the chain **forward** to
the consistency point:

- If the copy caught **K-A at T1**, replay applies only DELETE (T1 -> T2); the INSERT and UPDATE are
  already reflected in the copied version and are skipped.
- If the copy has **no record** (absent, or an in-flight one that recovers to absent), replay starts
  from the INSERT root (null -> T0) and follows forward.

**Delete then re-insert — multiple chain segments.** A record can be deleted and inserted again, so it
has **more than one INSERT root**: every INSERT starts a fresh chain with `prev_tx_id = null`.
Extending K-A above (which ended with DELETE at T2), suppose it is re-inserted and updated:

- segment 1: INSERT (null -> T0), UPDATE (T0 -> T1), DELETE (T1 -> T2)
- segment 2: INSERT (null -> T3), UPDATE (T3 -> T4)

The two segments are **not** linked — the DELETE does not point to the later INSERT. Replay walks a
segment forward until it reaches a DELETE (record now deleted), then resumes from the next unused
INSERT root, finishing at the surviving segment's terminal version (alive at T4 here). For a
well-formed, chain-closed backup this is order-independent: exactly one root survives, so the order the
roots are tried in never changes the outcome (ordering roots by commit time is only a try-order
optimization, and the reproducible tiebreak used *only if* a malformed backup left two live roots).
Because DELETE is a **logical tombstone**, a deleted-then-reinserted record still carries its chain
position: replay marks the caught version *and its chain-ancestors* (walking `prev_tx_id` back) as
**reflected** and never re-applies a reflected INSERT root. For example, if the copy caught K-A at T4
and a later `DELETE (T4 -> T5)` was logged: replay marks T4 and its ancestor T3 reflected, applies the
delete (K-A now absent), then re-tries segment 1 — which again ends in `DELETE (T1 -> T2)` — and skips
the reflected re-insert root T3. So K-A ends **absent**, never revived to the re-inserted version it
already reflects. (Below-base segments are always safe to re-try: a re-insert is only possible after a
delete, so such a segment must end in a DELETE.)

**Crash-safety.** Write-back is per record, not atomic, so a crash mid-restore leaves a partial set.
Safety comes from **convergence, not atomicity**: re-running from the same backup re-derives the
identical state per record and overwrites any partial ones (recovery is a no-op on already-committed
records, and replay is idempotent — a re-insert is deduped, never applied twice). A Byteman crash test
injects failures at each restore stage and asserts a re-run converges.

**Windowed repair.** Under window-scoped logging a record's chain root can predate the window and never
be captured, so the first in-window operation links below the copied version. Such below-the-base
operations are skipped — the copy already reflects them — so a record whose history predates the window
still restores correctly.

## Configuration

- `scalar.db.consensus_commit.backup.check_interval_millis` — daemon poll interval (default 5000 ms;
  recommended 5–10s).
- `scalar.db.consensus_commit.backup.staleness_bound_millis` — oldest cache `begin()` accepts before a
  forced fresh scan (default ~`3 × check_interval`).
- `scalar.db.consensus_commit.transaction_timeout_millis` — **global** transaction-lifetime bound
  (default `3 × check_interval`); see *Known gaps*.
- `scalar.db.cross_partition_scan.enabled=true` — required by restore, which scans the coordinator
  table for redo.
- Restore concurrency: `replay_buckets` / `replay_workers` (each key's whole recover→replay→write-back
  pipeline runs on the one worker that owns its bucket).

## Implementation notes

- **Restore engine (built):** `CbrlRestore` → `RecordShuffler` (buckets by key) → `RecordApplier`
  (per-key recover via `recoverRecord`, read base, chain-replay, write back). Capture uses
  `WriteSetEncoder`; the redo proto (`WriteSet`/`EntryGroup`/`Entry`) carries `backup_label`,
  `prev_tx_id`, `tx_version`, and columns in the coordinator's `tx_write_set`.
- **Backup mode (partly built):** the `coordinator.backup` `TableMetadata` (wired into
  `ConsensusCommitAdmin`'s coordinator-table create/drop/truncate/exists/repair), a `BackupModeDaemon`
  (periodic single-partition scan + one synchronous scan at startup; logs an error if a scan returns
  more than one `BACKING_UP` row), `enableRedoLogging`/`disableRedoLogging` writing the row, and
  `begin()` reading the daemon cache + stamping the begin time so the commit path can enforce the
  transaction timeout. Only the enter/exit transitions are wired (see *Lifecycle*).

## Assumptions & preconditions

Correctness depends on all of these; violating any is a **silent** wrong restore.

1. **Atomic coordinator snapshot.** The coordinator table must be copied as a single-point-in-time,
   read-from-closed snapshot (e.g. `pg_dump --single-transaction` on a single DB). A torn/multi-partition
   read of the coordinator can include a transaction but miss one it read from → non-serializable restore.
2. **Coordinator snapshotted strictly last.** Every user-table snapshot must be no newer than the
   coordinator snapshot, and each user-table copy should itself be single-instant (`mysqldump
   --single-transaction`, InnoDB, or a quiesced storage snapshot). Redo only rolls records *forward* to
   the cut; a user record newer than the cut cannot be rolled back.
3. **Coordinator-state cleanup suspended for the window.** `finishTransaction` (and any Cluster
   coordinator-state cleanup) deletes a committed transaction's coordinator row *and its redo*. If that
   runs during the window, the committed write's redo is gone before the coordinator copy → the record
   is rolled back at restore and lost. Cleanup must be paused from window open until the coordinator
   snapshot completes.
4. **Coordinator restored before user records are recovered.** `CbrlRestore` recovers in-flight copy
   records against the restored coordinator; if the coordinator isn't fully restored first, an ancient
   PREPARED record is judged expired and rolled back (committed write lost), with no error.
5. **Single active window.** The write path stamps one `EntryGroup.backup_label` per commit; only one
   `BACKING_UP` label may be open at a time.

## Known gaps

Ranked roughly by severity — residual limitations to harden or accept for the PoC.

- **Completeness is undetectable.** Replay cannot distinguish a dropped mid-chain op from a
  legitimately-below-base op, so any capture hole yields a wrong-but-plausible record, never an error.
  The preconditions above exist to prevent capture holes; there is no post-restore integrity check.
- **Timeout is checked once, before prepare.** The transaction-timeout check runs once at the top of
  `commit()` (using a monotonic `System.nanoTime()` clock), not across prepare→commitState — so a
  pause *inside* the commit protocol can still land an unlogged commit arbitrarily late. The
  cache-wait is therefore a strong mitigation, not a hard upper bound; a re-check just before the
  coordinator commit write would tighten it.
- **Global always-on transaction timeout.** The 15 s (default) cap applies to *every* transaction, not
  just during backups, so enabling CBRL breaks any legitimately long transaction; "retryable" is
  misleading because a transaction that inherently needs >15 s re-times-out forever. Consider defaulting
  it off and enabling it only while a window is open, or decoupling it from `check_interval`.
- **Multi-window mis-attribution.** `enableRedoLogging` doesn't check for a *different* open label, so a
  second window can open; the write path then logs all redo under the first sorted label, silently
  corrupting the other backup. The `>1 BACKING_UP` log is after-the-fact detection, not prevention.
- **Restore is single-JVM, in-memory.** It materializes the full window redo plus the whole
  coordinator txId→commit-time map (the latter for *all* committed rows, before the label filter) in one
  heap — OOM risk on large or long-lived windows. Needs paging/spill or sharding.
- **Write-set size vs backend item limits.** The redo is an inline BLOB on the coordinator row
  (aggregated across children under group commit); on DynamoDB (400 KB) / Cosmos (2 MB) a large or
  batched transaction that commits normally can fail *only while a window is open*.
- **Flag rows resurrect on restore.** The `backup` table is in the backed-up coordinator namespace, so a
  restored cluster re-enters backup mode on daemon startup (phantom window, redo overhead) and carries
  every `tx_write_set` blob forward as bloat. Reset tooling isn't built; the alternative is a separate,
  excluded system namespace.
