package com.scalar.db.transaction.consensuscommit.cbrl;

import com.google.common.base.Preconditions;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.annotation.Nullable;

/**
 * Pass 2: replays each bucket's redo ops onto the records being restored. A worker owns whole
 * buckets, so there is no intra-key concurrency — no CAS, no locks (the design's core
 * simplification over SSR). Within a bucket, ops are grouped by {@link RecordKey} and each key is
 * replayed by the cursor-driven primitive ({@link #replayKey}), which mirrors SSR's {@code
 * RecordApplyService.findWriteOperationsToApply}.
 */
public final class RecordApplier {
  private final RestoredRecordReader reader;
  // Per-bucket completion, so a crashed re-run skips done buckets (idempotency requirement). The
  // primitive is itself idempotent, so re-running a bucket is also safe; this just avoids the work.
  private final Set<Integer> completedBuckets = ConcurrentHashMap.newKeySet();

  public RecordApplier(RestoredRecordReader reader) {
    this.reader = reader;
  }

  /**
   * Replays all buckets, returning the resulting state per key. {@code workerCount} workers each
   * own whole buckets ({@code M <= N}).
   */
  public Map<RecordKey, RecordState> apply(List<List<RedoOp>> buckets, int workerCount)
      throws InterruptedException {
    Preconditions.checkArgument(workerCount >= 1, "workerCount must be >= 1");
    Map<RecordKey, RecordState> result = new ConcurrentHashMap<>();
    ExecutorService executor = Executors.newFixedThreadPool(Math.min(workerCount, buckets.size()));
    try {
      List<Future<?>> futures = new ArrayList<>(buckets.size());
      for (int b = 0; b < buckets.size(); b++) {
        int bucketIndex = b;
        List<RedoOp> bucket = buckets.get(b);
        futures.add(executor.submit(() -> applyBucket(bucketIndex, bucket, result)));
      }
      for (Future<?> future : futures) {
        future.get();
      }
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof CbrlReplayException) {
        throw (CbrlReplayException) cause;
      }
      throw new CbrlReplayException("Replay failed: " + cause);
    } finally {
      executor.shutdownNow();
    }
    return result;
  }

  private void applyBucket(int bucketIndex, List<RedoOp> bucket, Map<RecordKey, RecordState> out) {
    if (!completedBuckets.add(bucketIndex)) {
      return; // Already applied (idempotent re-run).
    }
    Map<RecordKey, List<RedoOp>> byKey = new LinkedHashMap<>();
    for (RedoOp op : bucket) {
      byKey.computeIfAbsent(op.key(), k -> new ArrayList<>()).add(op);
    }
    for (Map.Entry<RecordKey, List<RedoOp>> entry : byKey.entrySet()) {
      out.put(entry.getKey(), replayKey(entry.getKey(), entry.getValue()));
    }
  }

  /**
   * The replay primitive (PoC plan §5). Given the key's current state in the database being
   * restored and that key's ops, follows the {@code prevTxId -> txId} chain forward from the
   * record's current version to the final state. Order- and input-independent within a key, and
   * idempotent.
   *
   * <p>An op that does not connect to the chain reachable forward from the current version is left
   * unapplied, mirroring SSR's {@code findWriteOperationsToApply} (which keeps such ops as {@code
   * remainingWriteOperations} rather than rejecting them). This is what makes <b>windowed
   * repair</b> work: under window-scoped logging a record's chain root predates the logging window
   * and is never captured, so the first in-window op links to a {@code prev_tx_id} that is neither
   * another op nor — once the torn-copy base has advanced past it — the current version. That op
   * sits below the base, whose state already reflects it, and is correctly skipped. The torn copy
   * may also have captured a deleted or arbitrarily-advanced version, which carries no position
   * information, so — like SSR — replay does not try to tell a legitimately-below op apart from a
   * genuinely dropped mid-chain op; completeness is the backup capture's responsibility (full
   * coordinator scan + chain closure), not this primitive's. The one structural anomaly still
   * rejected is a fork (two ops sharing a {@code prev_tx_id}), which serializable commit cannot
   * produce.
   */
  RecordState replayKey(RecordKey key, List<RedoOp> ops) {
    RecordState current = reader.get(key);

    // divideWriteOperations: INSERT roots vs the prevTxId-keyed chain of UPDATE/DELETE ops.
    List<RedoOp> insertList = new ArrayList<>();
    Map<String, RedoOp> nonInsertOps = new HashMap<>();
    for (RedoOp op : ops) {
      if (op.isInsert()) {
        insertList.add(op);
      } else {
        RedoOp clash = nonInsertOps.put(op.prevTxId(), op);
        if (clash != null) {
          throw new CbrlReplayException(
              "Two ops on "
                  + key
                  + " share prev_tx_id "
                  + op.prevTxId()
                  + " (txns "
                  + clash.txId()
                  + ", "
                  + op.txId()
                  + ") — not a linear chain");
        }
      }
    }
    insertList.sort(Comparator.comparingLong(RedoOp::createdAtMillis));
    Deque<RedoOp> insertQueue = new ArrayDeque<>(insertList);

    RecordState.Builder state = current.toBuilder();
    // Created-at of the last applied op (chain order is created-at-monotonic). After a DELETE we
    // resume only from an INSERT root that genuinely follows it — never one already reflected in a
    // mid-chain base (which is the case during windowed repair onto a torn copy).
    long lastCreatedAtMillis = Long.MIN_VALUE;
    while (true) {
      RedoOp op;
      if (state.currentTxId() == null || state.deleted()) {
        // Record absent or deleted: resume from the oldest not-yet-applied INSERT root that comes
        // after the current position.
        op = nextInsert(insertQueue, lastCreatedAtMillis, state);
        if (op == null) {
          break;
        }
      } else {
        // Follow the chain from the record's current version.
        op = nonInsertOps.remove(state.currentTxId());
        if (op == null) {
          break;
        }
      }

      if (op.isInsert()) {
        state.applyInsert(op.entry().getColumnsList());
        state.markInsertApplied(op.txId());
      } else if (op.isUpdate()) {
        state.applyUpdate(op.entry().getColumnsList());
      } else {
        state.applyDelete();
      }
      state.advanceCursor(op.txId());
      lastCreatedAtMillis = op.createdAtMillis();
    }
    // Ops left in nonInsertOps were never reached (window-boundary or below-base links). They are
    // tolerated, as in SSR — see the method comment.
    return state.build();
  }

  /**
   * The next INSERT root to apply: the oldest one created after {@code lastCreatedAtMillis} (so an
   * insert already reflected in a mid-chain base is not re-applied) that has not already been
   * applied (idempotency). Inserts failing either test are discarded — chain order is
   * created-at-monotonic, so they can never apply on a later iteration.
   */
  @Nullable
  private static RedoOp nextInsert(
      Deque<RedoOp> insertQueue, long lastCreatedAtMillis, RecordState.Builder state) {
    while (!insertQueue.isEmpty()) {
      RedoOp candidate = insertQueue.poll();
      if (candidate.createdAtMillis() <= lastCreatedAtMillis) {
        continue; // Predates the current position — already reflected.
      }
      if (state.isInsertApplied(candidate.txId())) {
        continue; // Already applied (idempotent re-run).
      }
      return candidate;
    }
    return null;
  }
}
