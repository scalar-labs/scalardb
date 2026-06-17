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

/**
 * Pass 2: replays each bucket's redo ops onto the records being restored. A worker owns whole
 * buckets, so there is no intra-key concurrency — no CAS, no locks (the design's core
 * simplification over SSR). Within a bucket, ops are grouped by {@link RecordKey} and each key is
 * replayed by the cursor-driven primitive ({@link #replayKey}), which mirrors SSR's {@code
 * RecordApplyService.findWriteOperationsToApply}.
 */
public final class RecordApplier {
  private final RestoredRecordReader reader;
  private final IntegrityChecker integrityChecker = new IntegrityChecker();
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
   * restored and that key's ops, follows the {@code prevTxId -> txId} chain to the final state.
   * Order- and input-independent within a key, and idempotent.
   */
  RecordState replayKey(RecordKey key, List<RedoOp> ops) {
    RecordState current = reader.get(key);
    integrityChecker.check(key, ops, current);

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
    while (true) {
      RedoOp op;
      if (state.currentTxId() == null || state.deleted()) {
        // Record absent or deleted: the next op must be an INSERT root.
        op = insertQueue.poll();
        if (op == null) {
          break;
        }
        if (state.isInsertApplied(op.txId())) {
          continue; // Root already applied on a prior run — dedup (idempotency).
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
    }
    return state.build();
  }
}
