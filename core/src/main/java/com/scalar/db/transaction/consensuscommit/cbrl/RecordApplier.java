package com.scalar.db.transaction.consensuscommit.cbrl;

import com.google.common.base.Preconditions;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pass 2: replays each bucket's redo ops onto the records being restored and writes the result
 * back. A worker owns whole buckets, so there is no intra-key concurrency — no CAS, no locks (the
 * design's core simplification over SSR). Within a bucket, ops are grouped by {@link RecordKey} and
 * each key runs its whole recover&rarr;read&rarr;replay&rarr;write-back pipeline on that one
 * worker: {@link #computeWriteOps} mirrors SSR's {@code
 * RecordApplyService.findWriteOperationsToApply} (the per-key recovery and base read happen inside
 * the injected {@link RestoredRecordReader}), then the reconstructed state is handed to the {@link
 * RecordSink} for write-back.
 */
final class RecordApplier {
  private static final Logger logger = LoggerFactory.getLogger(RecordApplier.class);
  // Bound on how long apply() waits for workers to drain on shutdown. The happy path returns
  // immediately (every future already resolved); this only bites if a worker is wedged in a storage
  // call after a failure.
  private static final long SHUTDOWN_TIMEOUT_SECONDS = 60;
  private final RestoredRecordReader reader;

  RecordApplier(RestoredRecordReader reader) {
    this.reader = reader;
  }

  /**
   * Replays all buckets and hands each key's reconstructed final state to {@code sink}. {@code
   * workerCount} workers each own whole buckets ({@code M <= N}), so a key's whole pipeline —
   * recover, read the base, replay, write back — runs on one worker with no intra-key concurrency.
   */
  void apply(List<RedoBucket> buckets, int workerCount, RecordSink sink)
      throws InterruptedException {
    Preconditions.checkArgument(workerCount >= 1, "workerCount must be >= 1");
    ExecutorService executor =
        Executors.newFixedThreadPool(Math.min(workerCount, Math.max(1, buckets.size())));
    try {
      List<Future<Void>> futures = new ArrayList<>(buckets.size());
      for (RedoBucket bucket : buckets) {
        futures.add(
            executor.submit(
                () -> {
                  applyBucket(bucket, sink);
                  return null;
                }));
      }
      for (Future<Void> future : futures) {
        future.get();
      }
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof CbrlReplayException) {
        throw (CbrlReplayException) cause;
      }
      throw new CbrlReplayException("Replay failed", cause);
    } finally {
      // Drain before returning: a worker may still be mid-write-back when one bucket failed, and
      // storage calls do not honor interruption, so shutdownNow() alone would let apply() return
      // while another worker is still mutating storage.
      shutdownAndAwait(executor);
    }
  }

  /**
   * Graceful shutdown: stop accepting work and let the running workers finish, then force-cancel
   * and wait again if they overrun the bound — so apply() never returns while a worker still
   * touches storage.
   */
  private static void shutdownAndAwait(ExecutorService executor) {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        executor.shutdownNow();
        if (!executor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
          logger.warn("Replay workers did not terminate after shutdown.");
        }
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Replays one bucket's keys independently, writing each reconstructed state back via {@code
   * sink}.
   */
  private void applyBucket(RedoBucket bucket, RecordSink sink) throws Exception {
    Map<RecordKey, List<RedoOperation>> byKey = new HashMap<>();
    for (RedoOperation op : bucket.read()) {
      byKey.computeIfAbsent(op.key(), k -> new ArrayList<>()).add(op);
    }
    for (Map.Entry<RecordKey, List<RedoOperation>> entry : byKey.entrySet()) {
      sink.writeBack(entry.getKey(), computeWriteOps(entry.getKey(), entry.getValue()));
    }
  }

  /**
   * The replay primitive (PoC plan §5). Given the key's current state in the database being
   * restored and that key's ops, follows the {@code prevTxId -> txId} chain forward from the
   * record's current version to the final state. Correctness — what gets applied — is determined
   * solely by this chain, never by {@code created_at} or {@code tx_version} (the PoC plan's highest
   * rule); {@code created_at} is used only as a try-order optimization for independent INSERT roots
   * (see below), which cannot change the chain's outcome. Order- and input-independent within a
   * key, and idempotent.
   *
   * <p>An op that does not connect to the chain reachable forward from the current version is left
   * unapplied, mirroring SSR's {@code findWriteOperationsToApply} (which keeps such ops as {@code
   * remainingWriteOperations} rather than rejecting them). This is what makes <b>windowed
   * repair</b> work: under window-scoped logging a record's chain root predates the logging window
   * and is never captured, so the first in-window op links to a {@code prev_tx_id} that is neither
   * another op nor — once the copy's base has advanced past it — the current version. That op sits
   * below the base, whose state already reflects it, and is correctly skipped. The copy may also
   * have captured a deleted or arbitrarily-advanced version, which carries no position information,
   * so — like SSR — replay does not try to tell a legitimately-below op apart from a genuinely
   * dropped mid-chain op; completeness is the backup capture's responsibility (full coordinator
   * scan + chain closure), not this primitive's.
   */
  RecordState computeWriteOps(RecordKey key, List<RedoOperation> ops) {
    RecordState current = reader.get(key);

    // divideWriteOperations: INSERT roots vs the prevTxId-keyed chain of UPDATE/DELETE ops.
    // producedBy maps each op's resulting version (its tx id) to the op, so the chain can be walked
    // backward from the base to find the versions it already reflects.
    List<RedoOperation> insertList = new ArrayList<>();
    Map<String, RedoOperation> nonInsertOps = new HashMap<>();
    Map<String, RedoOperation> producedBy = new HashMap<>();
    for (RedoOperation op : ops) {
      producedBy.put(op.txId(), op);
      if (op.isInsert()) {
        insertList.add(op);
      } else if (op.prevTxId() != null) {
        // Chain by prev_tx_id. Like SSR, a later op keyed on the same prev_tx_id simply overwrites
        // (last wins) — a fork cannot arise from serializable commit, so replay does not guard
        // against it.
        nonInsertOps.put(op.prevTxId(), op);
      } else {
        // A non-INSERT op with no captured prior committed version — e.g. a DELETE/UPDATE of a
        // deemed-as-committed (imported / pre-ConsensusCommit) record. Not expected for
        // ConsensusCommit-managed data, and unreachable from the chain walk (which advances only by
        // a non-null cursor), so it is left unapplied. Warn so the anomaly is visible rather than
        // silently swallowed.
        logger.warn(
            "Skipping a {} redo op on {} with no prev_tx_id (no captured prior committed version) —"
                + " unexpected for ConsensusCommit-managed data. Transaction ID: {}",
            op.isDelete() ? "DELETE" : "UPDATE",
            key,
            op.txId());
      }
    }
    // Apply INSERT roots oldest-first by commit time, with tx id as the tiebreak that makes this a
    // total order (so restore is reproducible regardless of scan/bucket order). created_at is a
    // wall-clock stamp and can be skewed across nodes, so "oldest" is only a best-effort heuristic,
    // never authoritative — correctness rests solely on the causal, skew-immune chain (prev_tx_id
    // ->
    // tx_id). A well-formed (chain-closed) backup has at most one live root, so this ordering never
    // changes a correct result; it is only a safety net (a reproducible, not scan-order-dependent,
    // result even if a malformed backup somehow left two live roots) and an optimization (fewer
    // superseded roots tried). Mirrors SSR's RecordApplyService.
    insertList.sort(
        Comparator.comparingLong(RedoOperation::committedAt).thenComparing(RedoOperation::txId));
    Deque<RedoOperation> insertQueue = new ArrayDeque<>(insertList);
    // Versions the base already reflects: its current tx id and every chain-ancestor reachable by
    // walking prev_tx_id back through the captured ops. A root in this set was applied before the
    // backup and must not be re-applied after a DELETE during windowed repair — identified purely
    // from the chain, never from a timestamp.
    Set<String> reflected = reflectedVersions(current.currentTxId(), producedBy);

    RecordState.Builder state = current.toBuilder();
    while (true) {
      RedoOperation op;
      if (state.currentTxId() == null || state.deleted()) {
        // Record absent or deleted: resume from an INSERT root the base does not already reflect.
        op = nextInsert(insertQueue, reflected, state);
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
      // Stamp the version and commit time this write produced (write-back metadata, not ordering).
      state.setVersion(op.version());
      state.setCommittedAt(op.committedAt());
    }
    // Ops left in nonInsertOps were never reached (window-boundary or below-base links). They are
    // tolerated, as in SSR — see the method comment.
    return state.build();
  }

  /**
   * The versions the base already reflects: the base's current tx id and its chain-ancestors,
   * reached by walking {@code prev_tx_id} back through the captured ops until the link leaves the
   * captured set (the window boundary) or a root is hit. Used to skip INSERT roots that predate the
   * base — chain-only, no {@code created_at}/{@code tx_version} (highest rule).
   */
  private static Set<String> reflectedVersions(
      @Nullable String baseTxId, Map<String, RedoOperation> producedBy) {
    Set<String> reflected = new HashSet<>();
    String txId = baseTxId;
    while (txId != null && producedBy.containsKey(txId) && reflected.add(txId)) {
      txId = producedBy.get(txId).prevTxId();
    }
    return reflected;
  }

  /**
   * The next INSERT root to apply: one that is neither already reflected in the base (a
   * chain-ancestor of the base's current version) nor already applied this run (idempotency).
   * Inserts failing either test are discarded. Selection is order-independent — the chain converges
   * to the same final version regardless of which root is taken first.
   */
  @Nullable
  private static RedoOperation nextInsert(
      Deque<RedoOperation> insertQueue, Set<String> reflected, RecordState.Builder state) {
    while (!insertQueue.isEmpty()) {
      RedoOperation candidate = insertQueue.poll();
      if (reflected.contains(candidate.txId())) {
        continue; // Already reflected in the base (chain-ancestor of the current version).
      }
      if (state.isInsertApplied(candidate.txId())) {
        continue; // Already applied (idempotent re-run).
      }
      return candidate;
    }
    return null;
  }
}
