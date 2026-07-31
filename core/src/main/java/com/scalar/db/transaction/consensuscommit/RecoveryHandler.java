package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils.getTransactionTableMetadata;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.Key;
import com.scalar.db.util.ScalarDbUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class RecoveryHandler {
  @VisibleForTesting static final long TRANSACTION_LIFETIME_MILLIS = 15000;
  private static final Logger logger = LoggerFactory.getLogger(RecoveryHandler.class);
  private final DistributedStorage storage;
  private final CoordinatorStateAccessor coordinator;
  private final TransactionTableMetadataManager tableMetadataManager;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public RecoveryHandler(
      DistributedStorage storage,
      CoordinatorStateAccessor coordinator,
      TransactionTableMetadataManager tableMetadataManager) {
    this.storage = checkNotNull(storage);
    this.coordinator = checkNotNull(coordinator);
    this.tableMetadataManager = checkNotNull(tableMetadataManager);
  }

  /**
   * Best-effort recovery of the given record according to the writer transaction's coordinator
   * state. This is the entry point for lazy recovery on the read path: it advances recovery as far
   * as it cheaply can but does not guarantee the record reaches its resolved physical state, and it
   * returns nothing.
   *
   * <ul>
   *   <li>State present {@code COMMITTED}: roll the record forward.
   *   <li>State present otherwise (i.e. {@code ABORTED}): roll the record back.
   *   <li>State absent and the writer expired: abort the writer and roll the record back; if a
   *       concurrent actor already resolved the writer (the abort is conflict-absorbed), the record
   *       is left for that actor / a subsequent lazy recovery — a later read will recover it.
   *   <li>State absent and the writer not expired: perform no recovery, since the writer may still
   *       be in flight.
   * </ul>
   *
   * <p>Use {@link #recover(Selection, TransactionResult, Optional)} instead when the caller needs a
   * guarantee that the record has been physically resolved on return (e.g. the synchronous {@code
   * recoverRecord} API).
   *
   * @param selection the selection that identifies the record to recover
   * @param result the latest known {@link TransactionResult} for the record
   * @param state the coordinator state of the writer transaction, or empty if it has no state row
   * @throws ExecutionException if the underlying storage read or mutation fails
   * @throws CoordinatorException if reading or updating the coordinator state fails
   */
  public void tryRecover(
      Selection selection, TransactionResult result, Optional<CoordinatorStateAccessor.State> state)
      throws ExecutionException, CoordinatorException {
    if (state.isPresent()) {
      rollRecordToState(selection, result, state.get());
    } else {
      abortIfExpired(selection, result, false);
    }
  }

  /**
   * Recovers the given record according to the writer transaction's coordinator state, guaranteeing
   * that when {@code true} is returned the record has been physically resolved (rolled forward or
   * back). This is the entry point for callers that must be able to rely on the record being
   * recovered once the call succeeds.
   *
   * <ul>
   *   <li>State present {@code COMMITTED}: roll the record forward; returns {@code true}.
   *   <li>State present otherwise (i.e. {@code ABORTED}): roll the record back; returns {@code
   *       true}.
   *   <li>State absent and the writer expired: abort the writer and roll the record back; returns
   *       {@code true}. If a concurrent actor already resolved the writer (the abort is
   *       conflict-absorbed), the record is rolled to the winner's terminal outcome. If the
   *       writer's state row is already gone, {@code true} is still returned — the Coordinator
   *       state cleanup process removed the state row after the winner committed, which also
   *       resolved this record, so the record is already consistent.
   *   <li>State absent and the writer not expired: perform no recovery and return {@code false},
   *       since the writer may still be in flight. This is a "not yet recoverable" outcome; the
   *       caller can retry later.
   * </ul>
   *
   * @param selection the selection that identifies the record to recover
   * @param result the latest known {@link TransactionResult} for the record
   * @param state the coordinator state of the writer transaction, or empty if it has no state row
   * @return {@code true} if the record was recovered (resolved to a terminal state), {@code false}
   *     if the writer may still be in flight and the call should be retried later
   * @throws ExecutionException if the underlying storage read or mutation fails
   * @throws CoordinatorException if reading or updating the coordinator state fails
   */
  public boolean recover(
      Selection selection, TransactionResult result, Optional<CoordinatorStateAccessor.State> state)
      throws ExecutionException, CoordinatorException {
    if (state.isPresent()) {
      recover(selection, result, state.get());
      return true;
    } else {
      return abortIfExpired(selection, result, true);
    }
  }

  /**
   * Recovers the given record when the writer's coordinator state is already known to be present.
   * With a present state the record is always rolled (forward if {@code COMMITTED}, back
   * otherwise); the coordinator is never read or written, so — unlike {@link #recover(Selection,
   * TransactionResult, Optional)} — this neither defers the roll nor throws {@link
   * CoordinatorException}. This is the entry point for callers that already hold a terminal
   * coordinator state.
   *
   * @param selection the selection that identifies the record to recover
   * @param result the latest known {@link TransactionResult} for the record
   * @param state the present terminal coordinator state of the writer transaction
   * @throws ExecutionException if the underlying storage read or mutation fails
   */
  public void recover(
      Selection selection, TransactionResult result, CoordinatorStateAccessor.State state)
      throws ExecutionException {
    rollRecordToState(selection, result, state);
  }

  /**
   * Rolls the record forward (if {@code state} is {@code COMMITTED}) or back (if {@code ABORTED}).
   *
   * <p>{@code state} must be a terminal state. The coordinator only ever persists terminal states,
   * so every caller passes a {@code COMMITTED} or {@code ABORTED} state. The {@code assert}
   * documents that invariant for developers and trips on a corrupt non-terminal state when
   * assertions are enabled (e.g. in tests); in production, where assertions are disabled, a
   * non-terminal state is treated as a rollback.
   */
  private void rollRecordToState(
      Selection selection, TransactionResult result, CoordinatorStateAccessor.State state)
      throws ExecutionException {
    if (state.getState().equals(TransactionState.COMMITTED)) {
      rollforwardRecord(selection, result, state.getCreatedAt());
    } else {
      assert state.getState().equals(TransactionState.ABORTED);
      rollbackRecord(selection, result);
    }
  }

  void rollbackRecord(Selection selection, TransactionResult result) throws ExecutionException {
    assert selection.forFullTableName().isPresent();

    TransactionTableMetadata tableMetadata =
        getTransactionTableMetadata(tableMetadataManager, selection);
    Key partitionKey = ScalarDbUtils.getPartitionKey(result, tableMetadata.getTableMetadata());
    Optional<Key> clusteringKey =
        ScalarDbUtils.getClusteringKey(result, tableMetadata.getTableMetadata());

    logger.info(
        "Rolling back for a record. Table: {}; Partition Key: {}; Clustering Key: {}; Transaction ID that wrote the record: {}",
        selection.forFullTableName().get(),
        partitionKey,
        clusteringKey,
        result.getId());

    RollbackMutationComposer composer = createRollbackMutationComposer(selection, result);

    try {
      mutate(composer.get());
    } catch (NoMutationException e) {
      logger.info(
          "Rolling back for a record failed. Table: {}; Partition Key: {}; Clustering Key: {}; Transaction ID that wrote the record: {}",
          selection.forFullTableName().get(),
          partitionKey,
          clusteringKey,
          result.getId(),
          e);

      // This can happen when the record has already been rolled back by another transaction. In
      // this case, we just ignore it.
    }
  }

  @VisibleForTesting
  RollbackMutationComposer createRollbackMutationComposer(
      Selection selection, TransactionResult result) throws ExecutionException {
    RollbackMutationComposer composer =
        new RollbackMutationComposer(result.getId(), storage, tableMetadataManager);
    composer.add(selection, result);
    return composer;
  }

  @VisibleForTesting
  void rollforwardRecord(Selection selection, TransactionResult result, long committedAt)
      throws ExecutionException {
    assert selection.forFullTableName().isPresent();

    TransactionTableMetadata tableMetadata =
        getTransactionTableMetadata(tableMetadataManager, selection);
    Key partitionKey = ScalarDbUtils.getPartitionKey(result, tableMetadata.getTableMetadata());
    Optional<Key> clusteringKey =
        ScalarDbUtils.getClusteringKey(result, tableMetadata.getTableMetadata());

    logger.info(
        "Rolling forward for a record. Table: {}; Partition Key: {}; Clustering Key: {}; Transaction ID that wrote the record: {}",
        selection.forFullTableName().get(),
        partitionKey,
        clusteringKey,
        result.getId());

    CommitMutationComposer composer = createCommitMutationComposer(selection, result, committedAt);

    try {
      mutate(composer.get());
    } catch (NoMutationException e) {
      logger.info(
          "Rolling forward for a record failed. Table: {}; Partition Key: {}; Clustering Key: {}; Transaction ID that wrote the record: {}",
          selection.forFullTableName().get(),
          partitionKey,
          clusteringKey,
          result.getId(),
          e);

      // This can happen when the record has already been committed by another transaction. In this
      // case, we just ignore it.
    }
  }

  @VisibleForTesting
  CommitMutationComposer createCommitMutationComposer(
      Selection selection, TransactionResult result, long committedAt) throws ExecutionException {
    CommitMutationComposer composer =
        new CommitMutationComposer(result.getId(), committedAt, tableMetadataManager);
    composer.add(selection, result);
    return composer;
  }

  /**
   * Aborts the transaction that wrote a record, for the read path, when that transaction has
   * expired and has no coordinator state, by writing its ABORTED coordinator state (the
   * lazy-recovery rollback). The before-image of such a record is only the correct value to return
   * once that transaction is known to be aborted, which this confirms.
   *
   * @param id the transaction id of the transaction that wrote the record. The caller must have
   *     already confirmed the transaction is expired and has no coordinator state; this method does
   *     not check either condition
   * @return {@code true} if the ABORTED state was written — the transaction is now aborted, so
   *     returning the before-image is correct; {@code false} if writing it conflicted because a
   *     concurrent actor resolved the transaction (e.g. it committed), in which case the caller
   *     must re-read the coordinator state and resolve the read from that outcome instead of
   *     returning a stale before-image
   * @throws CoordinatorException if writing the ABORTED state fails for a reason other than a
   *     conflict
   */
  boolean tryAbortExpiredTransaction(String id) throws CoordinatorException {
    try {
      coordinator.forceAbort(id);
      return true;
    } catch (CoordinatorConflictException e) {
      logger.info(
          "Putting state in coordinator for a record conflicted; a concurrent actor resolved the transaction. Transaction ID: {}",
          id,
          e);
      return false;
    }
  }

  /**
   * Aborts the writer of an uncommitted record if it has expired, and reports whether the record
   * was resolved.
   *
   * @param ensureRecordResolved when {@code true}, the targeted record is guaranteed to be
   *     physically resolved on return even when the abort conflicts with a concurrent actor (the
   *     {@code recover} path); when {@code false}, a conflicting abort leaves the record for the
   *     winning actor / a subsequent lazy recovery (the best-effort {@code tryRecover} path).
   * @return {@code true} if the record was definitely recovered (the writer was aborted and the
   *     record rolled, or a concurrent actor's outcome was applied), {@code false} if recovery
   *     could not be guaranteed — either the writer has not expired and may still be in flight, or
   *     the abort conflicted in the best-effort path and the targeted record was left for the
   *     winning actor / a subsequent lazy recovery
   */
  private boolean abortIfExpired(
      Selection selection, TransactionResult result, boolean ensureRecordResolved)
      throws CoordinatorException, ExecutionException {
    assert selection.forFullTableName().isPresent();

    if (!isTransactionExpired(result)) {
      // The writer may still be in flight; do not abort it. Report that the record is not yet
      // recoverable so callers can retry later.
      return false;
    }

    // result is the expired uncommitted writer being recovered, so its tx_id is non-null here;
    // getId() is @Nullable only for deemed-committed records, which are never recovered.
    String txId = result.getId();
    assert txId != null;

    // Before writing the ABORTED state, physically re-read the record. A Coordinator state cleanup
    // process can finalize the record and remove the writer's state row, so an expired writer with
    // no coordinator state no longer implies the record is still uncommitted: the writer may have
    // committed and been cleaned up. Only abort when the record is still uncommitted by this
    // writer; otherwise writing the ABORTED state would leave a spurious tombstone for an
    // already-resolved (possibly committed) transaction.
    Optional<TransactionResult> latest =
        ConsensusCommitUtils.rereadRecord(storage, tableMetadataManager, selection, result);
    if (!latest.isPresent() || latest.get().isCommitted() || !txId.equals(latest.get().getId())) {
      // The record is gone, already committed, or re-prepared by a different transaction — this
      // writer's record has already been resolved (rolled and possibly replaced), so the record is
      // consistent and no ABORTED state should be written.
      return true;
    }

    try {
      coordinator.forceAbort(txId);
    } catch (CoordinatorConflictException e) {
      TransactionTableMetadata tableMetadata =
          getTransactionTableMetadata(tableMetadataManager, selection);
      Key partitionKey = ScalarDbUtils.getPartitionKey(result, tableMetadata.getTableMetadata());
      Optional<Key> clusteringKey =
          ScalarDbUtils.getClusteringKey(result, tableMetadata.getTableMetadata());

      logger.info(
          "Putting state in coordinator for a record conflicted; a concurrent actor resolved the transaction. "
              + "Table: {}; Partition Key: {}; Clustering Key: {}; Transaction ID that wrote the record: {}",
          selection.forFullTableName().get(),
          partitionKey,
          clusteringKey,
          txId,
          e);

      if (!ensureRecordResolved) {
        // Best-effort path (tryRecover): the targeted record is left for the winning actor / a
        // subsequent lazy recovery, so this call did not resolve it. Skip the coordinator re-read
        // entirely — it would be wasted work — and report that recovery is not guaranteed. The
        // return value is discarded by the caller (tryRecover is void), so this only documents the
        // honest outcome of the best-effort path.
        return false;
      }

      // Guaranteed path (recover): a concurrent actor already resolved the writer. Re-read the
      // coordinator state and roll the record to the winner's outcome so it is physically resolved
      // on return.
      Optional<CoordinatorStateAccessor.State> rereadState = coordinator.getState(txId);

      if (rereadState.isPresent()) {
        // Roll the targeted record to the winner's outcome. This is idempotent: the underlying
        // composers absorb NoMutationException if the winner already rolled the record.
        rollRecordToState(selection, result, rereadState.get());
        return true;
      }

      // The writer's state row is already gone — which only happens after the Coordinator state
      // cleanup process removes the state row, and that process also resolves this record before
      // the cleanup. The record is therefore already consistent.
      return true;
    }

    rollbackRecord(selection, result);
    return true;
  }

  private void mutate(List<Mutation> mutations) throws ExecutionException {
    if (mutations.isEmpty()) {
      return;
    }
    storage.mutate(mutations);
  }

  boolean isTransactionExpired(TransactionResult result) {
    long current = System.currentTimeMillis();
    return current > result.getPreparedAt() + TRANSACTION_LIFETIME_MILLIS;
  }
}
