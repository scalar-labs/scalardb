package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.concurrent.LazyInit;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.storage.RetriableExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.exception.transaction.ValidationException;
import com.scalar.db.transaction.consensuscommit.Coordinator.State;
import com.scalar.db.transaction.consensuscommit.ParallelExecutor.ParallelExecutorTask;
import com.scalar.db.transaction.consensuscommit.proto.v1.WriteSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class CommitHandler {
  private static final Logger logger = LoggerFactory.getLogger(CommitHandler.class);
  private final DistributedStorage storage;
  protected final Coordinator coordinator;
  private final TransactionTableMetadataManager tableMetadataManager;
  private final ParallelExecutor parallelExecutor;
  private final MutationsGrouper mutationsGrouper;
  final WriteSetEncoder writeSetEncoder;
  protected final boolean coordinatorWriteOmissionOnReadOnlyEnabled;
  private final boolean onePhaseCommitEnabled;

  @LazyInit @Nullable private BeforePreparationHook beforePreparationHook;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public CommitHandler(
      DistributedStorage storage,
      Coordinator coordinator,
      TransactionTableMetadataManager tableMetadataManager,
      ParallelExecutor parallelExecutor,
      MutationsGrouper mutationsGrouper,
      boolean coordinatorWriteOmissionOnReadOnlyEnabled,
      boolean onePhaseCommitEnabled) {
    this.storage = checkNotNull(storage);
    this.coordinator = checkNotNull(coordinator);
    this.tableMetadataManager = checkNotNull(tableMetadataManager);
    this.parallelExecutor = checkNotNull(parallelExecutor);
    this.mutationsGrouper = checkNotNull(mutationsGrouper);
    this.writeSetEncoder = new WriteSetEncoder(tableMetadataManager);
    this.coordinatorWriteOmissionOnReadOnlyEnabled = coordinatorWriteOmissionOnReadOnlyEnabled;
    this.onePhaseCommitEnabled = onePhaseCommitEnabled;
  }

  /**
   * A callback invoked when any exception occurs before committing transactions.
   *
   * @param context the transaction context
   */
  protected void onFailureBeforeCommit(TransactionContext context) {}

  private void safelyCallOnFailureBeforeCommit(TransactionContext context) {
    try {
      onFailureBeforeCommit(context);
    } catch (Exception e) {
      logger.warn("Failed to call the callback. Transaction ID: {}", context.transactionId, e);
    }
  }

  /**
   * Aborts the coordinator state and rolls back records as needed after a pre-commit failure. When
   * the transaction has no writes and deletes, there are no records to roll back. In that case the
   * coordinator state is still aborted unless coordinator write omission on read-only is enabled:
   * when it is, a write-less transaction (a read-only one, or a non-read-only one with an empty
   * write set) writes no coordinator state row on the commit path either, so there is nothing to
   * abort.
   */
  private void abortStateAndRollbackRecordsIfNeeded(
      TransactionContext context, boolean hasWritesOrDeletesInSnapshot)
      throws UnknownTransactionStatusException {
    if (hasWritesOrDeletesInSnapshot || !coordinatorWriteOmissionOnReadOnlyEnabled) {
      abortState(context);
    }
    if (hasWritesOrDeletesInSnapshot) {
      rollbackRecords(context);
    }
  }

  private Optional<Future<Void>> invokeBeforePreparationHook(
      TransactionContext context, boolean hasWritesOrDeletesInSnapshot)
      throws UnknownTransactionStatusException, CommitException {
    if (beforePreparationHook == null) {
      return Optional.empty();
    }

    try {
      return Optional.of(beforePreparationHook.handle(tableMetadataManager, context));
    } catch (Exception e) {
      safelyCallOnFailureBeforeCommit(context);

      abortStateAndRollbackRecordsIfNeeded(context, hasWritesOrDeletesInSnapshot);

      throw new CommitException(
          CoreError.CONSENSUS_COMMIT_HANDLING_BEFORE_PREPARATION_HOOK_FAILED.buildMessage(
              e.getMessage()),
          e,
          context.transactionId);
    }
  }

  private void waitBeforePreparationHookFuture(
      TransactionContext context,
      @Nullable Future<Void> beforePreparationHookFuture,
      boolean hasWritesOrDeletesInSnapshot)
      throws UnknownTransactionStatusException, CommitException {
    if (beforePreparationHookFuture == null) {
      return;
    }

    try {
      beforePreparationHookFuture.get();
    } catch (Exception e) {
      safelyCallOnFailureBeforeCommit(context);

      abortStateAndRollbackRecordsIfNeeded(context, hasWritesOrDeletesInSnapshot);

      throw new CommitException(
          CoreError.CONSENSUS_COMMIT_HANDLING_BEFORE_PREPARATION_HOOK_FAILED.buildMessage(
              e.getMessage()),
          e,
          context.transactionId);
    }
  }

  public void commit(TransactionContext context)
      throws CommitException, UnknownTransactionStatusException {
    boolean hasWritesOrDeletesInSnapshot =
        !context.readOnly && context.snapshot.hasWritesOrDeletes();

    Optional<Future<Void>> beforePreparationHookFuture =
        invokeBeforePreparationHook(context, hasWritesOrDeletesInSnapshot);

    if (canOnePhaseCommit(context)) {
      try {
        onePhaseCommitRecords(context);
        return;
      } catch (Exception e) {
        safelyCallOnFailureBeforeCommit(context);
        throw e;
      }
    }

    if (hasWritesOrDeletesInSnapshot) {
      try {
        prepareRecords(context);
      } catch (PreparationException e) {
        safelyCallOnFailureBeforeCommit(context);
        abortState(context);
        rollbackRecords(context);
        if (e instanceof PreparationConflictException) {
          throw new CommitConflictException(e.getMessage(), e, e.getTransactionId().orElse(null));
        }
        throw new CommitException(e.getMessage(), e, e.getTransactionId().orElse(null));
      } catch (Exception e) {
        safelyCallOnFailureBeforeCommit(context);
        throw e;
      }
    }

    try {
      validateRecords(context);
    } catch (ValidationException e) {
      safelyCallOnFailureBeforeCommit(context);

      abortStateAndRollbackRecordsIfNeeded(context, hasWritesOrDeletesInSnapshot);

      if (e instanceof ValidationConflictException) {
        throw new CommitConflictException(e.getMessage(), e, e.getTransactionId().orElse(null));
      }
      throw new CommitException(e.getMessage(), e, e.getTransactionId().orElse(null));
    } catch (Exception e) {
      safelyCallOnFailureBeforeCommit(context);
      throw e;
    }

    waitBeforePreparationHookFuture(
        context, beforePreparationHookFuture.orElse(null), hasWritesOrDeletesInSnapshot);

    if (hasWritesOrDeletesInSnapshot || !coordinatorWriteOmissionOnReadOnlyEnabled) {
      commitState(context);
    }
    if (hasWritesOrDeletesInSnapshot) {
      commitRecords(context);
    }
  }

  @VisibleForTesting
  boolean canOnePhaseCommit(TransactionContext context) throws CommitException {
    if (!onePhaseCommitEnabled) {
      return false;
    }

    // If validation is required, we cannot one-phase commit the transaction
    if (context.isValidationRequired()) {
      return false;
    }

    // If the snapshot has no write and deletes, we do not one-phase commit the transaction
    if (!context.snapshot.hasWritesOrDeletes()) {
      return false;
    }

    Collection<Map.Entry<Snapshot.Key, Delete>> deleteSetEntries = context.snapshot.getDeleteSet();

    // If a record corresponding to a delete in the delete set does not exist in the storage,　we
    // cannot one-phase commit the transaction. This is because the storage does not support
    // delete-if-not-exists semantics, so we cannot detect conflicts with other transactions.
    for (Map.Entry<Snapshot.Key, Delete> entry : deleteSetEntries) {
      Delete delete = entry.getValue();
      Optional<TransactionResult> result =
          context.snapshot.getFromReadSet(new Snapshot.Key(delete));

      // For deletes, we always perform implicit pre-reads if the result does not exit in the read
      // set. So the result should always exist in the read set.
      assert result != null;

      if (!result.isPresent()) {
        return false;
      }
    }

    try {
      // If the mutations can be grouped altogether, the mutations can be done in a single mutate
      // API call, so we can one-phase commit the transaction
      return mutationsGrouper.canBeGroupedAltogether(
          Stream.concat(
                  context.snapshot.getWriteSet().stream().map(Map.Entry::getValue),
                  deleteSetEntries.stream().map(Map.Entry::getValue))
              .collect(Collectors.toList()));
    } catch (ExecutionException e) {
      throw new CommitException(
          CoreError.CONSENSUS_COMMIT_COMMITTING_RECORDS_FAILED.buildMessage(e.getMessage()),
          e,
          context.transactionId);
    }
  }

  protected void handleCommitConflict(TransactionContext context, Exception cause)
      throws CommitConflictException, UnknownTransactionStatusException {
    try {
      Optional<State> s = coordinator.getState(context.transactionId);
      if (s.isPresent()) {
        TransactionState state = s.get().getState();
        if (state.equals(TransactionState.ABORTED)) {
          rollbackRecords(context);
          throw new CommitConflictException(
              CoreError.CONSENSUS_COMMIT_CONFLICT_OCCURRED_WHEN_COMMITTING_STATE.buildMessage(
                  cause.getMessage()),
              cause,
              context.transactionId);
        }
        // Otherwise the coordinator state is present and COMMITTED, which means this transaction
        // has already committed. Only Two-phase Commit I/F reaches this branch: there the same
        // transaction's commit can be driven more than once -- re-invoked for recovery, or
        // committed by multiple participants -- so an earlier or concurrent commit of this
        // transaction may have already written its COMMITTED state, and this commit then loses the
        // putIfNotExists race and observes it here. With One-phase Commit I/F this is unreachable:
        // this commit is the only writer of the COMMITTED state and it just lost the race, so the
        // conflicting row was an ABORTED from a lazy recovery, handled above. The transaction is
        // committed, so return normally and let the caller commit the records.
        //
        // TODO: revisit this if/when the Two-phase Commit I/F is removed -- it would
        // then be unreachable (a COMMITTED state could never be observed after a conflict here).
      } else {
        // The coordinator state is absent: a row existed when our putIfNotExists lost the race, but
        // it is gone now. In both interfaces this means the conflicting row was an ABORTED written
        // by a lazy recovery (which also rolled the records back) and later removed by the
        // Coordinator state cleanup process, so the transaction is definitively aborted. Roll the
        // records back and report a conflict -- the same outcome as the present-ABORTED case above.
        //
        // A COMMITTED row can be ruled out here in both interfaces:
        //   - One-phase Commit I/F: this commit is the only writer of this transaction's COMMITTED
        //     state, and it just lost the race, so the conflicting row could only have been an
        //     ABORTED from a lazy recovery -- a COMMITTED for this transaction never existed.
        //   - Two-phase Commit I/F: other participants or re-driven commits of the same transaction
        //     can also write COMMITTED, so the conflict could in principle have been against a
        //     COMMITTED row. But the Two-phase Commit I/F does not assume finishTransaction,
        //     and a COMMITTED coordinator row is only ever removed by finishTransaction (the
        //     periodic cleanup removes only ABORTED rows). So a COMMITTED row never disappears
        //     here: had the conflict been against one, it would still be present and handled by
        //     the present-COMMITTED branch above. An absent row therefore means the conflict was
        //     an ABORTED.
        //
        // Group commit reaches an absent state by a second, more common route, and the outcome is
        // still correct. There the conflicting putIfNotExists is keyed on the parent ID (the group
        // emitter writes the parent-ID COMMITTED row), so the row it conflicts with -- a lazy
        // recovery's empty-tx_child_ids ABORTED parent row -- can still be present. getState then
        // resolves by full ID: it finds the parent row, sees it does not list this child, and finds
        // no full-ID row, so it returns empty. That empty does not mean a cleaned-up row; it means
        // this child was never committed (a committed child would either be listed in the parent
        // row or have its own full-ID COMMITTED row, and getState would return it). Rolling back
        // and reporting a retryable conflict is the correct outcome for such an uncommitted child.
        //
        // TODO: revisit this if/when the Two-phase Commit I/F is removed.

        rollbackRecords(context);
        throw new CommitConflictException(
            CoreError.CONSENSUS_COMMIT_CONFLICT_OCCURRED_WHEN_COMMITTING_STATE.buildMessage(
                cause.getMessage()),
            cause,
            context.transactionId);
      }
    } catch (CoordinatorException ex) {
      throw new UnknownTransactionStatusException(
          CoreError.CONSENSUS_COMMIT_CANNOT_GET_COORDINATOR_STATUS.buildMessage(ex.getMessage()),
          ex,
          context.transactionId);
    }
  }

  @VisibleForTesting
  void onePhaseCommitRecords(TransactionContext context)
      throws CommitConflictException, UnknownTransactionStatusException {
    try {
      OnePhaseCommitMutationComposer composer =
          new OnePhaseCommitMutationComposer(context.transactionId, tableMetadataManager);
      context.snapshot.to(composer);

      // One-phase commit does not require grouping mutations and using the parallel executor since
      // it is always executed in a single mutate API call.
      storage.mutate(composer.get());
    } catch (NoMutationException e) {
      throw new CommitConflictException(
          CoreError.CONSENSUS_COMMIT_PREPARING_RECORD_EXISTS.buildMessage(e.getMessage()),
          e,
          context.transactionId);
    } catch (RetriableExecutionException e) {
      throw new CommitConflictException(
          CoreError.CONSENSUS_COMMIT_CONFLICT_OCCURRED_WHEN_COMMITTING_RECORDS.buildMessage(
              e.getMessage()),
          e,
          context.transactionId);
    } catch (ExecutionException e) {
      throw new UnknownTransactionStatusException(
          CoreError.CONSENSUS_COMMIT_ONE_PHASE_COMMITTING_RECORDS_FAILED.buildMessage(
              e.getMessage()),
          e,
          context.transactionId);
    }
  }

  public void prepareRecords(TransactionContext context) throws PreparationException {
    try {
      PrepareMutationComposer composer =
          new PrepareMutationComposer(context.transactionId, tableMetadataManager);
      context.snapshot.to(composer);
      List<List<Mutation>> groupedMutations = mutationsGrouper.groupMutations(composer.get());

      List<ParallelExecutorTask> tasks = new ArrayList<>(groupedMutations.size());
      for (List<Mutation> mutations : groupedMutations) {
        tasks.add(() -> storage.mutate(mutations));
      }
      parallelExecutor.prepareRecords(tasks, context.transactionId);
    } catch (NoMutationException e) {
      throw new PreparationConflictException(
          CoreError.CONSENSUS_COMMIT_PREPARING_RECORD_EXISTS.buildMessage(e.getMessage()),
          e,
          context.transactionId);
    } catch (RetriableExecutionException e) {
      throw new PreparationConflictException(
          CoreError.CONSENSUS_COMMIT_CONFLICT_OCCURRED_WHEN_PREPARING_RECORDS.buildMessage(
              e.getMessage()),
          e,
          context.transactionId);
    } catch (ExecutionException e) {
      throw new PreparationException(
          CoreError.CONSENSUS_COMMIT_PREPARING_RECORDS_FAILED.buildMessage(e.getMessage()),
          e,
          context.transactionId);
    }
  }

  public void validateRecords(TransactionContext context) throws ValidationException {
    if (!context.isValidationRequired()) {
      return;
    }

    try {
      // validation is executed when SERIALIZABLE is chosen.
      context.snapshot.toSerializable(storage);
    } catch (ExecutionException e) {
      throw new ValidationException(
          CoreError.CONSENSUS_COMMIT_VALIDATION_FAILED.buildMessage(e.getMessage()),
          e,
          context.transactionId);
    }
  }

  /**
   * Writes the COMMITTED state with the {@code tx_write_set} populated from the given context's
   * snapshot.
   *
   * @param context the transaction context
   * @throws CommitConflictException if another commit attempt has already written a conflicting
   *     coordinator state
   * @throws UnknownTransactionStatusException if the final transaction status cannot be determined
   */
  public void commitState(TransactionContext context)
      throws CommitConflictException, UnknownTransactionStatusException {
    commitStateInternal(context, writeSetEncoder.encodeSingleGroupWriteSet(context, false));
  }

  /**
   * Writes the COMMITTED state without persisting a {@code tx_write_set}. Intended for callers that
   * don't want per-transaction write-set logging.
   *
   * @param context the transaction context
   * @throws CommitConflictException if another commit attempt has already written a conflicting
   *     coordinator state
   * @throws UnknownTransactionStatusException if the final transaction status cannot be determined
   */
  public void commitStateWithoutWriteSet(TransactionContext context)
      throws CommitConflictException, UnknownTransactionStatusException {
    commitStateInternal(context, null);
  }

  private void commitStateInternal(TransactionContext context, @Nullable WriteSet writeSet)
      throws CommitConflictException, UnknownTransactionStatusException {
    String id = context.transactionId;
    try {
      Coordinator.State state =
          new Coordinator.State(
              id, writeSet, TransactionState.COMMITTED, System.currentTimeMillis());
      coordinator.putState(state);
      logger.debug(
          "Transaction {} is committed successfully at {}", id, System.currentTimeMillis());
    } catch (CoordinatorConflictException e) {
      handleCommitConflict(context, e);
    } catch (CoordinatorException e) {
      throw new UnknownTransactionStatusException(
          CoreError.CONSENSUS_COMMIT_UNKNOWN_COORDINATOR_STATUS.buildMessage(e.getMessage()),
          e,
          id);
    }
  }

  public void commitRecords(TransactionContext context) {
    try {
      CommitMutationComposer composer =
          new CommitMutationComposer(context.transactionId, tableMetadataManager);
      context.snapshot.to(composer);
      List<List<Mutation>> groupedMutations = mutationsGrouper.groupMutations(composer.get());

      List<ParallelExecutorTask> tasks = new ArrayList<>(groupedMutations.size());
      for (List<Mutation> mutations : groupedMutations) {
        tasks.add(() -> storage.mutate(mutations));
      }
      parallelExecutor.commitRecords(tasks, context.transactionId);
    } catch (Exception e) {
      logger.info("Committing records failed. Transaction ID: {}", context.transactionId, e);
      // ignore since records are recovered lazily
    }
  }

  /**
   * Writes the ABORTED state with the {@code tx_write_set} populated from the given context's
   * snapshot.
   *
   * @param context the transaction context
   * @return the resulting transaction state — either {@link TransactionState#ABORTED} or, if a
   *     concurrent writer beat us, whatever state ({@link TransactionState#COMMITTED} or {@link
   *     TransactionState#ABORTED}) is already persisted
   * @throws UnknownTransactionStatusException if the final transaction status cannot be determined
   */
  public TransactionState abortState(TransactionContext context)
      throws UnknownTransactionStatusException {
    return abortStateInternal(
        context.transactionId, writeSetEncoder.encodeSingleGroupWriteSet(context, false));
  }

  /**
   * Writes the ABORTED state without persisting a {@code tx_write_set} using a single {@code
   * putState}. Intended for callers that don't want per-transaction write-set logging.
   *
   * <p>On a {@code putIfNotExists} conflict this resolves a now-absent coordinator state as {@link
   * TransactionState#ABORTED} (see {@link #abortStateInternal}). That is only sound for callers
   * that cannot be racing a real, deletable COMMITTED row: the self-abort path (which provably
   * never committed) and all Two-phase Commit I/F aborts (the Two-phase Commit I/F does not assume
   * {@code finishTransaction}, so a COMMITTED coordinator row is never removed). A One-phase Commit
   * I/F abort-by-id, which can target a transaction that actually committed and whose COMMITTED row
   * {@code finishTransaction} can remove, must NOT use this method — use {@link
   * #abortStateForRollback} instead, which honestly reports {@code UNKNOWN} for an absent row.
   *
   * @param id the transaction ID
   * @return the resulting transaction state — {@link TransactionState#ABORTED} (including when a
   *     concurrent writer's row is absent on re-read), or, if a concurrent writer beat us and its
   *     state is still present, whatever state ({@link TransactionState#COMMITTED} or {@link
   *     TransactionState#ABORTED}) is already persisted
   * @throws UnknownTransactionStatusException if the final transaction status cannot be determined
   */
  public TransactionState abortStateWithoutWriteSet(String id)
      throws UnknownTransactionStatusException {
    return abortStateInternal(id, null);
  }

  private TransactionState abortStateInternal(String id, @Nullable WriteSet writeSet)
      throws UnknownTransactionStatusException {
    try {
      Coordinator.State state =
          new Coordinator.State(id, writeSet, TransactionState.ABORTED, System.currentTimeMillis());
      coordinator.putState(state);
      return TransactionState.ABORTED;
    } catch (CoordinatorConflictException e) {
      // Resolves the final transaction state after our ABORTED putIfNotExists lost the race, for
      // the self-abort path and all Two-phase Commit I/F aborts. Follows the persisted state when
      // present; an absent row is determinable as ABORTED here:
      //   - One-phase Commit I/F self-abort (abortState from a commit-path failure, before
      //     commitState runs): the transaction provably never committed, so the conflicting row
      //     could only have been a lazy-recovery ABORTED, later removed by the cleanup process.
      //   - Two-phase Commit I/F (rollback and abort-by-id): other participants can write
      //     COMMITTED, but the Two-phase Commit I/F does not assume finishTransaction, and a
      //     COMMITTED coordinator row is only ever removed by finishTransaction (the periodic
      //     cleanup removes only ABORTED rows). So a COMMITTED never disappears: had the conflict
      //     been against one, it would still be present and returned above. An absent row therefore
      //     means the conflict was an ABORTED.
      //
      // TODO: revisit this if/when the Two-phase Commit I/F is removed

      return readCoordinatorStateAfterAbortConflict(id, e).orElse(TransactionState.ABORTED);
    } catch (CoordinatorException e) {
      throw new UnknownTransactionStatusException(
          CoreError.CONSENSUS_COMMIT_UNKNOWN_COORDINATOR_STATUS.buildMessage(e.getMessage()),
          e,
          id);
    }
  }

  /**
   * Reads the persisted coordinator state after our ABORTED putIfNotExists lost the race. Returns
   * the already-persisted COMMITTED/ABORTED state when present, or empty when the row is absent
   * (already removed by the Coordinator state cleanup process). The two resolvers below interpret
   * an absent row differently, because whether it is determinable depends on the calling path. A
   * coordinator read failure is undeterminable regardless of path, so it surfaces as
   * UnknownTransactionStatusException with the original conflict preserved as the cause.
   */
  private Optional<TransactionState> readCoordinatorStateAfterAbortConflict(
      String id, CoordinatorConflictException e) throws UnknownTransactionStatusException {
    try {
      return coordinator.getState(id).map(Coordinator.State::getState);
    } catch (CoordinatorException e1) {
      e1.addSuppressed(e);
      throw new UnknownTransactionStatusException(
          CoreError.CONSENSUS_COMMIT_CANNOT_GET_COORDINATOR_STATUS.buildMessage(e1.getMessage()),
          e1,
          id);
    }
  }

  public void rollbackRecords(TransactionContext context) {
    logger.debug("Rollback from snapshot for {}", context.transactionId);
    try {
      RollbackMutationComposer composer =
          new RollbackMutationComposer(context.transactionId, storage, tableMetadataManager);
      context.snapshot.to(composer);
      List<List<Mutation>> groupedMutations = mutationsGrouper.groupMutations(composer.get());

      List<ParallelExecutorTask> tasks = new ArrayList<>(groupedMutations.size());
      for (List<Mutation> mutations : groupedMutations) {
        tasks.add(() -> storage.mutate(mutations));
      }
      parallelExecutor.rollbackRecords(tasks, context.transactionId);
    } catch (Exception e) {
      logger.info("Rolling back records failed. Transaction ID: {}", context.transactionId, e);
      // ignore since records are recovered lazily
    }
  }

  /**
   * Writes the ABORTED state for a manager-level rollback/abort by transaction ID (the {@code
   * DistributedTransactionManager.rollback(String)} / {@code abort(String)} path), where only the
   * transaction ID is known.
   *
   * <p>This delegates to {@link Coordinator#forceAbort(String)}, which branches on the given ID:
   * for a group commit full key it uses the same two-step protocol as lazy recovery, writing the
   * parent-ID conflict marker before the full-ID ABORTED record so the abort wins against an
   * in-flight normal group commit (which writes the COMMITTED state under the parent ID); for a
   * non-group-commit ID it just writes the ABORTED record.
   *
   * @param id the transaction ID
   * @return the resulting transaction state — either {@link TransactionState#ABORTED} or, if a
   *     concurrent writer beat us, whatever state ({@link TransactionState#COMMITTED} or {@link
   *     TransactionState#ABORTED}) is already persisted
   * @throws UnknownTransactionStatusException if the final transaction status cannot be determined
   */
  public TransactionState abortStateForRollback(String id)
      throws UnknownTransactionStatusException {
    try {
      coordinator.forceAbort(id);
      return TransactionState.ABORTED;
    } catch (CoordinatorConflictException e) {
      // Resolves the final transaction state after our ABORTED putIfNotExists lost the race, for
      // the One-phase Commit I/F abort-by-id path (DistributedTransactionManager.rollback(String) /
      // abort(String)). Follows the persisted state when present. Unlike the self-abort and
      // Two-phase Commit I/F aborts, an absent row here is genuinely undeterminable: abort-by-id
      // can target a transaction that actually committed, and in One-phase Commit I/F
      // finishTransaction can remove that COMMITTED row, so an absent row may be a cleaned-up
      // COMMITTED rather than a cleaned-up ABORTED. Report an honest
      // UnknownTransactionStatusException preserving the original conflict, rather than fabricating
      // a terminal state.
      //
      // TODO: revisit this if/when the Two-phase Commit I/F is removed

      Optional<TransactionState> persisted = readCoordinatorStateAfterAbortConflict(id, e);
      if (persisted.isPresent()) {
        return persisted.get();
      }
      throw new UnknownTransactionStatusException(
          CoreError
              .CONSENSUS_COMMIT_ABORTING_STATE_FAILED_WITH_NO_MUTATION_EXCEPTION_BUT_COORDINATOR_STATUS_DOES_NOT_EXIST
              .buildMessage(e.getMessage()),
          e,
          id);
    } catch (CoordinatorException e) {
      throw new UnknownTransactionStatusException(
          CoreError.CONSENSUS_COMMIT_UNKNOWN_COORDINATOR_STATUS.buildMessage(e.getMessage()),
          e,
          id);
    }
  }

  /**
   * Sets the {@link BeforePreparationHook}. This method must be called immediately after the
   * constructor is invoked.
   *
   * @param beforePreparationHook The before-preparation hook to set.
   * @throws NullPointerException If the argument is null.
   */
  public void setBeforePreparationHook(BeforePreparationHook beforePreparationHook) {
    this.beforePreparationHook = checkNotNull(beforePreparationHook);
  }
}
