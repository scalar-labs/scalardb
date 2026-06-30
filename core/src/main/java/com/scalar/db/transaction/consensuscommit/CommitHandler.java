package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.errorprone.annotations.concurrent.LazyInit;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.CoreError;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.exception.transaction.ValidationException;
import com.scalar.db.transaction.consensuscommit.proto.v1.WriteSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Optional;
import java.util.concurrent.Future;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orchestrates the Consensus Commit protocol. Holds two specialized handlers and delegates to them:
 *
 * <ul>
 *   <li>{@link CoordinatorCommitHandler} owns Coordinator-table state writes (COMMITTED / ABORTED).
 *       The group-commit-enabled variant ({@link CoordinatorCommitHandlerWithGroupCommit}) routes
 *       commitState through the group-commit emitter.
 *   <li>{@link ParticipantCommitHandler} owns participant-side data-record writes (prepare,
 *       validate, commit, rollback, one-phase commit).
 * </ul>
 *
 * <p>The orchestrator owns the {@link #commit(TransactionContext)} flow (prepare → validate →
 * commit-state → commit-records, plus the abort/rollback path on failure) and the {@link
 * BeforePreparationHook} integration. {@link #forceAbortState(String)} is exposed on this class as
 * a convenience pass-through to the Coordinator-side handler so the manager-level rollback callers
 * (ConsensusCommit / TwoPhaseConsensusCommit / ConsensusCommitManager) can depend on the
 * orchestrator alone.
 *
 * <p>Several public methods are exposed on this class so direct callers (notably {@link
 * TwoPhaseConsensusCommit}) can drive individual commit phases without depending on the specialized
 * handlers directly. {@code prepareRecords}, {@code commitRecords}, {@code rollbackRecords}, {@code
 * commitStateWithoutWriteSet}, and {@code abortStateWithoutWriteSet} are thin pass-throughs. {@code
 * commitState} and {@code abortState} are not: they encode the transaction's write set via the
 * orchestrator-owned {@link WriteSetEncoder} before delegating to the Coordinator-side handler.
 */
@ThreadSafe
public class CommitHandler {
  private static final Logger logger = LoggerFactory.getLogger(CommitHandler.class);

  private final CoordinatorCommitHandler coordinatorCommitHandler;
  private final ParticipantCommitHandler participantCommitHandler;
  protected final WriteSetEncoder writeSetEncoder;
  protected final boolean coordinatorWriteOmissionOnReadOnlyEnabled;
  protected final boolean coordinatorWriteSetLoggingEnabled;

  @LazyInit @Nullable private BeforePreparationHook beforePreparationHook;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public CommitHandler(
      DistributedStorage storage,
      Coordinator coordinator,
      TransactionTableMetadataManager tableMetadataManager,
      ParallelExecutor parallelExecutor,
      MutationsGrouper mutationsGrouper,
      boolean coordinatorWriteOmissionOnReadOnlyEnabled,
      boolean coordinatorWriteSetLoggingEnabled,
      boolean onePhaseCommitEnabled) {
    this.coordinatorWriteOmissionOnReadOnlyEnabled = coordinatorWriteOmissionOnReadOnlyEnabled;
    this.coordinatorWriteSetLoggingEnabled = coordinatorWriteSetLoggingEnabled;
    this.writeSetEncoder = new WriteSetEncoder(tableMetadataManager);
    this.coordinatorCommitHandler = new CoordinatorCommitHandler(coordinator);
    this.participantCommitHandler =
        new ParticipantCommitHandler(
            storage,
            tableMetadataManager,
            parallelExecutor,
            mutationsGrouper,
            onePhaseCommitEnabled);
  }

  // Constructor for subclasses (CommitHandlerWithGroupCommit) that need to inject a
  // group-commit-aware CoordinatorCommitHandler. The two handlers are still constructed by the
  // subclass to keep the dependency graph explicit. `onePhaseCommitEnabled` is baked into the
  // pre-built ParticipantCommitHandler, so it is not threaded through here.
  @SuppressFBWarnings("EI_EXPOSE_REP2")
  protected CommitHandler(
      boolean coordinatorWriteOmissionOnReadOnlyEnabled,
      boolean coordinatorWriteSetLoggingEnabled,
      WriteSetEncoder writeSetEncoder,
      CoordinatorCommitHandler coordinatorCommitHandler,
      ParticipantCommitHandler participantCommitHandler) {
    this.coordinatorWriteOmissionOnReadOnlyEnabled = coordinatorWriteOmissionOnReadOnlyEnabled;
    this.coordinatorWriteSetLoggingEnabled = coordinatorWriteSetLoggingEnabled;
    this.writeSetEncoder = checkNotNull(writeSetEncoder);
    this.coordinatorCommitHandler = checkNotNull(coordinatorCommitHandler);
    this.participantCommitHandler = checkNotNull(participantCommitHandler);
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

  private Optional<Future<Void>> invokeBeforePreparationHook(
      TransactionContext context, boolean hasWritesOrDeletesInSnapshot)
      throws UnknownTransactionStatusException, CommitException {
    if (beforePreparationHook == null) {
      return Optional.empty();
    }

    try {
      return Optional.of(beforePreparationHook.handle(context));
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

    if (canOnePhaseCommit(context)) {
      try {
        onePhaseCommitRecords(context);
        return;
      } catch (Exception e) {
        safelyCallOnFailureBeforeCommit(context);
        throw e;
      }
    }

    // The one-phase commit fast path does not invoke the before-preparation hook.
    Optional<Future<Void>> beforePreparationHookFuture =
        invokeBeforePreparationHook(context, hasWritesOrDeletesInSnapshot);

    if (hasWritesOrDeletesInSnapshot) {
      try {
        prepareRecords(context, System.currentTimeMillis());
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
      long committedAt;
      try {
        // commitState writes the COMMITTED Coordinator state row and returns the committedAt it
        // stamped (in group commit, the emit-time value shared across the batch). The data records
        // are then committed with the same value so the row and the records share one timestamp.
        committedAt = commitState(context);
      } catch (CommitConflictException e) {
        // The COMMITTED-state write lost a putState race that resolved to ABORTED (or absent): the
        // transaction is aborted. The Coordinator-side handler only reports the conflict; the
        // orchestrator owns the records, so roll back the prepared records here before surfacing
        // it.
        if (hasWritesOrDeletesInSnapshot) {
          rollbackRecords(context);
        }
        throw e;
      }
      if (hasWritesOrDeletesInSnapshot) {
        commitRecords(context, committedAt);
      }
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

  // ---------- Pass-through methods that delegate to the specialized handlers. ----------
  // Exposed on this class so direct callers (TwoPhaseConsensusCommit / ConsensusCommit /
  // ConsensusCommitManager / RecoveryHandler / tests) can drive individual commit phases through
  // the orchestrator's primary dependency.
  //
  // TODO: revisit this if/when the Two-phase Commit I/F is removed.

  public void prepareRecords(TransactionContext context, long preparedAt)
      throws PreparationException {
    participantCommitHandler.prepareRecords(context, preparedAt);
  }

  public void validateRecords(TransactionContext context) throws ValidationException {
    participantCommitHandler.validateRecords(context);
  }

  public void commitRecords(TransactionContext context, long committedAt) {
    participantCommitHandler.commitRecords(context, committedAt);
  }

  public void rollbackRecords(TransactionContext context) {
    participantCommitHandler.rollbackRecords(context);
  }

  /**
   * Checks whether the transaction is eligible for a one-phase commit. Subclasses may override to
   * insert behavior (the group-commit variant cancels its slot reservation on the exception path).
   */
  boolean canOnePhaseCommit(TransactionContext context) throws CommitException {
    return participantCommitHandler.canOnePhaseCommit(context);
  }

  /**
   * Performs the one-phase commit storage mutation. Called only when {@link
   * #canOnePhaseCommit(TransactionContext)} returns {@code true}. Subclasses may override to insert
   * behavior (the group-commit variant cancels its slot reservation before / after).
   */
  void onePhaseCommitRecords(TransactionContext context)
      throws CommitConflictException, UnknownTransactionStatusException {
    participantCommitHandler.onePhaseCommitRecords(context);
  }

  public long commitState(TransactionContext context)
      throws CommitConflictException, UnknownTransactionStatusException {
    return coordinatorCommitHandler.commitState(
        context.transactionId, encodeWriteSetIfLoggingEnabled(context));
  }

  // The tx_write_set column is part of the Coordinator schema only when the opt-in
  // `coordinator.write_set_logging.enabled` config is on. When it is off, skip encoding entirely so
  // no WriteSet is persisted (the column is not part of the schema in that case).
  @Nullable
  private WriteSet encodeWriteSetIfLoggingEnabled(TransactionContext context) {
    return coordinatorWriteSetLoggingEnabled
        ? writeSetEncoder.encodeSingleGroupWriteSet(context, false)
        : null;
  }

  // 2PC-only. Delegates to commitState(id, null) / abortState(id, null), which the group-commit
  // coordinator handler overrides to route through the group committer. That null never reaches the
  // group committer here because 2PC forbids group commit
  // (TwoPhaseConsensusCommitManager#throwIfGroupCommitIsEnabled) and never builds a
  // CommitHandlerWithGroupCommit.
  //
  // TODO: revisit this if/when the Two-phase Commit I/F is removed.
  public long commitStateWithoutWriteSet(TransactionContext context)
      throws CommitConflictException, UnknownTransactionStatusException {
    return coordinatorCommitHandler.commitState(context.transactionId, null);
  }

  public TransactionState abortState(TransactionContext context)
      throws UnknownTransactionStatusException {
    return coordinatorCommitHandler.abortState(
        context.transactionId, encodeWriteSetIfLoggingEnabled(context));
  }

  // 2PC-only; see the null-write-set note on commitStateWithoutWriteSet above.
  //
  // TODO: revisit this if/when the Two-phase Commit I/F is removed.
  public TransactionState abortStateWithoutWriteSet(String id)
      throws UnknownTransactionStatusException {
    return coordinatorCommitHandler.abortState(id, null);
  }

  public TransactionState forceAbortState(String id) throws UnknownTransactionStatusException {
    return coordinatorCommitHandler.forceAbortState(id);
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
