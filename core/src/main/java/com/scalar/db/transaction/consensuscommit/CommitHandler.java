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
 * <p>Public methods like {@code commitState}, {@code abortState}, {@code prepareRecords}, {@code
 * commitRecords}, {@code rollbackRecords}, {@code commitStateWithoutWriteSet}, and {@code
 * abortStateWithoutWriteSet} are exposed on this class as thin pass-throughs to the specialized
 * handlers so direct callers (notably {@link TwoPhaseConsensusCommit}) can drive individual commit
 * phases without depending on the handlers directly.
 */
@ThreadSafe
public class CommitHandler {
  private static final Logger logger = LoggerFactory.getLogger(CommitHandler.class);

  private final CoordinatorCommitHandler coordinatorCommitHandler;
  private final ParticipantCommitHandler participantCommitHandler;
  protected final boolean coordinatorWriteOmissionOnReadOnlyEnabled;

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
    this.coordinatorCommitHandler =
        new CoordinatorCommitHandler(
            coordinator,
            new WriteSetEncoder(tableMetadataManager),
            coordinatorWriteSetLoggingEnabled);
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
      CoordinatorCommitHandler coordinatorCommitHandler,
      ParticipantCommitHandler participantCommitHandler) {
    this.coordinatorWriteOmissionOnReadOnlyEnabled = coordinatorWriteOmissionOnReadOnlyEnabled;
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
      try {
        commitState(context);
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
    }
    if (hasWritesOrDeletesInSnapshot) {
      commitRecords(context);
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

  public void prepareRecords(TransactionContext context) throws PreparationException {
    participantCommitHandler.prepareRecords(context);
  }

  public void validateRecords(TransactionContext context) throws ValidationException {
    participantCommitHandler.validateRecords(context);
  }

  public void commitRecords(TransactionContext context) {
    participantCommitHandler.commitRecords(context);
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

  public void commitState(TransactionContext context)
      throws CommitConflictException, UnknownTransactionStatusException {
    coordinatorCommitHandler.commitState(context);
  }

  public void commitStateWithoutWriteSet(TransactionContext context)
      throws CommitConflictException, UnknownTransactionStatusException {
    coordinatorCommitHandler.commitStateWithoutWriteSet(context);
  }

  public TransactionState abortState(TransactionContext context)
      throws UnknownTransactionStatusException {
    return coordinatorCommitHandler.abortState(context);
  }

  public TransactionState abortStateWithoutWriteSet(String id)
      throws UnknownTransactionStatusException {
    return coordinatorCommitHandler.abortStateWithoutWriteSet(id);
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
