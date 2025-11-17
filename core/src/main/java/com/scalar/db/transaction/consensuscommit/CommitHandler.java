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

  private Optional<Future<Void>> invokeBeforePreparationHook(TransactionContext context)
      throws UnknownTransactionStatusException, CommitException {
    if (beforePreparationHook == null) {
      return Optional.empty();
    }

    try {
      return Optional.of(beforePreparationHook.handle(tableMetadataManager, context));
    } catch (Exception e) {
      safelyCallOnFailureBeforeCommit(context);
      abortState(context.transactionId);
      rollbackRecords(context);
      throw new CommitException(
          CoreError.CONSENSUS_COMMIT_HANDLING_BEFORE_PREPARATION_HOOK_FAILED.buildMessage(
              e.getMessage()),
          e,
          context.transactionId);
    }
  }

  private void waitBeforePreparationHookFuture(
      TransactionContext context, @Nullable Future<Void> beforePreparationHookFuture)
      throws UnknownTransactionStatusException, CommitException {
    if (beforePreparationHookFuture == null) {
      return;
    }

    try {
      beforePreparationHookFuture.get();
    } catch (Exception e) {
      safelyCallOnFailureBeforeCommit(context);
      abortState(context.transactionId);
      rollbackRecords(context);
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

    Optional<Future<Void>> beforePreparationHookFuture = invokeBeforePreparationHook(context);

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
        abortState(context.transactionId);
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

    if (context.snapshot.hasReads()) {
      try {
        validateRecords(context);
      } catch (ValidationException e) {
        safelyCallOnFailureBeforeCommit(context);

        // If the transaction has no writes and deletes, we don't need to abort-state and
        // rollback-records since there are no changes to be made.
        if (hasWritesOrDeletesInSnapshot || !coordinatorWriteOmissionOnReadOnlyEnabled) {
          abortState(context.transactionId);
        }
        if (hasWritesOrDeletesInSnapshot) {
          rollbackRecords(context);
        }

        if (e instanceof ValidationConflictException) {
          throw new CommitConflictException(e.getMessage(), e, e.getTransactionId().orElse(null));
        }
        throw new CommitException(e.getMessage(), e, e.getTransactionId().orElse(null));
      } catch (Exception e) {
        safelyCallOnFailureBeforeCommit(context);
        throw e;
      }
    }

    waitBeforePreparationHookFuture(context, beforePreparationHookFuture.orElse(null));

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

    // If validation is required (in SERIALIZABLE isolation), we cannot one-phase commit the
    // transaction
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
              CoreError.CONSENSUS_COMMIT_CONFLICT_OCCURRED_WHEN_COMMITTING_STATE.buildMessage(),
              cause,
              context.transactionId);
        }
      } else {
        throw new UnknownTransactionStatusException(
            CoreError
                .CONSENSUS_COMMIT_COMMITTING_STATE_FAILED_WITH_NO_MUTATION_EXCEPTION_BUT_COORDINATOR_STATUS_DOES_NOT_EXIST
                .buildMessage(),
            cause,
            context.transactionId);
      }
    } catch (CoordinatorException ex) {
      throw new UnknownTransactionStatusException(
          CoreError.CONSENSUS_COMMIT_CANNOT_GET_STATE.buildMessage(), ex, context.transactionId);
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
          CoreError.CONSENSUS_COMMIT_PREPARING_RECORD_EXISTS.buildMessage(),
          e,
          context.transactionId);
    } catch (RetriableExecutionException e) {
      throw new CommitConflictException(
          CoreError.CONSENSUS_COMMIT_CONFLICT_OCCURRED_WHEN_COMMITTING_RECORDS.buildMessage(),
          e,
          context.transactionId);
    } catch (ExecutionException e) {
      throw new UnknownTransactionStatusException(
          CoreError.CONSENSUS_COMMIT_COMMITTING_RECORDS_FAILED.buildMessage(e.getMessage()),
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
          CoreError.CONSENSUS_COMMIT_PREPARING_RECORD_EXISTS.buildMessage(),
          e,
          context.transactionId);
    } catch (RetriableExecutionException e) {
      throw new PreparationConflictException(
          CoreError.CONSENSUS_COMMIT_CONFLICT_OCCURRED_WHEN_PREPARING_RECORDS.buildMessage(),
          e,
          context.transactionId);
    } catch (ExecutionException e) {
      throw new PreparationException(
          CoreError.CONSENSUS_COMMIT_PREPARING_RECORDS_FAILED.buildMessage(),
          e,
          context.transactionId);
    }
  }

  public void validateRecords(TransactionContext context) throws ValidationException {
    try {
      // validation is executed when SERIALIZABLE is chosen.
      context.snapshot.toSerializable(storage);
    } catch (ExecutionException e) {
      throw new ValidationException(
          CoreError.CONSENSUS_COMMIT_VALIDATION_FAILED.buildMessage(), e, context.transactionId);
    }
  }

  public void commitState(TransactionContext context)
      throws CommitConflictException, UnknownTransactionStatusException {
    String id = context.transactionId;
    try {
      Coordinator.State state = new Coordinator.State(id, TransactionState.COMMITTED);
      coordinator.putState(state);
      logger.debug(
          "Transaction {} is committed successfully at {}", id, System.currentTimeMillis());
    } catch (CoordinatorConflictException e) {
      handleCommitConflict(context, e);
    } catch (CoordinatorException e) {
      throw new UnknownTransactionStatusException(
          CoreError.CONSENSUS_COMMIT_UNKNOWN_COORDINATOR_STATUS.buildMessage(), e, id);
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

  public TransactionState abortState(String id) throws UnknownTransactionStatusException {
    try {
      Coordinator.State state = new Coordinator.State(id, TransactionState.ABORTED);
      coordinator.putState(state);
      return TransactionState.ABORTED;
    } catch (CoordinatorConflictException e) {
      try {
        Optional<Coordinator.State> state = coordinator.getState(id);
        if (state.isPresent()) {
          // successfully COMMITTED or ABORTED
          return state.get().getState();
        }
        throw new UnknownTransactionStatusException(
            CoreError
                .CONSENSUS_COMMIT_ABORTING_STATE_FAILED_WITH_NO_MUTATION_EXCEPTION_BUT_COORDINATOR_STATUS_DOES_NOT_EXIST
                .buildMessage(),
            e,
            id);
      } catch (CoordinatorException e1) {
        throw new UnknownTransactionStatusException(
            CoreError.CONSENSUS_COMMIT_CANNOT_GET_STATE.buildMessage(), e1, id);
      }
    } catch (CoordinatorException e) {
      throw new UnknownTransactionStatusException(
          CoreError.CONSENSUS_COMMIT_UNKNOWN_COORDINATOR_STATUS.buildMessage(), e, id);
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
