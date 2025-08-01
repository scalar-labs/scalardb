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
import java.util.List;
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

  @LazyInit @Nullable private BeforePreparationSnapshotHook beforePreparationSnapshotHook;

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
   * @param snapshot the failed snapshot.
   */
  protected void onFailureBeforeCommit(Snapshot snapshot) {}

  private void safelyCallOnFailureBeforeCommit(Snapshot snapshot) {
    try {
      onFailureBeforeCommit(snapshot);
    } catch (Exception e) {
      logger.warn("Failed to call the callback. Transaction ID: {}", snapshot.getId(), e);
    }
  }

  private Optional<Future<Void>> invokeBeforePreparationSnapshotHook(Snapshot snapshot)
      throws UnknownTransactionStatusException, CommitException {
    if (beforePreparationSnapshotHook == null) {
      return Optional.empty();
    }

    try {
      return Optional.of(
          beforePreparationSnapshotHook.handle(tableMetadataManager, snapshot.getReadWriteSets()));
    } catch (Exception e) {
      safelyCallOnFailureBeforeCommit(snapshot);
      abortState(snapshot.getId());
      rollbackRecords(snapshot);
      throw new CommitException(
          CoreError.CONSENSUS_COMMIT_HANDLING_BEFORE_PREPARATION_SNAPSHOT_HOOK_FAILED.buildMessage(
              e.getMessage()),
          e,
          snapshot.getId());
    }
  }

  private void waitBeforePreparationSnapshotHookFuture(
      Snapshot snapshot, @Nullable Future<Void> snapshotHookFuture)
      throws UnknownTransactionStatusException, CommitException {
    if (snapshotHookFuture == null) {
      return;
    }

    try {
      snapshotHookFuture.get();
    } catch (Exception e) {
      safelyCallOnFailureBeforeCommit(snapshot);
      abortState(snapshot.getId());
      rollbackRecords(snapshot);
      throw new CommitException(
          CoreError.CONSENSUS_COMMIT_HANDLING_BEFORE_PREPARATION_SNAPSHOT_HOOK_FAILED.buildMessage(
              e.getMessage()),
          e,
          snapshot.getId());
    }
  }

  public void commit(Snapshot snapshot, boolean readOnly)
      throws CommitException, UnknownTransactionStatusException {
    boolean hasWritesOrDeletesInSnapshot = !readOnly && snapshot.hasWritesOrDeletes();

    Optional<Future<Void>> snapshotHookFuture = invokeBeforePreparationSnapshotHook(snapshot);

    if (canOnePhaseCommit(snapshot)) {
      try {
        onePhaseCommitRecords(snapshot);
        return;
      } catch (Exception e) {
        safelyCallOnFailureBeforeCommit(snapshot);
        throw e;
      }
    }

    if (hasWritesOrDeletesInSnapshot) {
      try {
        prepareRecords(snapshot);
      } catch (PreparationException e) {
        safelyCallOnFailureBeforeCommit(snapshot);
        abortState(snapshot.getId());
        rollbackRecords(snapshot);
        if (e instanceof PreparationConflictException) {
          throw new CommitConflictException(e.getMessage(), e, e.getTransactionId().orElse(null));
        }
        throw new CommitException(e.getMessage(), e, e.getTransactionId().orElse(null));
      } catch (Exception e) {
        safelyCallOnFailureBeforeCommit(snapshot);
        throw e;
      }
    }

    if (snapshot.hasReads()) {
      try {
        validateRecords(snapshot);
      } catch (ValidationException e) {
        safelyCallOnFailureBeforeCommit(snapshot);

        // If the transaction has no writes and deletes, we don't need to abort-state and
        // rollback-records since there are no changes to be made.
        if (hasWritesOrDeletesInSnapshot || !coordinatorWriteOmissionOnReadOnlyEnabled) {
          abortState(snapshot.getId());
        }
        if (hasWritesOrDeletesInSnapshot) {
          rollbackRecords(snapshot);
        }

        if (e instanceof ValidationConflictException) {
          throw new CommitConflictException(e.getMessage(), e, e.getTransactionId().orElse(null));
        }
        throw new CommitException(e.getMessage(), e, e.getTransactionId().orElse(null));
      } catch (Exception e) {
        safelyCallOnFailureBeforeCommit(snapshot);
        throw e;
      }
    }

    waitBeforePreparationSnapshotHookFuture(snapshot, snapshotHookFuture.orElse(null));

    if (hasWritesOrDeletesInSnapshot || !coordinatorWriteOmissionOnReadOnlyEnabled) {
      commitState(snapshot);
    }
    if (hasWritesOrDeletesInSnapshot) {
      commitRecords(snapshot);
    }
  }

  @VisibleForTesting
  boolean canOnePhaseCommit(Snapshot snapshot) throws CommitException {
    if (!onePhaseCommitEnabled) {
      return false;
    }

    // If validation is required (in SERIALIZABLE isolation), we cannot one-phase commit the
    // transaction
    if (snapshot.isValidationRequired()) {
      return false;
    }

    // If the snapshot has no write and deletes, we do not one-phase commit the transaction
    if (!snapshot.hasWritesOrDeletes()) {
      return false;
    }

    List<Delete> deletesInDeleteSet = snapshot.getDeletesInDeleteSet();

    // If a record corresponding to a delete in the delete set does not exist in the storage,　we
    // cannot one-phase commit the transaction. This is because the storage does not support
    // delete-if-not-exists semantics, so we cannot detect conflicts with other transactions.
    for (Delete delete : deletesInDeleteSet) {
      Optional<TransactionResult> result = snapshot.getFromReadSet(new Snapshot.Key(delete));

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
          Stream.concat(snapshot.getPutsInWriteSet().stream(), deletesInDeleteSet.stream())
              .collect(Collectors.toList()));
    } catch (ExecutionException e) {
      throw new CommitException(
          CoreError.CONSENSUS_COMMIT_COMMITTING_RECORDS_FAILED.buildMessage(e.getMessage()),
          e,
          snapshot.getId());
    }
  }

  protected void handleCommitConflict(Snapshot snapshot, Exception cause)
      throws CommitConflictException, UnknownTransactionStatusException {
    try {
      Optional<State> s = coordinator.getState(snapshot.getId());
      if (s.isPresent()) {
        TransactionState state = s.get().getState();
        if (state.equals(TransactionState.ABORTED)) {
          rollbackRecords(snapshot);
          throw new CommitConflictException(
              CoreError.CONSENSUS_COMMIT_CONFLICT_OCCURRED_WHEN_COMMITTING_STATE.buildMessage(
                  cause.getMessage()),
              cause,
              snapshot.getId());
        }
      } else {
        throw new UnknownTransactionStatusException(
            CoreError
                .CONSENSUS_COMMIT_COMMITTING_STATE_FAILED_WITH_NO_MUTATION_EXCEPTION_BUT_COORDINATOR_STATUS_DOES_NOT_EXIST
                .buildMessage(cause.getMessage()),
            cause,
            snapshot.getId());
      }
    } catch (CoordinatorException ex) {
      throw new UnknownTransactionStatusException(
          CoreError.CONSENSUS_COMMIT_CANNOT_GET_COORDINATOR_STATUS.buildMessage(ex.getMessage()),
          ex,
          snapshot.getId());
    }
  }

  @VisibleForTesting
  void onePhaseCommitRecords(Snapshot snapshot)
      throws CommitConflictException, UnknownTransactionStatusException {
    try {
      OnePhaseCommitMutationComposer composer =
          new OnePhaseCommitMutationComposer(snapshot.getId(), tableMetadataManager);
      snapshot.to(composer);

      // One-phase commit does not require grouping mutations and using the parallel executor since
      // it is always executed in a single mutate API call.
      storage.mutate(composer.get());
    } catch (NoMutationException e) {
      throw new CommitConflictException(
          CoreError.CONSENSUS_COMMIT_PREPARING_RECORD_EXISTS.buildMessage(e.getMessage()),
          e,
          snapshot.getId());
    } catch (RetriableExecutionException e) {
      throw new CommitConflictException(
          CoreError.CONSENSUS_COMMIT_CONFLICT_OCCURRED_WHEN_COMMITTING_RECORDS.buildMessage(
              e.getMessage()),
          e,
          snapshot.getId());
    } catch (ExecutionException e) {
      throw new UnknownTransactionStatusException(
          CoreError.CONSENSUS_COMMIT_ONE_PHASE_COMMITTING_RECORDS_FAILED.buildMessage(
              e.getMessage()),
          e,
          snapshot.getId());
    }
  }

  public void prepareRecords(Snapshot snapshot) throws PreparationException {
    try {
      PrepareMutationComposer composer =
          new PrepareMutationComposer(snapshot.getId(), tableMetadataManager);
      snapshot.to(composer);
      List<List<Mutation>> groupedMutations = mutationsGrouper.groupMutations(composer.get());

      List<ParallelExecutorTask> tasks = new ArrayList<>(groupedMutations.size());
      for (List<Mutation> mutations : groupedMutations) {
        tasks.add(() -> storage.mutate(mutations));
      }
      parallelExecutor.prepareRecords(tasks, snapshot.getId());
    } catch (NoMutationException e) {
      throw new PreparationConflictException(
          CoreError.CONSENSUS_COMMIT_PREPARING_RECORD_EXISTS.buildMessage(e.getMessage()),
          e,
          snapshot.getId());
    } catch (RetriableExecutionException e) {
      throw new PreparationConflictException(
          CoreError.CONSENSUS_COMMIT_CONFLICT_OCCURRED_WHEN_PREPARING_RECORDS.buildMessage(
              e.getMessage()),
          e,
          snapshot.getId());
    } catch (ExecutionException e) {
      throw new PreparationException(
          CoreError.CONSENSUS_COMMIT_PREPARING_RECORDS_FAILED.buildMessage(e.getMessage()),
          e,
          snapshot.getId());
    }
  }

  public void validateRecords(Snapshot snapshot) throws ValidationException {
    try {
      // validation is executed when SERIALIZABLE is chosen.
      snapshot.toSerializable(storage);
    } catch (ExecutionException e) {
      throw new ValidationException(
          CoreError.CONSENSUS_COMMIT_VALIDATION_FAILED.buildMessage(e.getMessage()),
          e,
          snapshot.getId());
    }
  }

  public void commitState(Snapshot snapshot)
      throws CommitConflictException, UnknownTransactionStatusException {
    String id = snapshot.getId();
    try {
      Coordinator.State state = new Coordinator.State(id, TransactionState.COMMITTED);
      coordinator.putState(state);
      logger.debug(
          "Transaction {} is committed successfully at {}", id, System.currentTimeMillis());
    } catch (CoordinatorConflictException e) {
      handleCommitConflict(snapshot, e);
    } catch (CoordinatorException e) {
      throw new UnknownTransactionStatusException(
          CoreError.CONSENSUS_COMMIT_UNKNOWN_COORDINATOR_STATUS.buildMessage(e.getMessage()),
          e,
          id);
    }
  }

  public void commitRecords(Snapshot snapshot) {
    try {
      CommitMutationComposer composer =
          new CommitMutationComposer(snapshot.getId(), tableMetadataManager);
      snapshot.to(composer);
      List<List<Mutation>> groupedMutations = mutationsGrouper.groupMutations(composer.get());

      List<ParallelExecutorTask> tasks = new ArrayList<>(groupedMutations.size());
      for (List<Mutation> mutations : groupedMutations) {
        tasks.add(() -> storage.mutate(mutations));
      }
      parallelExecutor.commitRecords(tasks, snapshot.getId());
    } catch (Exception e) {
      logger.info("Committing records failed. Transaction ID: {}", snapshot.getId(), e);
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
                .buildMessage(e.getMessage()),
            e,
            id);
      } catch (CoordinatorException e1) {
        throw new UnknownTransactionStatusException(
            CoreError.CONSENSUS_COMMIT_CANNOT_GET_COORDINATOR_STATUS.buildMessage(e1.getMessage()),
            e1,
            id);
      }
    } catch (CoordinatorException e) {
      throw new UnknownTransactionStatusException(
          CoreError.CONSENSUS_COMMIT_UNKNOWN_COORDINATOR_STATUS.buildMessage(e.getMessage()),
          e,
          id);
    }
  }

  public void rollbackRecords(Snapshot snapshot) {
    logger.debug("Rollback from snapshot for {}", snapshot.getId());
    try {
      RollbackMutationComposer composer =
          new RollbackMutationComposer(snapshot.getId(), storage, tableMetadataManager);
      snapshot.to(composer);
      List<List<Mutation>> groupedMutations = mutationsGrouper.groupMutations(composer.get());

      List<ParallelExecutorTask> tasks = new ArrayList<>(groupedMutations.size());
      for (List<Mutation> mutations : groupedMutations) {
        tasks.add(() -> storage.mutate(mutations));
      }
      parallelExecutor.rollbackRecords(tasks, snapshot.getId());
    } catch (Exception e) {
      logger.info("Rolling back records failed. Transaction ID: {}", snapshot.getId(), e);
      // ignore since records are recovered lazily
    }
  }

  /**
   * Sets the {@link BeforePreparationSnapshotHook}. This method must be called immediately after
   * the constructor is invoked.
   *
   * @param beforePreparationSnapshotHook The snapshot hook to set.
   * @throws NullPointerException If the argument is null.
   */
  public void setBeforePreparationSnapshotHook(
      BeforePreparationSnapshotHook beforePreparationSnapshotHook) {
    this.beforePreparationSnapshotHook = checkNotNull(beforePreparationSnapshotHook);
  }
}
