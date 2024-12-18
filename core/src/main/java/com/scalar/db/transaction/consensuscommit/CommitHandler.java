package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.concurrent.LazyInit;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.error.CoreError;
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

  @LazyInit @Nullable private BeforePreparationSnapshotHook beforePreparationSnapshotHook;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public CommitHandler(
      DistributedStorage storage,
      Coordinator coordinator,
      TransactionTableMetadataManager tableMetadataManager,
      ParallelExecutor parallelExecutor) {
    this.storage = checkNotNull(storage);
    this.coordinator = checkNotNull(coordinator);
    this.tableMetadataManager = checkNotNull(tableMetadataManager);
    this.parallelExecutor = checkNotNull(parallelExecutor);
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
          CoreError.HANDLING_BEFORE_PREPARATION_SNAPSHOT_HOOK_FAILED.buildMessage(e.getMessage()),
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
          CoreError.HANDLING_BEFORE_PREPARATION_SNAPSHOT_HOOK_FAILED.buildMessage(e.getMessage()),
          e,
          snapshot.getId());
    }
  }

  public void commit(Snapshot snapshot) throws CommitException, UnknownTransactionStatusException {
    Optional<Future<Void>> snapshotHookFuture = invokeBeforePreparationSnapshotHook(snapshot);
    try {
      prepare(snapshot);
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

    try {
      validate(snapshot);
    } catch (ValidationException e) {
      safelyCallOnFailureBeforeCommit(snapshot);
      abortState(snapshot.getId());
      rollbackRecords(snapshot);
      if (e instanceof ValidationConflictException) {
        throw new CommitConflictException(e.getMessage(), e, e.getTransactionId().orElse(null));
      }
      throw new CommitException(e.getMessage(), e, e.getTransactionId().orElse(null));
    } catch (Exception e) {
      safelyCallOnFailureBeforeCommit(snapshot);
      throw e;
    }

    waitBeforePreparationSnapshotHookFuture(snapshot, snapshotHookFuture.orElse(null));

    commitState(snapshot);
    commitRecords(snapshot);
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
              CoreError.CONSENSUS_COMMIT_CONFLICT_OCCURRED_WHEN_COMMITTING_STATE.buildMessage(),
              cause,
              snapshot.getId());
        }
      } else {
        throw new UnknownTransactionStatusException(
            CoreError
                .CONSENSUS_COMMIT_COMMITTING_STATE_FAILED_WITH_NO_MUTATION_EXCEPTION_BUT_COORDINATOR_STATUS_DOES_NOT_EXIST
                .buildMessage(),
            cause,
            snapshot.getId());
      }
    } catch (CoordinatorException ex) {
      throw new UnknownTransactionStatusException(
          CoreError.CONSENSUS_COMMIT_CANNOT_GET_STATE.buildMessage(), ex, snapshot.getId());
    }
  }

  public void prepare(Snapshot snapshot) throws PreparationException {
    try {
      prepareRecords(snapshot);
    } catch (NoMutationException e) {
      throw new PreparationConflictException(
          CoreError.CONSENSUS_COMMIT_PREPARING_RECORD_EXISTS.buildMessage(), e, snapshot.getId());
    } catch (RetriableExecutionException e) {
      throw new PreparationConflictException(
          CoreError.CONSENSUS_COMMIT_CONFLICT_OCCURRED_WHEN_PREPARING_RECORDS.buildMessage(),
          e,
          snapshot.getId());
    } catch (ExecutionException e) {
      throw new PreparationException(
          CoreError.CONSENSUS_COMMIT_PREPARING_RECORDS_FAILED.buildMessage(), e, snapshot.getId());
    }
  }

  private void prepareRecords(Snapshot snapshot)
      throws ExecutionException, PreparationConflictException {
    PrepareMutationComposer composer =
        new PrepareMutationComposer(snapshot.getId(), tableMetadataManager);
    snapshot.to(composer);
    PartitionedMutations mutations = new PartitionedMutations(composer.get());

    ImmutableList<PartitionedMutations.Key> orderedKeys = mutations.getOrderedKeys();
    List<ParallelExecutorTask> tasks = new ArrayList<>(orderedKeys.size());
    for (PartitionedMutations.Key key : orderedKeys) {
      tasks.add(() -> storage.mutate(mutations.get(key)));
    }
    parallelExecutor.prepare(tasks, snapshot.getId());
  }

  public void validate(Snapshot snapshot) throws ValidationException {
    try {
      // validation is executed when SERIALIZABLE with EXTRA_READ strategy is chosen.
      snapshot.toSerializableWithExtraRead(storage);
    } catch (ExecutionException e) {
      throw new ValidationException(
          CoreError.CONSENSUS_COMMIT_VALIDATION_FAILED.buildMessage(), e, snapshot.getId());
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
          CoreError.CONSENSUS_COMMIT_UNKNOWN_COORDINATOR_STATUS.buildMessage(), e, id);
    }
  }

  public void commitRecords(Snapshot snapshot) {
    try {
      CommitMutationComposer composer =
          new CommitMutationComposer(snapshot.getId(), tableMetadataManager);
      snapshot.to(composer);
      PartitionedMutations mutations = new PartitionedMutations(composer.get());

      ImmutableList<PartitionedMutations.Key> orderedKeys = mutations.getOrderedKeys();
      List<ParallelExecutorTask> tasks = new ArrayList<>(orderedKeys.size());
      for (PartitionedMutations.Key key : orderedKeys) {
        tasks.add(() -> storage.mutate(mutations.get(key)));
      }
      parallelExecutor.commitRecords(tasks, snapshot.getId());
    } catch (Exception e) {
      logger.warn("Committing records failed. Transaction ID: {}", snapshot.getId(), e);
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

  public void rollbackRecords(Snapshot snapshot) {
    logger.debug("Rollback from snapshot for {}", snapshot.getId());
    try {
      RollbackMutationComposer composer =
          new RollbackMutationComposer(snapshot.getId(), storage, tableMetadataManager);
      snapshot.to(composer);
      PartitionedMutations mutations = new PartitionedMutations(composer.get());

      ImmutableList<PartitionedMutations.Key> orderedKeys = mutations.getOrderedKeys();
      List<ParallelExecutorTask> tasks = new ArrayList<>(orderedKeys.size());
      for (PartitionedMutations.Key key : orderedKeys) {
        tasks.add(() -> storage.mutate(mutations.get(key)));
      }
      parallelExecutor.rollbackRecords(tasks, snapshot.getId());
    } catch (Exception e) {
      logger.warn("Rolling back records failed. Transaction ID: {}", snapshot.getId(), e);
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
