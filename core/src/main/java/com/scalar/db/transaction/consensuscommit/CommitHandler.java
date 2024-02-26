package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableList;
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
import com.scalar.db.util.groupcommit.GroupCommitAlreadyCompletedException;
import com.scalar.db.util.groupcommit.GroupCommitException;
import com.scalar.db.util.groupcommit.GroupCommitTargetNotFoundException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class CommitHandler {
  private static final Logger logger = LoggerFactory.getLogger(CommitHandler.class);
  private final DistributedStorage storage;
  private final Coordinator coordinator;
  private final TransactionTableMetadataManager tableMetadataManager;
  private final ParallelExecutor parallelExecutor;
  @Nullable private final CoordinatorGroupCommitter groupCommitter;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public CommitHandler(
      DistributedStorage storage,
      Coordinator coordinator,
      TransactionTableMetadataManager tableMetadataManager,
      ParallelExecutor parallelExecutor) {
    this(storage, coordinator, tableMetadataManager, parallelExecutor, null);
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public CommitHandler(
      DistributedStorage storage,
      Coordinator coordinator,
      TransactionTableMetadataManager tableMetadataManager,
      ParallelExecutor parallelExecutor,
      @Nullable CoordinatorGroupCommitter groupCommitter) {
    this.storage = checkNotNull(storage);
    this.coordinator = checkNotNull(coordinator);
    this.tableMetadataManager = checkNotNull(tableMetadataManager);
    this.parallelExecutor = checkNotNull(parallelExecutor);

    if (groupCommitter != null) {
      // This method reference will be called via GroupCommitter.ready().
      groupCommitter.setEmitter(this::commitStateForGroupCommit);
    }
    this.groupCommitter = groupCommitter;
  }

  public void commit(Snapshot snapshot) throws CommitException, UnknownTransactionStatusException {
    try {
      prepare(snapshot);
    } catch (PreparationException e) {
      abortState(snapshot.getId());
      rollbackRecords(snapshot);
      if (e instanceof PreparationConflictException) {
        throw new CommitConflictException(e.getMessage(), e, e.getTransactionId().orElse(null));
      }
      throw new CommitException(e.getMessage(), e, e.getTransactionId().orElse(null));
    }
    // TODO: Consider if group-commit should be always canceled when other exception is thrown

    try {
      validate(snapshot);
    } catch (ValidationException e) {
      abortState(snapshot.getId());
      rollbackRecords(snapshot);
      if (e instanceof ValidationConflictException) {
        throw new CommitConflictException(e.getMessage(), e, e.getTransactionId().orElse(null));
      }
      throw new CommitException(e.getMessage(), e, e.getTransactionId().orElse(null));
    }
    // TODO: Consider if group-commit should be always canceled when other exception is thrown

    // if (groupCommitter != null && groupCommitter.isGroupCommitFullKey(snapshot.getId())) {
    if (groupCommitter != null) {
      commitViaGroupCommit(snapshot);
      return;
    }

    commitState(snapshot);
    commitRecords(snapshot);
  }

  private void commitOrRollbackRecordsAccordingToState(Snapshot snapshot, Exception cause)
      throws CommitException, UnknownTransactionStatusException {
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
      commitRecords(snapshot);
    } catch (CoordinatorException ex) {
      throw new UnknownTransactionStatusException(
          CoreError.CONSENSUS_COMMIT_CANNOT_GET_STATE.buildMessage(), ex, snapshot.getId());
    }
  }

  public void commitViaGroupCommit(Snapshot snapshot)
      throws CommitException, UnknownTransactionStatusException {
    String id = snapshot.getId();
    try {
      groupCommitter.ready(id, snapshot);
    } catch (GroupCommitAlreadyCompletedException e) {
      cancelGroupCommitIfNeeded(id);
      commitOrRollbackRecordsAccordingToState(snapshot, e);
      throw new CommitConflictException("Group commit failed due to a conflict", e, id);
    } catch (GroupCommitTargetNotFoundException e) {
      // This would happen with 2PC interface.
      // TODO: Why...?
      cancelGroupCommitIfNeeded(id);
      commitOrRollbackRecordsAccordingToState(snapshot, e);
    } catch (GroupCommitException e) {
      cancelGroupCommitIfNeeded(id);
      commitOrRollbackRecordsAccordingToState(snapshot, e);
      throw new CommitException("Group commit failed", e, id);
    }
    commitRecords(snapshot);
  }

  private void cancelGroupCommitIfNeeded(String id) {
    if (groupCommitter != null) {
      groupCommitter.remove(id);
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
      // TODO: The following code can be replaced with commitOrRollbackRecordsAccordingToState()?
      try {
        Optional<Coordinator.State> s = coordinator.getState(id);
        if (s.isPresent()) {
          TransactionState state = s.get().getState();
          if (state.equals(TransactionState.ABORTED)) {
            rollbackRecords(snapshot);
            throw new CommitConflictException(
                CoreError.CONSENSUS_COMMIT_CONFLICT_OCCURRED_WHEN_COMMITTING_STATE.buildMessage(),
                e,
                id);
          }
        } else {
          throw new UnknownTransactionStatusException(
              CoreError
                  .CONSENSUS_COMMIT_COMMITTING_STATE_FAILED_WITH_NO_MUTATION_EXCEPTION_BUT_COORDINATOR_STATUS_DOES_NOT_EXIST
                  .buildMessage(),
              e,
              id);
        }
      } catch (CoordinatorException e1) {
        throw new UnknownTransactionStatusException(
            CoreError.CONSENSUS_COMMIT_CANNOT_GET_STATE.buildMessage(), e1, id);
      }
    } catch (CoordinatorException e) {
      throw new UnknownTransactionStatusException(
          CoreError.CONSENSUS_COMMIT_UNKNOWN_COORDINATOR_STATUS.buildMessage(), e, id);
    }
  }

  private void commitStateForGroupCommit(String parentId, List<Snapshot> snapshots)
      throws CommitException, UnknownTransactionStatusException {
    if (snapshots.isEmpty()) {
      // This means all buffered transactions failed in the prepare phase.
      // Each call of prepare() puts ABORT state for the *full* id.
      return;
    }

    List<String> transactionIds =
        snapshots.stream().map(Snapshot::getId).collect(Collectors.toList());
    try {
      coordinator.putStateForGroupCommit(
          parentId, transactionIds, TransactionState.COMMITTED, System.currentTimeMillis());

      logger.debug(
          "Transaction {} is committed successfully at {}", parentId, System.currentTimeMillis());
    } catch (CoordinatorConflictException e) {
      // TODO: Execute this operation in parallel.
      for (Snapshot snapshot : snapshots) {
        commitOrRollbackRecordsAccordingToState(snapshot, e);
      }
    } catch (CoordinatorException e) {
      throw new UnknownTransactionStatusException("Coordinator status is unknown", e, parentId);
    }
  }

  public void commitRecords(Snapshot snapshot) {
    try {
      CommitMutationComposer composer = new CommitMutationComposer(snapshot.getId());
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
      cancelGroupCommitIfNeeded(id);
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
}
