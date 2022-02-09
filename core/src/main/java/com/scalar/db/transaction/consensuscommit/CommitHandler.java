package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableList;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.storage.RetriableExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.transaction.consensuscommit.ParallelExecutor.ParallelExecutorTask;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class CommitHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(CommitHandler.class);
  private final DistributedStorage storage;
  private final Coordinator coordinator;
  private final TransactionalTableMetadataManager tableMetadataManager;
  private final ParallelExecutor parallelExecutor;

  public CommitHandler(
      DistributedStorage storage,
      Coordinator coordinator,
      TransactionalTableMetadataManager tableMetadataManager,
      ParallelExecutor parallelExecutor) {
    this.storage = checkNotNull(storage);
    this.coordinator = checkNotNull(coordinator);
    this.tableMetadataManager = checkNotNull(tableMetadataManager);
    this.parallelExecutor = checkNotNull(parallelExecutor);
  }

  public void commit(Snapshot snapshot) throws CommitException, UnknownTransactionStatusException {
    prepare(snapshot, true);
    preCommitValidation(snapshot, true);
    commitState(snapshot);
    commitRecords(snapshot);
  }

  public void prepare(Snapshot snapshot, boolean abortIfError)
      throws CommitException, UnknownTransactionStatusException {
    String id = snapshot.getId();
    try {
      prepareRecords(snapshot);
    } catch (Exception e) {
      LOGGER.warn("preparing records failed", e);
      if (abortIfError) {
        abort(id);
        rollbackRecords(snapshot);
      }
      if (e instanceof CommitConflictException) {
        throw (CommitConflictException) e;
      }
      if (e instanceof NoMutationException) {
        throw new CommitConflictException("preparing record exists", e);
      }
      if (e instanceof RetriableExecutionException) {
        throw new CommitConflictException("conflict happened when preparing records", e);
      }
      throw new CommitException("preparing records failed", e);
    }
  }

  private void prepareRecords(Snapshot snapshot)
      throws ExecutionException, CommitConflictException {
    PrepareMutationComposer composer = new PrepareMutationComposer(snapshot.getId());
    snapshot.to(composer);
    PartitionedMutations mutations = new PartitionedMutations(composer.get());

    ImmutableList<PartitionedMutations.Key> orderedKeys = mutations.getOrderedKeys();
    List<ParallelExecutorTask> tasks = new ArrayList<>(orderedKeys.size());
    for (PartitionedMutations.Key key : orderedKeys) {
      tasks.add(() -> storage.mutate(mutations.get(key)));
    }
    parallelExecutor.prepare(tasks);
  }

  public void preCommitValidation(Snapshot snapshot, boolean abortIfError)
      throws CommitException, UnknownTransactionStatusException {
    // pre-commit validation is executed when SERIALIZABLE with EXTRA_READ strategy is chosen.
    try {
      snapshot.toSerializableWithExtraRead(storage);
    } catch (Exception e) {
      LOGGER.warn("pre-commit validation failed", e);
      if (abortIfError) {
        abort(snapshot.getId());
        rollbackRecords(snapshot);
      }
      if (e instanceof CommitConflictException) {
        throw (CommitConflictException) e;
      }
      throw new CommitException("pre-commit validation failed", e);
    }
  }

  public void commitState(Snapshot snapshot)
      throws CommitException, UnknownTransactionStatusException {
    String id = snapshot.getId();
    try {
      commitState(snapshot.getId());
    } catch (CoordinatorException e) {
      TransactionState state = abort(id);
      if (state.equals(TransactionState.ABORTED)) {
        rollbackRecords(snapshot);
        throw new CommitException(
            "committing state in coordinator failed. the transaction is aborted", e);
      }
    }
    LOGGER.debug("transaction {} is committed successfully at {}", id, System.currentTimeMillis());
  }

  private void commitState(String id) throws CoordinatorException {
    Coordinator.State state = new Coordinator.State(id, TransactionState.COMMITTED);
    coordinator.putState(state);
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
      parallelExecutor.commit(tasks);
    } catch (Exception e) {
      LOGGER.warn("committing records failed", e);
      // ignore since records are recovered lazily
    }
  }

  public TransactionState abort(String id) throws UnknownTransactionStatusException {
    try {
      abortState(id);
      return TransactionState.ABORTED;
    } catch (CoordinatorException e) {
      try {
        Optional<Coordinator.State> state = coordinator.getState(id);
        if (state.isPresent()) {
          // successfully COMMITTED or ABORTED
          return state.get().getState();
        }
        LOGGER.warn("coordinator status for {} doesn't exist", id);
      } catch (CoordinatorException e1) {
        LOGGER.warn("can't get the state", e1);
      }
      throw new UnknownTransactionStatusException("coordinator status is unknown", e, id);
    }
  }

  private void abortState(String id) throws CoordinatorException {
    Coordinator.State state = new Coordinator.State(id, TransactionState.ABORTED);
    coordinator.putState(state);
  }

  public void rollbackRecords(Snapshot snapshot) {
    LOGGER.debug("rollback from snapshot for {}", snapshot.getId());
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
      parallelExecutor.rollback(tasks);
    } catch (Exception e) {
      LOGGER.warn("rolling back records failed", e);
      // ignore since records are recovered lazily
    }
  }
}
