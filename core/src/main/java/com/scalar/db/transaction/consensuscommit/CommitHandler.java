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
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.exception.transaction.ValidationException;
import com.scalar.db.transaction.consensuscommit.ParallelExecutor.ParallelExecutorTask;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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

  public void commit(Snapshot snapshot) throws CommitException, UnknownTransactionStatusException {
    try {
      prepare(snapshot);
    } catch (PreparationException e) {
      abort(snapshot.getId());
      rollbackRecords(snapshot);
      if (e instanceof PreparationConflictException) {
        throw new CommitConflictException(e.getMessage(), e);
      }
      throw new CommitException(e.getMessage(), e);
    }

    try {
      validate(snapshot);
    } catch (ValidationException e) {
      abort(snapshot.getId());
      rollbackRecords(snapshot);
      if (e instanceof ValidationConflictException) {
        throw new CommitConflictException(e.getMessage(), e);
      }
      throw new CommitException(e.getMessage(), e);
    }

    commitState(snapshot);
    commitRecords(snapshot);
  }

  public void prepare(Snapshot snapshot) throws PreparationException {
    try {
      prepareRecords(snapshot);
    } catch (NoMutationException e) {
      throw new PreparationConflictException("preparing record exists", e);
    } catch (RetriableExecutionException e) {
      throw new PreparationConflictException("conflict happened when preparing records", e);
    } catch (ExecutionException e) {
      throw new PreparationException("preparing records failed", e);
    }
  }

  private void prepareRecords(Snapshot snapshot)
      throws ExecutionException, PreparationConflictException {
    PrepareMutationComposer composer = new PrepareMutationComposer(snapshot.getId());
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
      throw new ValidationException("validation failed", e);
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
    logger.debug("transaction {} is committed successfully at {}", id, System.currentTimeMillis());
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
      parallelExecutor.commitRecords(tasks, snapshot.getId());
    } catch (Exception e) {
      logger.warn("committing records failed", e);
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
        logger.warn("coordinator status for {} doesn't exist", id);
      } catch (CoordinatorException e1) {
        logger.warn("can't get the state", e1);
      }
      throw new UnknownTransactionStatusException("coordinator status is unknown", e, id);
    }
  }

  private void abortState(String id) throws CoordinatorException {
    Coordinator.State state = new Coordinator.State(id, TransactionState.ABORTED);
    coordinator.putState(state);
  }

  public void rollbackRecords(Snapshot snapshot) {
    logger.debug("rollback from snapshot for {}", snapshot.getId());
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
      logger.warn("rolling back records failed", e);
      // ignore since records are recovered lazily
    }
  }
}
