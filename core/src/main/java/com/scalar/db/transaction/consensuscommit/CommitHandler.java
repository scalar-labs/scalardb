package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CoordinatorException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class CommitHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(CommitHandler.class);
  private final DistributedStorage storage;
  private final Coordinator coordinator;
  private final RecoveryHandler recovery;

  public CommitHandler(
      DistributedStorage storage, Coordinator coordinator, RecoveryHandler recovery) {
    this.storage = checkNotNull(storage);
    this.coordinator = checkNotNull(coordinator);
    this.recovery = checkNotNull(recovery);
  }

  public void commit(Snapshot snapshot) throws CommitException, UnknownTransactionStatusException {
    String id = snapshot.getId();

    try {
      prepareRecords(snapshot);
    } catch (Exception e) {
      LOGGER.warn("preparing records failed", e);
      abort(id);
      recovery.rollback(snapshot);
      if (e instanceof NoMutationException) {
        throw new CommitConflictException("preparing record exists", e);
      }
      throw new CommitException("preparing records failed", e);
    }

    // pre-commit validation is executed when SERIALIZABLE with EXTRA_READ strategy is chosen.
    try {
      snapshot.toSerializableWithExtraRead(storage);
    } catch (Exception e) {
      LOGGER.warn("pre-commit validation failed", e);
      abort(id);
      recovery.rollback(snapshot);
      throw new CommitConflictException("pre-commit validation failed", e);
    }

    try {
      commitState(snapshot.getId());
    } catch (CoordinatorException e) {
      TransactionState state = abort(id);
      if (state.equals(TransactionState.ABORTED)) {
        recovery.rollback(snapshot);
        throw new CommitException(
            "committing state in coordinator failed. " + "the transaction is aborted", e);
      }
    }
    LOGGER.info(
        "transaction " + id + " is committed successfully at " + System.currentTimeMillis());

    try {
      commitRecords(snapshot);
    } catch (ExecutionException e) {
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
        LOGGER.warn("coordinator status doesn't exist");
      } catch (CoordinatorException e1) {
        LOGGER.warn("can't get the state", e1);
      }
      throw new UnknownTransactionStatusException("coordinator status is unknown", e, id);
    }
  }

  private void prepareRecords(Snapshot snapshot)
      throws ExecutionException, CommitConflictException {
    PrepareMutationComposer composer = new PrepareMutationComposer(snapshot.getId());
    snapshot.to(composer);
    PartitionedMutations mutations = new PartitionedMutations(composer.get());

    for (PartitionedMutations.Key key : mutations.getOrderedKeys()) {
      storage.mutate(mutations.get(key));
    }
  }

  private void commitState(String id) throws CoordinatorException {
    Coordinator.State state = new Coordinator.State(id, TransactionState.COMMITTED);
    coordinator.putState(state);
  }

  private void commitRecords(Snapshot snapshot) throws ExecutionException, CommitConflictException {
    CommitMutationComposer composer = new CommitMutationComposer(snapshot.getId());
    snapshot.to(composer);
    PartitionedMutations mutations = new PartitionedMutations(composer.get());

    // TODO : make it configurable if it's synchronous or asynchronous
    for (PartitionedMutations.Key key : mutations.getOrderedKeys()) {
      storage.mutate(mutations.get(key));
    }
  }

  private void abortState(String id) throws CoordinatorException {
    Coordinator.State state = new Coordinator.State(id, TransactionState.ABORTED);
    coordinator.putState(state);
  }
}
