package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CoordinatorException;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class RecoveryHandler {
  static final long TRANSACTION_LIFETIME_MILLIS = 15000;
  private static final Logger LOGGER = LoggerFactory.getLogger(RecoveryHandler.class);
  private final DistributedStorage storage;
  private final Coordinator coordinator;

  public RecoveryHandler(DistributedStorage storage, Coordinator coordinator) {
    this.storage = checkNotNull(storage);
    this.coordinator = checkNotNull(coordinator);
  }

  // lazy recovery in read phase
  public void recover(Selection selection, TransactionResult result) {
    LOGGER.debug("recovering for {}", result.getId());
    Optional<Coordinator.State> state;
    try {
      state = coordinator.getState(result.getId());
    } catch (CoordinatorException e) {
      LOGGER.warn("can't get coordinator state", e);
      return;
    }

    if (state.isPresent()) {
      if (state.get().getState().equals(TransactionState.COMMITTED)) {
        rollforward(selection, result);
      } else {
        rollback(selection, result);
      }
    } else {
      abortIfExpired(selection, result);
    }
  }

  public void rollback(Snapshot snapshot) throws CommitConflictException {
    LOGGER.debug("rollback from snapshot for {}", snapshot.getId());
    RollbackMutationComposer composer = new RollbackMutationComposer(snapshot.getId(), storage);
    snapshot.to(composer);
    PartitionedMutations mutations = new PartitionedMutations(composer.get());

    for (PartitionedMutations.Key key : mutations.getOrderedKeys()) {
      mutate(mutations.get(key));
    }
  }

  @VisibleForTesting
  void rollback(Selection selection, TransactionResult result) {
    LOGGER.debug(
        "rollback for {}, {} mutated by {}",
        selection.getPartitionKey(),
        selection.getClusteringKey(),
        result.getId());
    RollbackMutationComposer composer = new RollbackMutationComposer(result.getId(), storage);
    composer.add(selection, result);
    mutate(composer.get());
  }

  @VisibleForTesting
  void rollforward(Selection selection, TransactionResult result) {
    LOGGER.debug(
        "rollforward for {}, {} mutated by {}",
        selection.getPartitionKey(),
        selection.getClusteringKey(),
        result.getId());
    CommitMutationComposer composer = new CommitMutationComposer(result.getId());
    composer.add(selection, result);
    mutate(composer.get());
  }

  private void abortIfExpired(Selection selection, TransactionResult result) {
    long current = System.currentTimeMillis();
    if (current <= result.getPreparedAt() + TRANSACTION_LIFETIME_MILLIS) {
      return;
    }

    try {
      coordinator.putState(new Coordinator.State(result.getId(), TransactionState.ABORTED));
      rollback(selection, result);
    } catch (CoordinatorException e) {
      LOGGER.warn("coordinator tries to abort {}, but failed", result.getId(), e);
    }
  }

  private void mutate(List<Mutation> mutations) {
    try {
      storage.mutate(mutations);
    } catch (ExecutionException e) {
      LOGGER.warn("mutation in recovery failed. the record will be eventually recovered", e);
    }
  }
}
