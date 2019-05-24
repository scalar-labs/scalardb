package com.scalar.database.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.database.api.DistributedStorage;
import com.scalar.database.api.Mutation;
import com.scalar.database.api.Operation;
import com.scalar.database.api.Scan;
import com.scalar.database.api.Selection;
import com.scalar.database.api.TransactionState;
import com.scalar.database.exception.storage.ExecutionException;
import com.scalar.database.exception.transaction.CoordinatorException;
import com.scalar.database.io.Key;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recovery handler for records that may have resulted from incomplete transactions.
 *
 * <p>If a transaction was unable to complete its commit phase for some reason or another there may be records that need to be recovered. If the record is in the COMMITTED state, recovery is not necessary.
 * However, if the record is in the PREPARED state then it will need to be either rolled forward or rolled back depending on the transaction status. If the transaction itself is in the COMMITTED state
 * then we roll forward, and otherwise we roll back.</p>
 */
public class RecoveryHandler {
  static final long TRANSACTION_LIFETIME_MILLIS = 15000;
  private static final Logger LOGGER = LoggerFactory.getLogger(RecoveryHandler.class);
  private final DistributedStorage storage;
  private final Coordinator coordinator;

  public RecoveryHandler(DistributedStorage storage, Coordinator coordinator) {
    this.storage = checkNotNull(storage);
    this.coordinator = checkNotNull(coordinator);
  }

  /**
   * This method will roll back or roll forward a record depending on the state of the transaction that produced it. If the transaction reached the COMMITTED state then the record will be rolled forward, otherwise it will be rolled back.
   *
   * <p>Used for lazy recovery in the read phase.
   *
   * @param selection a {@code Selection}
   * @param result a {@code TransactionResult}
   */
  public void recover(Selection selection, TransactionResult result) {
    LOGGER.info("recovering for " + result.getId());
    Optional<Coordinator.State> state = null;
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

  /**
   * Rolls back a records to its previous value before the transaction occurred. The value of the record is retrieved from the given {@code Snapshot}.
   *
   * @param snapshot a {@code Snapshot}
   */
  public void rollback(Snapshot snapshot) {
    LOGGER.info("rollback from snapshot for " + snapshot.getId());
    RollbackMutationComposer composer = new RollbackMutationComposer(snapshot.getId(), storage);
    snapshot.to(composer);
    PartitionedMutations mutations = new PartitionedMutations(composer.get());

    for (PartitionedMutations.Key key : mutations.getOrderedKeys()) {
      mutate(mutations.get(key));
    }
  }

  @VisibleForTesting
  void rollback(Selection selection, TransactionResult result) {
    LOGGER.info(
        "rollback for "
            + selection.getPartitionKey()
            + ", "
            + selection.getClusteringKey()
            + " mutated by "
            + result.getId());
    RollbackMutationComposer composer = new RollbackMutationComposer(result.getId(), storage);
    composer.add(selection, result);
    mutate(composer.get());
  }

  /**
   * Rolls forward a records to the value it would have attained assuming the transaction completed its commit phase.
   */
  @VisibleForTesting
  void rollforward(Selection selection, TransactionResult result) {
    LOGGER.info(
        "rollforward for "
            + selection.getPartitionKey()
            + ", "
            + selection.getClusteringKey()
            + " mutated by "
            + result.getId());
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
      LOGGER.warn("coordinator tries to abort " + result.getId() + ", but failed", e);
    }
  }

  private void mutate(List<Mutation> mutations) {
    try {
      storage.mutate(mutations);
    } catch (ExecutionException e) {
      LOGGER.warn("mutation in recovery failed. the record will be eventually recovered", e);
    }
  }

  private static Optional<Key> getClusteringKey(Operation base, TransactionResult result) {
    if (base instanceof Scan) {
      return result.getClusteringKey();
    } else {
      return base.getClusteringKey();
    }
  }
}
