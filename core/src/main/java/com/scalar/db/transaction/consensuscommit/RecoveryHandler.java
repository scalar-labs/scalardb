package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
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
  private final TransactionalTableMetadataManager tableMetadataManager;

  public RecoveryHandler(
      DistributedStorage storage,
      Coordinator coordinator,
      TransactionalTableMetadataManager tableMetadataManager) {
    this.storage = checkNotNull(storage);
    this.coordinator = checkNotNull(coordinator);
    this.tableMetadataManager = checkNotNull(tableMetadataManager);
  }

  // lazy recovery in read phase
  public void recover(Selection selection, TransactionResult result) {
    LOGGER.debug("recovering for {}", result.getId());

    // as the result doesn't have before image columns, need to get the latest result
    Optional<TransactionResult> latestResult;
    try {
      latestResult = getLatestResult(selection, result);
    } catch (ExecutionException e) {
      LOGGER.warn("can't get the latest result", e);
      return;
    }

    if (!latestResult.isPresent()) {
      // indicates the record has been deleted by another transaction
      return;
    }

    if (latestResult.get().isCommitted()) {
      // indicates the record has been committed by another transaction
      return;
    }

    Optional<Coordinator.State> state;
    try {
      state = coordinator.getState(latestResult.get().getId());
    } catch (CoordinatorException e) {
      LOGGER.warn("can't get coordinator state", e);
      return;
    }

    if (state.isPresent()) {
      if (state.get().getState().equals(TransactionState.COMMITTED)) {
        rollforwardRecord(selection, latestResult.get());
      } else {
        rollbackRecord(selection, latestResult.get());
      }
    } else {
      abortIfExpired(selection, latestResult.get());
    }
  }

  @VisibleForTesting
  Optional<TransactionResult> getLatestResult(Selection selection, TransactionResult result)
      throws ExecutionException {
    Get get =
        new Get(selection.getPartitionKey(), getClusteringKey(selection, result).orElse(null))
            .withConsistency(Consistency.LINEARIZABLE)
            .forNamespace(selection.forNamespace().get())
            .forTable(selection.forTable().get());
    return storage.get(get).map(TransactionResult::new);
  }

  private Optional<Key> getClusteringKey(Selection selection, TransactionResult result) {
    if (selection instanceof Scan) {
      return result.getClusteringKey();
    } else {
      return selection.getClusteringKey();
    }
  }

  @VisibleForTesting
  void rollbackRecord(Selection selection, TransactionResult result) {
    LOGGER.debug(
        "rollback for {}, {} mutated by {}",
        selection.getPartitionKey(),
        selection.getClusteringKey(),
        result.getId());
    try {
      RollbackMutationComposer composer =
          new RollbackMutationComposer(result.getId(), storage, tableMetadataManager);
      composer.add(selection, result);
      mutate(composer.get());
    } catch (Exception e) {
      LOGGER.warn("rolling back a record failed", e);
      // ignore since the record is recovered lazily
    }
  }

  @VisibleForTesting
  void rollforwardRecord(Selection selection, TransactionResult result) {
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
      rollbackRecord(selection, result);
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
