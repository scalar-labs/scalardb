package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class RecoveryHandler {
  static final long TRANSACTION_LIFETIME_MILLIS = 15000;
  private static final Logger logger = LoggerFactory.getLogger(RecoveryHandler.class);
  private final DistributedStorage storage;
  private final Coordinator coordinator;
  private final TransactionTableMetadataManager tableMetadataManager;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public RecoveryHandler(
      DistributedStorage storage,
      Coordinator coordinator,
      TransactionTableMetadataManager tableMetadataManager) {
    this.storage = checkNotNull(storage);
    this.coordinator = checkNotNull(coordinator);
    this.tableMetadataManager = checkNotNull(tableMetadataManager);
  }

  // lazy recovery in read phase
  public void recover(Selection selection, TransactionResult result) {
    logger.debug("Recovering for {}", result.getId());

    Optional<Coordinator.State> state;
    try {
      state = coordinator.getState(result.getId());
    } catch (CoordinatorException e) {
      logger.warn("Can't get coordinator state. Transaction ID: {}", result.getId(), e);
      return;
    }

    if (state.isPresent()) {
      if (state.get().getState().equals(TransactionState.COMMITTED)) {
        rollforwardRecord(selection, result);
      } else {
        rollbackRecord(selection, result);
      }
    } else {
      abortIfExpired(selection, result);
    }
  }

  @VisibleForTesting
  void rollbackRecord(Selection selection, TransactionResult result) {
    logger.debug(
        "Rollback for {}, {} mutated by {}",
        selection.getPartitionKey(),
        selection.getClusteringKey(),
        result.getId());
    try {
      RollbackMutationComposer composer =
          new RollbackMutationComposer(result.getId(), storage, tableMetadataManager);
      composer.add(selection, result);
      mutate(composer.get());
    } catch (Exception e) {
      logger.warn("Rolling back a record failed. Transaction ID: {}", result.getId(), e);
      // ignore since the record is recovered lazily
    }
  }

  @VisibleForTesting
  void rollforwardRecord(Selection selection, TransactionResult result) {
    logger.debug(
        "Rollforward for {}, {} mutated by {}",
        selection.getPartitionKey(),
        selection.getClusteringKey(),
        result.getId());
    try {
      CommitMutationComposer composer =
          new CommitMutationComposer(result.getId(), tableMetadataManager);
      composer.add(selection, result);
      mutate(composer.get());
    } catch (Exception e) {
      logger.warn("Rolling forward a record failed. Transaction ID: {}", result.getId(), e);
      // ignore since the record is recovered lazily
    }
  }

  private void abortIfExpired(Selection selection, TransactionResult result) {
    long current = System.currentTimeMillis();
    if (current <= result.getPreparedAt() + TRANSACTION_LIFETIME_MILLIS) {
      return;
    }

    try {
      coordinator.putStateForLazyRecoveryRollback(result.getId());
      rollbackRecord(selection, result);
    } catch (CoordinatorException e) {
      logger.warn("Coordinator tries to abort {}, but failed", result.getId(), e);
    }
  }

  private void mutate(List<Mutation> mutations) throws ExecutionException {
    if (mutations.isEmpty()) {
      return;
    }
    storage.mutate(mutations);
  }
}
