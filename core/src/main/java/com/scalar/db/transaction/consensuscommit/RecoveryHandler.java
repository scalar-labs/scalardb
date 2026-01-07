package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils.getTransactionTableMetadata;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.Key;
import com.scalar.db.util.ScalarDbUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class RecoveryHandler {
  @VisibleForTesting static final long TRANSACTION_LIFETIME_MILLIS = 15000;
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

  public void recover(
      Selection selection, TransactionResult result, Optional<Coordinator.State> state)
      throws ExecutionException, CoordinatorException {
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
  void rollbackRecord(Selection selection, TransactionResult result) throws ExecutionException {
    assert selection.forFullTableName().isPresent();

    TransactionTableMetadata tableMetadata =
        getTransactionTableMetadata(tableMetadataManager, selection);
    Key partitionKey = ScalarDbUtils.getPartitionKey(result, tableMetadata.getTableMetadata());
    Optional<Key> clusteringKey =
        ScalarDbUtils.getClusteringKey(result, tableMetadata.getTableMetadata());

    logger.info(
        "Rolling back for a record. Table: {}; Partition Key: {}; Clustering Key: {}; Transaction ID that wrote the record: {}",
        selection.forFullTableName().get(),
        partitionKey,
        clusteringKey,
        result.getId());

    RollbackMutationComposer composer = createRollbackMutationComposer(selection, result);

    try {
      mutate(composer.get());
    } catch (NoMutationException e) {
      logger.info(
          "Rolling back for a record failed. Table: {}; Partition Key: {}; Clustering Key: {}; Transaction ID that wrote the record: {}",
          selection.forFullTableName().get(),
          partitionKey,
          clusteringKey,
          result.getId(),
          e);

      // This can happen when the record has already been rolled back by another transaction. In
      // this case, we just ignore it.
    }
  }

  @VisibleForTesting
  RollbackMutationComposer createRollbackMutationComposer(
      Selection selection, TransactionResult result) throws ExecutionException {
    RollbackMutationComposer composer =
        new RollbackMutationComposer(result.getId(), storage, tableMetadataManager);
    composer.add(selection, result);
    return composer;
  }

  @VisibleForTesting
  void rollforwardRecord(Selection selection, TransactionResult result) throws ExecutionException {
    assert selection.forFullTableName().isPresent();

    TransactionTableMetadata tableMetadata =
        getTransactionTableMetadata(tableMetadataManager, selection);
    Key partitionKey = ScalarDbUtils.getPartitionKey(result, tableMetadata.getTableMetadata());
    Optional<Key> clusteringKey =
        ScalarDbUtils.getClusteringKey(result, tableMetadata.getTableMetadata());

    logger.info(
        "Rolling forward for a record. Table: {}; Partition Key: {}; Clustering Key: {}; Transaction ID that wrote the record: {}",
        selection.forFullTableName().get(),
        partitionKey,
        clusteringKey,
        result.getId());

    CommitMutationComposer composer = createCommitMutationComposer(selection, result);

    try {
      mutate(composer.get());
    } catch (NoMutationException e) {
      logger.info(
          "Rolling forward for a record failed. Table: {}; Partition Key: {}; Clustering Key: {}; Transaction ID that wrote the record: {}",
          selection.forFullTableName().get(),
          partitionKey,
          clusteringKey,
          result.getId(),
          e);

      // This can happen when the record has already been committed by another transaction. In this
      // case, we just ignore it.
    }
  }

  @VisibleForTesting
  CommitMutationComposer createCommitMutationComposer(Selection selection, TransactionResult result)
      throws ExecutionException {
    CommitMutationComposer composer =
        new CommitMutationComposer(result.getId(), tableMetadataManager);
    composer.add(selection, result);
    return composer;
  }

  private void abortIfExpired(Selection selection, TransactionResult result)
      throws CoordinatorException, ExecutionException {
    assert selection.forFullTableName().isPresent();

    if (!isTransactionExpired(result)) {
      return;
    }

    try {
      coordinator.putStateForLazyRecoveryRollback(result.getId());
    } catch (CoordinatorConflictException e) {
      TransactionTableMetadata tableMetadata =
          getTransactionTableMetadata(tableMetadataManager, selection);
      Key partitionKey = ScalarDbUtils.getPartitionKey(result, tableMetadata.getTableMetadata());
      Optional<Key> clusteringKey =
          ScalarDbUtils.getClusteringKey(result, tableMetadata.getTableMetadata());

      logger.info(
          "Putting state in coordinator for a record failed. Table: {}; Partition Key: {}; Clustering Key: {}; Transaction ID that wrote the record: {}",
          selection.forFullTableName().get(),
          partitionKey,
          clusteringKey,
          result.getId(),
          e);

      // This can happen when the record has already been rolled back by another transaction. In
      // this case, we just ignore it.
      return;
    }

    rollbackRecord(selection, result);
  }

  private void mutate(List<Mutation> mutations) throws ExecutionException {
    if (mutations.isEmpty()) {
      return;
    }
    storage.mutate(mutations);
  }

  public boolean isTransactionExpired(TransactionResult result) {
    long current = System.currentTimeMillis();
    return current > result.getPreparedAt() + TRANSACTION_LIFETIME_MILLIS;
  }
}
