package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils.*;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.storage.RetriableExecutionException;
import com.scalar.db.io.Key;
import com.scalar.db.util.ScalarDbUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class RecoveryHandler {
  @VisibleForTesting static final long TRANSACTION_LIFETIME_MILLIS = 15000;

  // The maximum number of retries for recovery mutations that the storage reports as a conflict.
  // See mutate(List, String) for why retrying is safe and why no backoff is applied.
  @VisibleForTesting static final int MAX_CONFLICT_RETRY_COUNT = 10;
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
      mutate(composer.get(), result.getId());
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
      mutate(composer.get(), result.getId());
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

  /**
   * Aborts the transaction that wrote a record, for the read path, when that transaction has
   * expired and has no coordinator state, by writing its ABORTED coordinator state (the
   * lazy-recovery rollback). The before-image of such a record is only the correct value to return
   * once that transaction is known to be aborted, which this confirms.
   *
   * @param id the transaction id of the transaction that wrote the record. The caller must have
   *     already confirmed the transaction is expired and has no coordinator state; this method does
   *     not check either condition
   * @return {@code true} if the ABORTED state was written — the transaction is now aborted, so
   *     returning the before-image is correct; {@code false} if writing it conflicted because a
   *     concurrent actor resolved the transaction (e.g. it committed), in which case the caller
   *     must re-read the coordinator state and resolve the read from that outcome instead of
   *     returning a stale before-image
   * @throws CoordinatorException if writing the ABORTED state fails for a reason other than a
   *     conflict
   */
  boolean tryAbortExpiredTransaction(String id) throws CoordinatorException {
    try {
      coordinator.putStateForLazyRecoveryRollback(id);
      return true;
    } catch (CoordinatorConflictException e) {
      logger.info(
          "Putting state in coordinator for a record conflicted; a concurrent actor resolved the transaction. Transaction ID: {}",
          id,
          e);
      return false;
    }
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
          "Putting state in coordinator for a record conflicted; a concurrent actor resolved the transaction. "
              + "Table: {}; Partition Key: {}; Clustering Key: {}; Transaction ID that wrote the record: {}",
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

  /**
   * Applies the given recovery mutations, retrying them when the storage reports a conflict.
   *
   * <p>Retrying is needed because the recovery of several records runs on separate threads (see
   * {@link RecoveryExecutor}), so two recovery tasks can write the same partition concurrently and
   * the storage can report a conflict. Without a retry, that conflict fails the read or the commit
   * that triggered the recovery, even though it is transient.
   *
   * <p>Retrying is safe. Recovery mutations are conditional on the record still being the one this
   * recovery observed (its {@code tx_id} and {@code state}), so re-applying them cannot overwrite a
   * newer state: if a concurrent actor resolved the record while we retried, the retry raises
   * {@link NoMutationException}, which the callers already absorb as the record having been rolled
   * back or committed by another transaction. A {@link RetriableExecutionException} also means the
   * mutation definitely did not apply, so the failed attempt leaves no partial effect behind.
   *
   * <p>No backoff is applied. Some conflicts are reported only after the competing work has ended,
   * for which waiting does not improve the odds (see {@code JdbcAdmin#executeWithConflictRetry}).
   * Others are reported while the competing operation is still in progress, such as a DynamoDB
   * {@code TransactionConflictException} raised against an in-flight {@code TransactWriteItems};
   * the retries span several round trips, which is expected to outlast such an operation, and if
   * they do not, the conflict reaches the caller as a conflict so that the transaction can be
   * retried. The loop is bounded by {@link #MAX_CONFLICT_RETRY_COUNT} so that sustained contention
   * surfaces to the caller instead of being absorbed indefinitely.
   */
  private void mutate(List<Mutation> mutations, @Nullable String transactionId)
      throws ExecutionException {
    if (mutations.isEmpty()) {
      return;
    }

    int attempt = 0;
    while (true) {
      try {
        storage.mutate(mutations);
        return;
      } catch (RetriableExecutionException e) {
        if (attempt >= MAX_CONFLICT_RETRY_COUNT) {
          logger.warn(
              "Giving up on conflicting recovery mutations after {} retries. Transaction ID that wrote the record: {}",
              attempt,
              transactionId,
              e);
          throw e;
        }
        attempt++;
        logger.warn(
            "Retrying conflicting recovery mutations (attempt {}). Transaction ID that wrote the record: {}",
            attempt,
            transactionId,
            e);
      }
    }
  }

  boolean isTransactionExpired(TransactionResult result) {
    long current = System.currentTimeMillis();
    return current > result.getPreparedAt() + TRANSACTION_LIFETIME_MILLIS;
  }
}
