package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.google.common.base.Objects;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.CoordinatorState;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.DeletedTuple;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.InsertedTuple;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Record;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Record.Value;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Transaction;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.UpdatedTuple;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.WrittenTuple;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.CoordinatorStateRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationRecordRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationTransactionRepository;
import java.time.Instant;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TransactionHandler {
  private static final Logger logger = LoggerFactory.getLogger(TransactionHandler.class);
  private final int recordTablePartitionSize;
  private final long oldTransactionThresholdMillis;
  private final ReplicationTransactionRepository replicationTransactionRepository;
  private final ReplicationRecordRepository replicationRecordRepository;
  private final CoordinatorStateRepository coordinatorStateRepository;
  private final InMemoryQueue<Key> recordQueue;
  private final MetricsLogger metricsLogger;

  public TransactionHandler(
      int recordTablePartitionSize,
      long oldTransactionThresholdMillis,
      ReplicationTransactionRepository replicationTransactionRepository,
      ReplicationRecordRepository replicationRecordRepository,
      CoordinatorStateRepository coordinatorStateRepository,
      InMemoryQueue<Key> recordQueue,
      MetricsLogger metricsLogger) {
    this.recordTablePartitionSize = recordTablePartitionSize;
    this.oldTransactionThresholdMillis = oldTransactionThresholdMillis;
    this.replicationTransactionRepository = replicationTransactionRepository;
    this.replicationRecordRepository = replicationRecordRepository;
    this.coordinatorStateRepository = coordinatorStateRepository;
    this.recordQueue = recordQueue;
    this.metricsLogger = metricsLogger;
  }

  private void handleWrittenTuple(
      String transactionId, WrittenTuple writtenTuple, Instant committedAt) {
    Key key =
        replicationRecordRepository.createKey(
            writtenTuple.namespace,
            writtenTuple.table,
            writtenTuple.partitionKey,
            writtenTuple.clusteringKey);

    Value newValue;
    if (writtenTuple instanceof InsertedTuple) {
      InsertedTuple tuple = (InsertedTuple) writtenTuple;
      newValue =
          new Value(
              null,
              transactionId,
              writtenTuple.txVersion,
              writtenTuple.txPreparedAtInMillis,
              committedAt.toEpochMilli(),
              "insert",
              tuple.columns);
    } else if (writtenTuple instanceof UpdatedTuple) {
      UpdatedTuple tuple = (UpdatedTuple) writtenTuple;
      newValue =
          new Value(
              tuple.prevTxId,
              transactionId,
              writtenTuple.txVersion,
              writtenTuple.txPreparedAtInMillis,
              committedAt.toEpochMilli(),
              "update",
              tuple.columns);
    } else if (writtenTuple instanceof DeletedTuple) {
      DeletedTuple tuple = (DeletedTuple) writtenTuple;
      newValue =
          new Value(
              tuple.prevTxId,
              transactionId,
              writtenTuple.txVersion,
              writtenTuple.txPreparedAtInMillis,
              committedAt.toEpochMilli(),
              "delete",
              null);
    } else {
      throw new AssertionError();
    }

    try {
      metricsLogger.execAppendValueToRecord(
          () -> {
            Optional<Record> recordOpt = replicationRecordRepository.get(key);
            long nextVersion = replicationRecordRepository.nextVersion(recordOpt);

            int partitionIdForRecord =
                Math.abs(
                        Objects.hashCode(
                            writtenTuple.namespace,
                            writtenTuple.table,
                            writtenTuple.partitionKey,
                            writtenTuple.clusteringKey))
                    % recordTablePartitionSize;

            // Add the new values to the record.
            replicationRecordRepository.upsertWithNewValue(key, recordOpt, newValue);

            recordQueue.enqueue(
                partitionIdForRecord,
                replicationRecordRepository.createKey(
                    writtenTuple.namespace,
                    writtenTuple.table,
                    writtenTuple.partitionKey,
                    writtenTuple.clusteringKey));

            return null;
          });
    } catch (Exception e) {
      String message =
          String.format(
              "Failed to append the value. key:%s, txId:%s, newValue:%s",
              key, transactionId, newValue);
      throw new RuntimeException(message, e);
    }
  }

  // FIXME: Return value comment.
  /**
   * Handle a transaction
   *
   * @param transaction A transaction
   * @return a transaction with updated `updated_at` if the transaction hasn't finished, empty
   *     otherwise.
   */
  Optional<Transaction> handleTransaction(Transaction transaction) throws ExecutionException {
    metricsLogger.incrementScannedTransactions();
    Optional<CoordinatorState> coordinatorState =
        coordinatorStateRepository.get(transaction.transactionId);
    if (!coordinatorState.isPresent()) {
      metricsLogger.incrementUncommittedTransactions();
      // TODO: Maybe always updating `updated_at` works better.
      if (transaction.updatedAt.isBefore(
          Instant.now().minusMillis(oldTransactionThresholdMillis))) {
        logger.info(
            "Updating an ongoing transaction to be handled later. txId:{}",
            transaction.transactionId);
        Transaction updatedTransaction =
            replicationTransactionRepository.updateUpdatedAt(transaction);
        return Optional.of(updatedTransaction);
      } else {
        return Optional.of(transaction);
      }
    }
    if (coordinatorState.get().txState != TransactionState.COMMITTED) {
      metricsLogger.incrementAbortedTransactions();
      replicationTransactionRepository.delete(transaction);
      return Optional.empty();
    }

    // Copy written tuples to `records` table
    for (WrittenTuple writtenTuple : transaction.writtenTuples) {
      handleWrittenTuple(
          transaction.transactionId, writtenTuple, coordinatorState.get().txCommittedAt);
    }
    metricsLogger.incrementHandledCommittedTransactions();
    replicationTransactionRepository.delete(transaction);
    return Optional.empty();
  }
}
