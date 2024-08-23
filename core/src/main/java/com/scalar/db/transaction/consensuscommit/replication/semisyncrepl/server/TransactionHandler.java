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
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.UpdatedRecord;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.UpdatedTuple;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.WrittenTuple;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.CoordinatorStateRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationRecordRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationTransactionRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationUpdatedRecordRepository;
import java.time.Instant;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TransactionHandler {
  private static final Logger logger = LoggerFactory.getLogger(TransactionHandler.class);
  private final int recordTablePartitionSize;
  private final long oldTransactionThresholdMillis;
  private final ReplicationTransactionRepository replicationTransactionRepository;
  private final ReplicationUpdatedRecordRepository replicationUpdatedRecordRepository;
  private final ReplicationRecordRepository replicationRecordRepository;
  private final CoordinatorStateRepository coordinatorStateRepository;
  private final InMemoryQueue<UpdatedRecord> queue;
  private final MetricsLogger metricsLogger;

  public TransactionHandler(
      int recordTablePartitionSize,
      long oldTransactionThresholdMillis,
      ReplicationTransactionRepository replicationTransactionRepository,
      ReplicationUpdatedRecordRepository replicationUpdatedRecordRepository,
      ReplicationRecordRepository replicationRecordRepository,
      CoordinatorStateRepository coordinatorStateRepository,
      InMemoryQueue<UpdatedRecord> queue,
      MetricsLogger metricsLogger) {
    this.recordTablePartitionSize = recordTablePartitionSize;
    this.oldTransactionThresholdMillis = oldTransactionThresholdMillis;
    this.replicationTransactionRepository = replicationTransactionRepository;
    this.replicationUpdatedRecordRepository = replicationUpdatedRecordRepository;
    this.replicationRecordRepository = replicationRecordRepository;
    this.coordinatorStateRepository = coordinatorStateRepository;
    this.queue = queue;
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

            UpdatedRecord updatedRecord =
                new UpdatedRecord(
                    partitionIdForRecord,
                    writtenTuple.namespace,
                    writtenTuple.table,
                    writtenTuple.partitionKey,
                    writtenTuple.clusteringKey,
                    Instant.now(),
                    nextVersion);

            // Persistent a notification to downstream workers.
            replicationUpdatedRecordRepository.add(updatedRecord);

            // Add the new values to the record.
            replicationRecordRepository.upsertWithNewValue(key, recordOpt, newValue);

            // Notify downstream workers via in-memory queue.
            queue.enqueue(partitionIdForRecord, updatedRecord);

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

  private boolean copyTuplesToRecords(Transaction transaction) throws ExecutionException {
    metricsLogger.incrementScannedTransactions();
    Optional<CoordinatorState> coordinatorState =
        coordinatorStateRepository.get(transaction.transactionId);
    if (!coordinatorState.isPresent()) {
      metricsLogger.incrementUncommittedTransactions();
      Instant now = Instant.now();
      if (transaction.updatedAt.isBefore(now.minusMillis(oldTransactionThresholdMillis))) {
        // TODO: Maybe always updating `updated_at` works better.
        logger.info(
            "Updating an old transaction to be handled later. txId:{}", transaction.transactionId);
        replicationTransactionRepository.updateUpdatedAt(transaction);
      }
      return false;
    }
    if (coordinatorState.get().txState != TransactionState.COMMITTED) {
      metricsLogger.incrementAbortedTransactions();
      return true;
    }

    // Copy written tuples to `records` table
    for (WrittenTuple writtenTuple : transaction.writtenTuples) {
      handleWrittenTuple(
          transaction.transactionId, writtenTuple, coordinatorState.get().txCommittedAt);
    }
    metricsLogger.incrementHandledCommittedTransactions();
    return true;
  }

  /**
   * Handle a transaction
   *
   * @param transaction A transaction
   * @return true if the transaction has finished and the transaction is removed, false otherwise.
   */
  boolean handleTransaction(Transaction transaction) throws ExecutionException {
    if (copyTuplesToRecords(transaction)) {
      replicationTransactionRepository.delete(transaction);
      return true;
    }
    return false;
  }
}
