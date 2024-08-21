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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionHandlerWorker extends BaseHandlerWorker<UpdatedRecord> {
  private static final Logger logger = LoggerFactory.getLogger(TransactionHandlerWorker.class);

  private final TransactionHandlerWorker.Configuration conf;
  private final CoordinatorStateRepository coordinatorStateRepository;
  private final ReplicationTransactionRepository replicationTransactionRepository;
  private final ReplicationUpdatedRecordRepository replicationUpdatedRecordRepository;
  private final ReplicationRecordRepository replicationRecordRepository;
  private final MetricsLogger metricsLogger;

  @Immutable
  public static class Configuration extends BaseHandlerWorker.Configuration {
    private final int fetchSize;
    private final long thresholdMillisForOldTransaction;

    public Configuration(
        int replicationDbPartitionSize,
        int threadSize,
        int waitMillisPerPartition,
        int fetchSize,
        long thresholdMillisForOldTransaction) {
      super(replicationDbPartitionSize, threadSize, waitMillisPerPartition);
      this.fetchSize = fetchSize;
      this.thresholdMillisForOldTransaction = thresholdMillisForOldTransaction;
    }
  }

  public TransactionHandlerWorker(
      Configuration conf,
      CoordinatorStateRepository coordinatorStateRepository,
      ReplicationTransactionRepository replicationTransactionRepository,
      ReplicationUpdatedRecordRepository replicationUpdatedRecordRepository,
      ReplicationRecordRepository replicationRecordRepository,
      List<BlockingQueue<UpdatedRecord>> updatedRecordQueues,
      MetricsLogger metricsLogger) {
    super(conf, "tx", metricsLogger, updatedRecordQueues, null);
    this.conf = conf;
    this.coordinatorStateRepository = coordinatorStateRepository;
    this.replicationUpdatedRecordRepository = replicationUpdatedRecordRepository;
    this.replicationTransactionRepository = replicationTransactionRepository;
    this.replicationRecordRepository = replicationRecordRepository;
    this.metricsLogger = metricsLogger;
  }

  private void handleWrittenTuple(
      int partitionId, String transactionId, WrittenTuple writtenTuple, Instant committedAt) {
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
                    % conf.replicationDbPartitionSize;

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
            getQueueToSupply(partitionIdForRecord).add(updatedRecord);

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

  /**
   * Copy tuples to Records table
   *
   * @param transaction A transaction
   * @return true if the transaction has finished, false otherwise.
   */
  private boolean copyTuplesToRecords(Transaction transaction) throws ExecutionException {
    metricsLogger.incrementScannedTransactions();
    Optional<CoordinatorState> coordinatorState =
        coordinatorStateRepository.get(transaction.transactionId);
    if (!coordinatorState.isPresent()) {
      metricsLogger.incrementUncommittedTransactions();
      Instant now = Instant.now();
      if (transaction.updatedAt.isBefore(now.minusMillis(conf.thresholdMillisForOldTransaction))) {
        // TODO: Maybe always updating `updated_at` works better.
        logger.info(
            "Updating an old transaction to be handled later. txId:{}", transaction.transactionId);
        replicationTransactionRepository.updateUpdatedAt(transaction);
      }
      return false;
    }
    if (coordinatorState.get().txState != TransactionState.COMMITTED) {
      // TODO: Add AbortedTransactions
      metricsLogger.incrementUncommittedTransactions();
      return true;
    }

    // Copy written tuples to `records` table
    for (WrittenTuple writtenTuple : transaction.writtenTuples) {
      handleWrittenTuple(
          transaction.partitionId,
          transaction.transactionId,
          writtenTuple,
          coordinatorState.get().txCommittedAt);
    }
    metricsLogger.incrementHandledCommittedTransactions();
    return true;
  }

  @Override
  protected boolean handle(int partitionId) throws ExecutionException {
    List<Transaction> scannedTxns =
        metricsLogger.execFetchTransactions(
            () -> replicationTransactionRepository.scan(partitionId, conf.fetchSize));
    int finishedTransactions = 0;
    for (Transaction transaction : scannedTxns) {
      if (copyTuplesToRecords(transaction)) {
        replicationTransactionRepository.delete(transaction);
        finishedTransactions++;
      }
    }

    return finishedTransactions >= conf.fetchSize;
  }
}
