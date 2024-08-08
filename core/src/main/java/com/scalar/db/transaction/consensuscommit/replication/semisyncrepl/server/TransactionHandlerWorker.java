package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.CoordinatorState;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.DeletedTuple;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.InsertedTuple;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Record.Value;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Transaction;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.UpdatedTuple;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.WrittenTuple;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.CoordinatorStateRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationRecordRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationTransactionRepository;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionHandlerWorker extends BaseHandlerWorker {
  private static final Logger logger = LoggerFactory.getLogger(TransactionHandlerWorker.class);

  private final TransactionHandlerWorker.Configuration conf;
  private final ReplicationRecordRepository replicationRecordRepository;
  private final ReplicationTransactionRepository replicationTransactionRepository;
  private final CoordinatorStateRepository coordinatorStateRepository;
  private final Queue<Key> recordWriterQueue;
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
      ReplicationRecordRepository replicationRecordRepository,
      Queue<Key> recordWriterQueue,
      MetricsLogger metricsLogger) {
    super(conf, "tx", metricsLogger);
    this.conf = conf;
    this.replicationTransactionRepository = replicationTransactionRepository;
    this.coordinatorStateRepository = coordinatorStateRepository;
    this.replicationRecordRepository = replicationRecordRepository;
    this.recordWriterQueue = recordWriterQueue;
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
            replicationRecordRepository.upsertWithNewValue(key, newValue);
            return null;
          });
    } catch (Exception e) {
      String message =
          String.format(
              "Failed to append the value. key:%s, txId:%s, newValue:%s",
              key, transactionId, newValue);
      throw new RuntimeException(message, e);
    }

    recordWriterQueue.add(key);
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
          transaction.transactionId, writtenTuple, coordinatorState.get().txCommittedAt);
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
