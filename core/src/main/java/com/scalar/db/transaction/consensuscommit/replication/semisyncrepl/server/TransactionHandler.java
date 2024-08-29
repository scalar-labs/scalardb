package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
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
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server.RecordHandler.ResultOfKeyHandling;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.function.FailableRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TransactionHandler {
  private static final Logger logger = LoggerFactory.getLogger(TransactionHandler.class);
  private final long oldTransactionThresholdMillis;
  private final ReplicationTransactionRepository replicationTransactionRepository;
  private final ReplicationRecordRepository replicationRecordRepository;
  private final CoordinatorStateRepository coordinatorStateRepository;
  private final RecordHandler recordHandler;
  private final ExecutorService recordHandlerExecutorService;
  private final MetricsLogger metricsLogger;

  public TransactionHandler(
      int recordTablePartitionSize,
      long oldTransactionThresholdMillis,
      ReplicationTransactionRepository replicationTransactionRepository,
      ReplicationRecordRepository replicationRecordRepository,
      CoordinatorStateRepository coordinatorStateRepository,
      RecordHandler recordHandler,
      ExecutorService recordHandlerExecutorService,
      MetricsLogger metricsLogger) {
    this.oldTransactionThresholdMillis = oldTransactionThresholdMillis;
    this.replicationTransactionRepository = replicationTransactionRepository;
    this.replicationRecordRepository = replicationRecordRepository;
    this.coordinatorStateRepository = coordinatorStateRepository;
    this.recordHandler = recordHandler;
    this.recordHandlerExecutorService = recordHandlerExecutorService;
    this.metricsLogger = metricsLogger;
  }

  private void handleWrittenTuple(
      String transactionId, WrittenTuple writtenTuple, Instant committedAt)
      throws ExecutionException {
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

    retryOnConflictException(
        "copy a written tuple to the record",
        key,
        () ->
            metricsLogger.execAppendValueToRecord(
                () -> {
                  Optional<Record> recordOpt = replicationRecordRepository.get(key);
                  // Add the new values to the record.
                  replicationRecordRepository.upsertWithNewValue(key, recordOpt, newValue);
                  return null;
                }));

    retryOnConflictException(
        "handle the written tuple",
        key,
        () -> {
          // Handle the new value.
          while (true) {
            ResultOfKeyHandling resultOfKeyHandling = recordHandler.handleKey(key, true);
            if (resultOfKeyHandling.nextConnectedValueExists) {
              // Need to handle the connected value immediately.
            } else {
              break;
            }
          }
        });
  }

  private boolean isConflictException(Throwable exception) {
    Throwable e = exception;
    while (e != null) {
      if (e instanceof NoMutationException) {
        return true;
      }

      e = e.getCause();
    }
    return false;
  }

  private void retryOnConflictException(
      String taskDesc, Key key, FailableRunnable<ExecutionException> task)
      throws ExecutionException {
    while (true) {
      try {
        task.run();
        return;
      } catch (Exception e) {
        if (isConflictException(e)) {
          logger.warn("Failed to {} due to conflict. Retrying... Key:{}", taskDesc, key, e);
          Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        } else {
          throw e;
        }
      }
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
  Optional<Transaction> handleTransaction(Transaction transaction) throws Exception {
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

    // Handle the written tuples.
    List<Future<?>> futures = new ArrayList<>(transaction.writtenTuples.size());
    for (WrittenTuple writtenTuple : transaction.writtenTuples) {
      logger.debug(
          "[handleTransaction]\n  transaction:{}\n  writtenTuple:{}\n", transaction, writtenTuple);

      futures.add(
          recordHandlerExecutorService.submit(
              () -> {
                try {
                  handleWrittenTuple(
                      transaction.transactionId,
                      writtenTuple,
                      coordinatorState.get().txCommittedAt);
                } catch (Exception e) {
                  throw new RuntimeException(
                      String.format(
                          "Failed to handle written tuples unexpectedly. Namespace:%s, Table:%s, Partition key:%s, Clustering key:%s",
                          writtenTuple.namespace,
                          writtenTuple.table,
                          writtenTuple.partitionKey,
                          writtenTuple.clusteringKey),
                      e);
                }
              }));
    }
    for (Future<?> future : futures) {
      // If a timeout occurs unexpectedly, all the written tuples will be retries.
      Uninterruptibles.getUninterruptibly(future, 60, TimeUnit.SECONDS);
    }

    metricsLogger.incrementHandledCommittedTransactions();
    replicationTransactionRepository.delete(transaction);
    return Optional.empty();
  }
}
