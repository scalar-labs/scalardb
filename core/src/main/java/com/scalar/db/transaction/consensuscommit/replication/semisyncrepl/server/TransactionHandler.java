package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.DeletedTuple;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.InsertedTuple;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Record;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Record.RecordKey;
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
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.function.FailableCallable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TransactionHandler {
  private static final Logger logger = LoggerFactory.getLogger(TransactionHandler.class);
  private final ReplicationTransactionRepository replicationTransactionRepository;
  private final ReplicationRecordRepository replicationRecordRepository;
  private final CoordinatorStateRepository coordinatorStateRepository;
  private final RecordHandler recordHandler;
  private final MetricsLogger metricsLogger;

  public TransactionHandler(
      ReplicationTransactionRepository replicationTransactionRepository,
      ReplicationRecordRepository replicationRecordRepository,
      CoordinatorStateRepository coordinatorStateRepository,
      RecordHandler recordHandler,
      MetricsLogger metricsLogger) {
    this.replicationTransactionRepository = replicationTransactionRepository;
    this.replicationRecordRepository = replicationRecordRepository;
    this.coordinatorStateRepository = coordinatorStateRepository;
    this.recordHandler = recordHandler;
    this.metricsLogger = metricsLogger;
  }

  private void handleWrittenTuple(
      String transactionId, WrittenTuple writtenTuple, Instant committedAt)
      throws ExecutionException {
    RecordKey key =
        new RecordKey(
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

    // These operations are retried separately since appending value doesn't need to be retried
    // once it finished successfully.
    AtomicReference<Record> updatedRecord = new AtomicReference<>();
    updatedRecord.set(
        retryOnConflictException(
            "copy a written tuple to the record",
            key,
            () -> {},
            () ->
                metricsLogger.execAppendValueToRecord(
                    () -> {
                      // Add the new values to the record.
                      return replicationRecordRepository.upsertWithNewValue(key, newValue);
                    })));

    retryOnConflictException(
        "handle the written tuple",
        key,
        // Clear the updated record to get the latest record from the table.
        () -> updatedRecord.set(null),
        () -> {
          while (true) {
            Record record = updatedRecord.get();
            if (record == null) {
              // If null, it means the record prepared above was too old.
              Optional<Record> recordOpt =
                  metricsLogger.execGetRecord(() -> replicationRecordRepository.get(key));
              if (!recordOpt.isPresent()) {
                throw new AssertionError("The record must exist. Key: " + key);
              }
              record = recordOpt.get();
            }
            ResultOfKeyHandling resultOfKeyHandling = recordHandler.handleRecord(record, true);
            if (resultOfKeyHandling.nextConnectedValueExists) {
              // There is a connected value in the record. Need to retry immediately.
            } else {
              return null;
            }
          }
        });
  }

  // TODO: Avoid the duplication.
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

  private <T> T retryOnConflictException(
      String taskDesc,
      RecordKey key,
      Runnable callbackOnConflict,
      FailableCallable<T, ExecutionException> task)
      throws ExecutionException {
    while (true) {
      try {
        return task.call();
      } catch (Exception e) {
        if (isConflictException(e)) {
          logger.warn("Failed to {} due to conflict. Retrying... Key:{}", taskDesc, key, e);
          callbackOnConflict.run();
          Uninterruptibles.sleepUninterruptibly(50, TimeUnit.MILLISECONDS);
        } else {
          throw e;
        }
      }
    }
  }

  /**
   * Handle a transaction
   *
   * @param transaction A transaction
   * @return true if handling the transaction is done, false otherwise.
   */
  boolean handleTransaction(ExecutorService executorService, Transaction transaction)
      throws Exception {
    Optional<Coordinator.State> coordinatorState =
        metricsLogger.execGetCoordinatorState(
            () -> coordinatorStateRepository.get(transaction.transactionId));
    if (!coordinatorState.isPresent()) {
      metricsLogger.incrementUncommittedTransactions();
      // This logic might be needed in TransactionScanner.
      /*
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
       */
      return false;
    }
    if (coordinatorState.get().getState() != TransactionState.COMMITTED) {
      metricsLogger.incrementAbortedTransactions();
      replicationTransactionRepository.delete(transaction);
      return true;
    }

    // Handle the written tuples.
    List<Future<?>> futures = new ArrayList<>(transaction.writtenTuples.size());
    int writtenTupleIndex = 0;
    for (WrittenTuple writtenTuple : transaction.writtenTuples) {
      logger.debug(
          "[handleTransaction]\n  transaction:{}\n  writtenTuple:{}\n", transaction, writtenTuple);

      Runnable task =
          () -> {
            try {
              handleWrittenTuple(
                  transaction.transactionId,
                  writtenTuple,
                  Instant.ofEpochMilli(coordinatorState.get().getCreatedAt()));
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
          };

      // Handle the last written tuple with its own thread.
      if (writtenTupleIndex < transaction.writtenTuples.size() - 1) {
        futures.add(executorService.submit(task));
      } else {
        task.run();
      }

      writtenTupleIndex++;
    }
    for (Future<?> future : futures) {
      // If a timeout occurs unexpectedly, all the written tuples will be retries.
      Uninterruptibles.getUninterruptibly(future, 60, TimeUnit.SECONDS);
    }

    metricsLogger.incrCommittedTxns();
    replicationTransactionRepository.delete(transaction);

    return true;
  }
}
