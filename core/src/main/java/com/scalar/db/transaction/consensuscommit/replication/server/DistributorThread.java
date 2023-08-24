package com.scalar.db.transaction.consensuscommit.replication.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.replication.model.CoordinatorState;
import com.scalar.db.transaction.consensuscommit.replication.model.DeletedTuple;
import com.scalar.db.transaction.consensuscommit.replication.model.InsertedTuple;
import com.scalar.db.transaction.consensuscommit.replication.model.Record.Value;
import com.scalar.db.transaction.consensuscommit.replication.model.Transaction;
import com.scalar.db.transaction.consensuscommit.replication.model.UpdatedTuple;
import com.scalar.db.transaction.consensuscommit.replication.model.WrittenTuple;
import com.scalar.db.transaction.consensuscommit.replication.repository.CoordinatorStateRepository;
import com.scalar.db.transaction.consensuscommit.replication.repository.ReplicationRecordRepository;
import com.scalar.db.transaction.consensuscommit.replication.repository.ReplicationTransactionRepository;
import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributorThread implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(DistributorThread.class);

  private final ExecutorService executorService;
  private final int replicationDbPartitionSize;
  private final int threadSize;
  private final int fetchThreadSize;
  private final ReplicationRecordRepository replicationRecordRepository;
  private final ReplicationTransactionRepository replicationTransactionRepository;
  private final CoordinatorStateRepository coordinatorStateRepository;
  private final Queue<Key> recordWriterQueue;

  public DistributorThread(
      int replicationDbPartitionSize,
      int threadSize,
      int fetchThreadSize,
      CoordinatorStateRepository coordinatorStateRepository,
      ReplicationTransactionRepository replicationTransactionRepository,
      ReplicationRecordRepository replicationRecordRepository,
      Queue<Key> recordWriterQueue) {
    if (replicationDbPartitionSize % threadSize != 0) {
      throw new IllegalArgumentException(
          String.format(
              "`replicationDbPartitionSize`(%d) should be a multiple of `replicationDbThreadSize`(%d)",
              replicationDbPartitionSize, threadSize));
    }
    this.replicationDbPartitionSize = replicationDbPartitionSize;
    this.threadSize = threadSize;
    this.fetchThreadSize = fetchThreadSize;
    this.executorService =
        Executors.newFixedThreadPool(
            threadSize,
            new ThreadFactoryBuilder()
                .setNameFormat("log-distributor-%d")
                .setUncaughtExceptionHandler(
                    (thread, e) -> logger.error("Got an uncaught exception. thread:{}", thread, e))
                .build());
    this.replicationTransactionRepository = replicationTransactionRepository;
    this.coordinatorStateRepository = coordinatorStateRepository;
    this.replicationRecordRepository = replicationRecordRepository;
    this.recordWriterQueue = recordWriterQueue;
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
      replicationRecordRepository.upsertWithNewValue(key, newValue);
    } catch (Exception e) {
      String message =
          String.format(
              "Failed to append the value. key:%s, txId:%s, newValue:%s",
              key, transactionId, newValue);
      throw new RuntimeException(message, e);
    }

    recordWriterQueue.add(key);
  }

  private void handleTransaction(Transaction transaction, Instant committedAt)
      throws ExecutionException {
    for (WrittenTuple writtenTuple : transaction.writtenTuples()) {
      handleWrittenTuple(transaction.transactionId(), writtenTuple, committedAt);
    }
    replicationTransactionRepository.delete(transaction);
  }

  private void fetchAndHandleTransactions(int partitionId) throws IOException, ExecutionException {
    for (Transaction transaction :
        replicationTransactionRepository.scan(partitionId, fetchThreadSize)) {
      Optional<CoordinatorState> coordinatorState =
          coordinatorStateRepository.getCommitted(transaction.transactionId());
      if (!coordinatorState.isPresent()) {
        continue;
      }
      handleTransaction(transaction, coordinatorState.get().txCommittedAt);
    }
  }

  public DistributorThread run() {
    for (int i = 0; i < threadSize; i++) {
      int startPartitionId = i;
      executorService.execute(
          () -> {
            while (!executorService.isShutdown()) {
              for (int partitionId = startPartitionId;
                  partitionId < replicationDbPartitionSize;
                  partitionId += threadSize) {
                try {
                  fetchAndHandleTransactions(partitionId);
                } catch (Throwable e) {
                  logger.error("Unexpected exception occurred", e);
                  try {
                    TimeUnit.MILLISECONDS.sleep(100);
                  } catch (InterruptedException ex) {
                    logger.error("Interrupted", ex);
                    Thread.currentThread().interrupt();
                    break;
                  }
                }
              }
            }
          });
    }
    return this;
  }

  @Override
  public void close() {
    executorService.shutdown();
    // TODO: Make this configurable
    try {
      if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.warn("Interrupted", e);
    }
  }

  public static void main(String[] args) {
    // FIXME: This is only for PoC.
    Properties replicationDbProps = new Properties();
    replicationDbProps.put("scalar.db.storage", "jdbc");
    replicationDbProps.put(
        "scalar.db.contact_points", "jdbc:postgresql://localhost:5433/replication");
    replicationDbProps.put("scalar.db.username", "postgres");
    replicationDbProps.put("scalar.db.password", "postgres");

    Properties coordinatorDbProps = new Properties();
    coordinatorDbProps.put("scalar.db.storage", "jdbc");
    coordinatorDbProps.put("scalar.db.contact_points", "jdbc:postgresql://localhost/scalardb");
    coordinatorDbProps.put("scalar.db.username", "postgres");
    coordinatorDbProps.put("scalar.db.password", "postgres");

    Properties backupScalarDbProps = new Properties();
    backupScalarDbProps.put("scalar.db.storage", "jdbc");
    backupScalarDbProps.put("scalar.db.contact_points", "jdbc:postgresql://localhost/backup");
    backupScalarDbProps.put("scalar.db.username", "postgres");
    backupScalarDbProps.put("scalar.db.password", "postgres");

    ObjectMapper objectMapper = new ObjectMapper();

    CoordinatorStateRepository coordinatorStateRepository =
        new CoordinatorStateRepository(
            StorageFactory.create(coordinatorDbProps).getStorage(), "coordinator", "state");

    ReplicationTransactionRepository replicationTransactionRepository =
        new ReplicationTransactionRepository(
            StorageFactory.create(replicationDbProps).getStorage(),
            objectMapper,
            "replication",
            "transactions");

    ReplicationRecordRepository replicationRecordRepository =
        new ReplicationRecordRepository(
            StorageFactory.create(replicationDbProps).getStorage(),
            objectMapper,
            "replication",
            "records");

    RecordWriterThread recordWriter =
        new RecordWriterThread(16, replicationRecordRepository, backupScalarDbProps).run();

    DistributorThread distributorThread =
        new DistributorThread(
                256,
                16,
                16,
                coordinatorStateRepository,
                replicationTransactionRepository,
                replicationRecordRepository,
                recordWriter.queue())
            .run();

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  distributorThread.close();
                  recordWriter.close();
                }));
  }
}
