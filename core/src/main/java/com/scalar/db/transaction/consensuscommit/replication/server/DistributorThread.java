package com.scalar.db.transaction.consensuscommit.replication.server;

import com.google.common.base.MoreObjects;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
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
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributorThread implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(DistributorThread.class);

  private final ExecutorService executorService;
  private final int replicationDbPartitionSize;
  private final int threadSize;
  private final int fetchThreadSize;
  private final int waitMillisPerPartition;
  private final ReplicationRecordRepository replicationRecordRepository;
  private final ReplicationTransactionRepository replicationTransactionRepository;
  private final CoordinatorStateRepository coordinatorStateRepository;
  private final Queue<Key> recordWriterQueue;

  private final MetricsLogger metricsLogger = new MetricsLogger();

  private static class Metrics {
    public final AtomicInteger scanCount = new AtomicInteger();
    public final AtomicInteger scannedTransactions = new AtomicInteger();
    public final AtomicInteger uncommittedTransactions = new AtomicInteger();
    public final AtomicInteger handledCommittedTransactions = new AtomicInteger();
    public final AtomicInteger exceptionCount = new AtomicInteger();

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("scans", scanCount)
          .add("scannedTxns", scannedTransactions)
          .add("uncommittedTxns", uncommittedTransactions)
          .add("handledTxns", handledCommittedTransactions)
          .add("exceptions", exceptionCount)
          .toString();
    }
  }

  private static class MetricsLogger {
    private final boolean isEnabled;
    private final Map<Instant, Metrics> metricsMap = new ConcurrentHashMap<>();
    private final AtomicReference<Instant> keyHolder = new AtomicReference<>();

    public MetricsLogger() {
      String metricsEnabled = System.getenv("LOG_APPLIER_METRICS_ENABLED");
      this.isEnabled = metricsEnabled != null && metricsEnabled.equalsIgnoreCase("true");
    }

    private Instant currentTimestampRoundedInSeconds() {
      return Instant.ofEpochSecond(System.currentTimeMillis() / 1000);
    }

    private void withPrintAndCleanup(Consumer<Metrics> consumer) {
      Instant currentKey = currentTimestampRoundedInSeconds();
      Instant oldKey = keyHolder.getAndSet(currentKey);
      Metrics metrics = metricsMap.computeIfAbsent(currentKey, k -> new Metrics());
      consumer.accept(metrics);
      if (oldKey == null) {
        return;
      }
      if (!oldKey.equals(currentKey)) {
        // logger.info("[{}] {}", currentKey, metricsMap.get(oldKey));
        System.out.printf("[%s] %s\n", currentKey, metricsMap.get(oldKey));
      }
    }

    public void incrementScanCount() {
      if (!isEnabled) {
        return;
      }
      withPrintAndCleanup(metrics -> metrics.scanCount.incrementAndGet());
    }

    public void incrementScannedTransactions() {
      if (!isEnabled) {
        return;
      }
      withPrintAndCleanup(metrics -> metrics.scannedTransactions.incrementAndGet());
    }

    public void incrementHandledCommittedTransactions() {
      if (!isEnabled) {
        return;
      }
      withPrintAndCleanup(metrics -> metrics.handledCommittedTransactions.incrementAndGet());
    }

    public void incrementUncommittedTransactions() {
      if (!isEnabled) {
        return;
      }
      withPrintAndCleanup(metrics -> metrics.uncommittedTransactions.incrementAndGet());
    }

    public void incrementExceptionCount() {
      if (!isEnabled) {
        return;
      }
      withPrintAndCleanup(metrics -> metrics.exceptionCount.incrementAndGet());
    }
  }

  public DistributorThread(
      int replicationDbPartitionSize,
      int threadSize,
      int fetchThreadSize,
      int waitMillisPerPartition,
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
    this.waitMillisPerPartition = waitMillisPerPartition;
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
    for (WrittenTuple writtenTuple : transaction.writtenTuples) {
      handleWrittenTuple(transaction.transactionId, writtenTuple, committedAt);
    }
    replicationTransactionRepository.delete(transaction);
    metricsLogger.incrementHandledCommittedTransactions();
  }

  private void fetchAndHandleTransactions(int partitionId) throws IOException, ExecutionException {
    metricsLogger.incrementScanCount();
    for (Transaction transaction :
        replicationTransactionRepository.scan(partitionId, fetchThreadSize)) {
      metricsLogger.incrementScannedTransactions();
      Optional<CoordinatorState> coordinatorState =
          coordinatorStateRepository.getCommitted(transaction.transactionId);
      if (!coordinatorState.isPresent()) {
        metricsLogger.incrementUncommittedTransactions();
        // TODO: Update `updated_at` if it's enough old
        continue;
      }
      if (coordinatorState.get().txState != TransactionState.COMMITTED) {
        metricsLogger.incrementUncommittedTransactions();
        if (coordinatorState.get().txState == TransactionState.ABORTED) {
          replicationTransactionRepository.delete(transaction);
        }
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
                  metricsLogger.incrementExceptionCount();
                  logger.error("Unexpected exception occurred", e);
                } finally {
                  try {
                    if (waitMillisPerPartition > 0) {
                      TimeUnit.MILLISECONDS.sleep(waitMillisPerPartition);
                    }
                  } catch (InterruptedException ex) {
                    logger.error("Interrupted", ex);
                    Thread.currentThread().interrupt();
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
}
