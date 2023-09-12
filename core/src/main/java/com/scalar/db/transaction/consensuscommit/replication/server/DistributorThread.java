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
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributorThread implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(DistributorThread.class);

  private final ExecutorService executorService;
  private final Configuration conf;
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

  @Immutable
  public static class Configuration {
    final int replicationDbPartitionSize;
    final int threadSize;
    final int fetchThreadSize;
    final int waitMillisPerPartition;
    // For potential aborted transactions which don't have a corresponding committed/aborted state
    final int thresholdMillisForOldTransaction;
    final int extraWaitMillisForOldTransaction;

    public Configuration(
        int replicationDbPartitionSize,
        int threadSize,
        int fetchThreadSize,
        int waitMillisPerPartition,
        int thresholdMillisForOldTransaction,
        int extraWaitMillisForOldTransaction) {
      this.replicationDbPartitionSize = replicationDbPartitionSize;
      this.threadSize = threadSize;
      this.fetchThreadSize = fetchThreadSize;
      this.waitMillisPerPartition = waitMillisPerPartition;
      this.thresholdMillisForOldTransaction = thresholdMillisForOldTransaction;
      this.extraWaitMillisForOldTransaction = extraWaitMillisForOldTransaction;
    }
  }

  public DistributorThread(
      Configuration conf,
      CoordinatorStateRepository coordinatorStateRepository,
      ReplicationTransactionRepository replicationTransactionRepository,
      ReplicationRecordRepository replicationRecordRepository,
      Queue<Key> recordWriterQueue) {
    if (conf.replicationDbPartitionSize % conf.threadSize != 0) {
      throw new IllegalArgumentException(
          String.format(
              "`replicationDbPartitionSize`(%d) should be a multiple of `replicationDbThreadSize`(%d)",
              conf.replicationDbPartitionSize, conf.threadSize));
    }
    this.conf = conf;

    this.executorService =
        Executors.newFixedThreadPool(
            conf.threadSize,
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

  private boolean fetchAndHandleTransactions(int partitionId)
      throws IOException, ExecutionException {
    metricsLogger.incrementScanCount();
    int fetchedCount = 0;
    for (Transaction transaction :
        replicationTransactionRepository.scan(partitionId, conf.fetchThreadSize)) {
      fetchedCount++;

      metricsLogger.incrementScannedTransactions();
      Optional<CoordinatorState> coordinatorState =
          coordinatorStateRepository.get(transaction.transactionId);
      if (!coordinatorState.isPresent()) {
        metricsLogger.incrementUncommittedTransactions();
        Instant now = Instant.now();
        if (transaction.updatedAt.isBefore(
            now.minusMillis(conf.thresholdMillisForOldTransaction))) {
          logger.info(
              "Updating an old transaction to be handled later. txId:{}",
              transaction.transactionId);
          replicationTransactionRepository.updateUpdatedAt(transaction);
        }
        continue;
      }
      if (coordinatorState.get().txState != TransactionState.COMMITTED) {
        metricsLogger.incrementUncommittedTransactions();
        if (coordinatorState.get().txState == TransactionState.ABORTED) {
          replicationTransactionRepository.delete(transaction);
        }
        // FIXME: Should delete other state?
        continue;
      }
      handleTransaction(transaction, coordinatorState.get().txCommittedAt);
    }

    return fetchedCount >= conf.fetchThreadSize;
  }

  public DistributorThread run() {
    for (int i = 0; i < conf.threadSize; i++) {
      int startPartitionId = i;
      executorService.execute(
          () -> {
            // Skip waits until this partition ID.
            // For instance,
            // - there are 64 threads and the number of total partitions is 256
            // - this is the first thread and handles partition ID: 0, 64, 128, 192
            // - if full transactions are fetched when handling partition ID 64,
            //   the partition ID 64 is remembered
            // - subsequent waits are skipped until next partition ID 64
            Integer skipWaitsStartPartitionId = null;
            while (!executorService.isShutdown()) {
              for (int partitionId = startPartitionId;
                  partitionId < conf.replicationDbPartitionSize;
                  partitionId += conf.threadSize) {
                // Disable the skip waits mode if it reaches the target partition ID
                if (skipWaitsStartPartitionId != null && skipWaitsStartPartitionId == partitionId) {
                  skipWaitsStartPartitionId = null;
                }

                try {
                  if (fetchAndHandleTransactions(partitionId)) {
                    // Enable the skip waits mode
                    skipWaitsStartPartitionId = partitionId;
                  }
                } catch (Throwable e) {
                  metricsLogger.incrementExceptionCount();
                  logger.error("Unexpected exception occurred", e);
                } finally {
                  try {
                    // Wait only if the skip wait mode is disabled
                    if (skipWaitsStartPartitionId == null) {
                      if (conf.waitMillisPerPartition > 0) {
                        TimeUnit.MILLISECONDS.sleep(conf.waitMillisPerPartition);
                      }
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
