package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.google.common.base.MoreObjects;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Transaction;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionHandleWorker {
  private static final Logger logger = LoggerFactory.getLogger(TransactionHandleWorker.class);

  private final TransactionHandler transactionHandler;
  private final ExecutorService transactionHandlerExecutorService;
  private final ExecutorService recordHandlerExecutorService;
  private final Configuration conf;
  private final MetricsLogger metricsLogger;

  @Immutable
  public static class Configuration {
    final int recordHandlerThreadSize;
    final int transactionHandlerThreadSize;
    final int waitMillisPerPartition;

    public Configuration(
        int transactionHandlerThreadSize, int recordHandlerThreadSize, int waitMillisPerPartition) {
      this.recordHandlerThreadSize = recordHandlerThreadSize;
      this.transactionHandlerThreadSize = transactionHandlerThreadSize;
      this.waitMillisPerPartition = waitMillisPerPartition;
    }
  }

  public TransactionHandleWorker(
      Configuration conf, TransactionHandler transactionHandler, MetricsLogger metricsLogger) {

    this.conf = conf;
    this.transactionHandler = transactionHandler;
    this.transactionHandlerExecutorService =
        Executors.newFixedThreadPool(
            conf.transactionHandlerThreadSize,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("txn-handler-%d")
                .setUncaughtExceptionHandler(
                    (thread, e) -> logger.error("Got an uncaught exception. thread:{}", thread, e))
                .build());
    this.recordHandlerExecutorService =
        Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("record-handler-%d")
                .setUncaughtExceptionHandler(
                    (thread, e) -> logger.error("Got an uncaught exception. thread:{}", thread, e))
                .build());
    this.metricsLogger = metricsLogger;
  }

  private void handleTransactionWithRetry(Transaction transaction) {
    while (true) {
      try {
        if (transactionHandler.handleTransaction(recordHandlerExecutorService, transaction)) {
          metricsLogger.incrementHandleTransaction();
          return;
        }
      } catch (InterruptedException e) {
        // TODO: Error handling.
        Thread.currentThread().interrupt();
        metricsLogger.incrementExceptionCount();
        throw new RuntimeException(e);
      } catch (Exception e) {
        metricsLogger.incrementExceptionCount();
        logger.error("Failed to handle a dequeued Transaction: {}", transaction, e);
      }
      metricsLogger.incrementRetryTransaction();
      Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(200));
    }
  }

  public void enqueue(Transaction transaction) {
    transactionHandlerExecutorService.execute(() -> handleTransactionWithRetry(transaction));
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("transactionHandlerExecutorService", transactionHandlerExecutorService)
        .add("recordHandlerExecutorService", recordHandlerExecutorService)
        .toString();
  }

  public String toJson() {
    return String.format(
        "{\"Thread\":{\"Txn\":{\"QueueLength\":%d, \"Active\":%d}, \"Record\":{\"QueueLength\":%d, \"Active\":%d}}}",
        ((ThreadPoolExecutor) transactionHandlerExecutorService).getQueue().size(),
        ((ThreadPoolExecutor) transactionHandlerExecutorService).getActiveCount(),
        ((ThreadPoolExecutor) recordHandlerExecutorService).getQueue().size(),
        ((ThreadPoolExecutor) recordHandlerExecutorService).getActiveCount());
  }
}
