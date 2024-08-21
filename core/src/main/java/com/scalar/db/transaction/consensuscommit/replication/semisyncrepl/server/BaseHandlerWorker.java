package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.exception.storage.ExecutionException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseHandlerWorker<T> {
  private static final Logger logger = LoggerFactory.getLogger(BaseHandlerWorker.class);

  private final ExecutorService executorService;
  private final Configuration conf;
  private final MetricsLogger metricsLogger;
  @Nullable private final List<BlockingQueue<T>> queuesToSupply;
  @Nullable private final List<BlockingQueue<T>> queuesToConsume;

  @Immutable
  public static class Configuration {
    final int replicationDbPartitionSize;
    final int threadSize;
    final int waitMillisPerPartition;

    public Configuration(
        int replicationDbPartitionSize, int threadSize, int waitMillisPerPartition) {
      this.replicationDbPartitionSize = replicationDbPartitionSize;
      this.threadSize = threadSize;
      this.waitMillisPerPartition = waitMillisPerPartition;
    }
  }

  public BaseHandlerWorker(
      Configuration conf,
      String label,
      MetricsLogger metricsLogger,
      @Nullable List<BlockingQueue<T>> queuesToSupply,
      @Nullable List<BlockingQueue<T>> queuesToConsume) {

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
                .setDaemon(true)
                .setNameFormat(String.format("log-distributor-%s", label) + "-%d")
                .setUncaughtExceptionHandler(
                    (thread, e) -> logger.error("Got an uncaught exception. thread:{}", thread, e))
                .build());
    this.metricsLogger = metricsLogger;

    this.queuesToSupply = queuesToSupply;
    this.queuesToConsume = queuesToConsume;

    if (this.queuesToConsume != null) {
      if (this.queuesToConsume.size() != conf.threadSize) {
        throw new IllegalArgumentException(
            String.format(
                "The size of queues must be the same as the size of the threads. Queue Size: %d, Config: %s",
                this.queuesToConsume.size(), conf));
      }
      ExecutorService executorServiceForInMemoryQueue =
          Executors.newFixedThreadPool(
              conf.threadSize,
              new ThreadFactoryBuilder()
                  .setDaemon(true)
                  .setNameFormat(String.format("log-distributor-queue-consume-%s", label) + "-%d")
                  .setUncaughtExceptionHandler(
                      (thread, e) ->
                          logger.error("Got an uncaught exception. thread:{}", thread, e))
                  .build());
      for (int i = 0; i < conf.threadSize; i++) {
        BlockingQueue<T> queueToConsume = getQueueToConsume(i);
        executorServiceForInMemoryQueue.execute(
            () -> {
              while (true) {
                T dequeuedItem = null;
                try {
                  dequeuedItem = queueToConsume.take();
                  if (handleQueuedItem(dequeuedItem)) {
                    queueToConsume.add(dequeuedItem);
                  }
                } catch (InterruptedException e) {
                  // TODO: Error handling.
                  Thread.currentThread().interrupt();
                  throw new RuntimeException(e);
                } catch (Exception e) {
                  logger.error("Failed to handle a dequeued item. Item: {}", dequeuedItem, e);
                  if (dequeuedItem != null) {
                    queueToConsume.add(dequeuedItem);
                  }
                  Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(200));
                }
              }
            });
      }
    }
  }

  @Nonnull
  protected BlockingQueue<T> getQueueToSupply(int partitionId) {
    assert queuesToSupply != null;
    return queuesToSupply.get(Math.abs(partitionId) % queuesToSupply.size());
  }

  @Nonnull
  protected BlockingQueue<T> getQueueToConsume(int threadIndex) {
    assert queuesToConsume != null;
    return queuesToConsume.get(threadIndex);
  }

  protected boolean handleQueuedItem(T item) throws Exception {
    throw new UnsupportedOperationException();
  }

  private void handlePartitions(int startPartitionId) {
    // Each worker thread handles some partition IDs that are calculated based on:
    // - The total partition size
    // - The number of threads
    // - `startPartitionId`
    //
    // For instance, assuming there are 64 threads, the number of total partitions is 256
    // and `startPartitionId` is 10, the thread is responsible for partition ID: 10, 74,
    // 138, 202.
    //
    // Each thread waits only if all the partitions the thread manages don't have next
    // entries to process.
    //
    // Let's say the following situation:
    // - The thread handles two partitions (ID: 10 and 74) sequentially. The thread fetches
    // entries from
    //   them as same as the maximum fetch size. The thread thinks they may have more
    // entries and decides not to wait until handling them again.
    // - The thread handles the other two partitions (ID: 138 and 202). The thread fetches
    // entries from them less
    //   than the maximum fetch size. The thread thinks they may not have more entries and
    // decides. But it doesn't wait until handling partition ID: 10 and 74.
    // - The thread handles the two partitions (ID: 10 and 74) again. The thread fetches
    // entries from them less
    //   than the maximum fetch size. The thread thinks all the partitions may not have more
    // entries and decides. It decides to wait until fetching the maximum size entries.
    //
    Integer partitionIdHavingMoreEnntries = null;
    while (!executorService.isShutdown()) {
      for (int partitionId = startPartitionId;
          partitionId < conf.replicationDbPartitionSize;
          partitionId += conf.threadSize) {
        // Quit no-wait mode if it reaches the target partition ID again.
        if (partitionIdHavingMoreEnntries != null && partitionIdHavingMoreEnntries == partitionId) {
          partitionIdHavingMoreEnntries = null;
        }

        try {
          if (handle(partitionId)) {
            // Fetched the maximum size entries or immediate retry is needed. Enter no-wait
            // mode.
            partitionIdHavingMoreEnntries = partitionId;
          }
        } catch (Throwable e) {
          metricsLogger.incrementExceptionCount();
          logger.error("Unexpected exception occurred", e);
        } finally {
          try {
            // Wait only unless in no-wait mode.
            if (partitionIdHavingMoreEnntries == null) {
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
  }

  protected abstract boolean handle(int partitionId) throws ExecutionException;

  public void run() {
    for (int i = 0; i < conf.threadSize; i++) {
      int startPartitionId = i;
      executorService.execute(() -> handlePartitions(startPartitionId));
    }
  }
}
