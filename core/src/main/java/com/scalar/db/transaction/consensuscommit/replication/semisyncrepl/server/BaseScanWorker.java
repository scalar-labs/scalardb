package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseScanWorker {
  private static final Logger logger = LoggerFactory.getLogger(BaseScanWorker.class);

  private final ExecutorService executorService;
  private final Configuration conf;
  private final MetricsLogger metricsLogger;

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

  public BaseScanWorker(Configuration conf, String label, MetricsLogger metricsLogger) {
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
                .setNameFormat(String.format("%s-scan", label) + "-%d")
                .setUncaughtExceptionHandler(
                    (thread, e) -> logger.error("Got an uncaught exception. thread:{}", thread, e))
                .build());
    this.metricsLogger = metricsLogger;
  }

  // TODO: BaseScanWorker should manage until which record is scanned for each partition, so that
  //       ScanWorkers can resume the scan from the last scanned point.
  protected abstract boolean handle(int partitionId) throws Exception;

  public void run() {
    for (int i = 0; i < conf.threadSize; i++) {
      int startPartitionId = i;
      executorService.execute(
          () -> {
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
                if (partitionIdHavingMoreEnntries != null
                    && partitionIdHavingMoreEnntries == partitionId) {
                  partitionIdHavingMoreEnntries = null;
                }

                try {
                  if (handle(partitionId)) {
                    // Fetched the maximum size entries. Enter no-wait mode.
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
          });
    }
  }
}
