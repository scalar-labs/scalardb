package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationRecordRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationRecordRepository.ScanResult;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordScanWorker {
  private static final Logger logger = LoggerFactory.getLogger(RecordScanWorker.class);

  private final ExecutorService executorService;
  private final Configuration conf;
  private final ReplicationRecordRepository replicationRecordRepository;
  private final InMemoryQueue<Key> queue;
  private final MetricsLogger metricsLogger;

  @Immutable
  public static class Configuration {
    private final int fetchSize;
    private final int thresholdMillis;

    public Configuration(int fetchSize, int thresholdMillis) {
      this.fetchSize = fetchSize;
      this.thresholdMillis = thresholdMillis;
    }
  }

  public RecordScanWorker(
      Configuration conf,
      ReplicationRecordRepository replicationRecordRepository,
      InMemoryQueue<Key> queue,
      MetricsLogger metricsLogger) {

    this.conf = conf;
    this.replicationRecordRepository = replicationRecordRepository;
    this.executorService =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("record-scanner-%d")
                .setUncaughtExceptionHandler(
                    (thread, e) -> logger.error("Got an uncaught exception. thread:{}", thread, e))
                .build());
    this.queue = queue;
    this.metricsLogger = metricsLogger;
  }

  public void run() {
    executorService.execute(
        () -> {
          Long nextOldestAppendedAt = null;
          while (true) {
            ScanResult result;
            try {
              long paramNextOldestAppendedAt;
              if (nextOldestAppendedAt == null) {
                paramNextOldestAppendedAt = System.currentTimeMillis() - conf.thresholdMillis;
              } else {
                paramNextOldestAppendedAt = nextOldestAppendedAt;
              }
              result = replicationRecordRepository.scan(conf.fetchSize, paramNextOldestAppendedAt);
            } catch (Exception e) {
              logger.error("Failed to scan records", e);
              Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(200));
              continue;
            }

            nextOldestAppendedAt = result.oldestAppendedAtMillis;
            if (nextOldestAppendedAt == null) {
              Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(1000));
              continue;
            }

            for (Key key : result.keys) {
              metricsLogger.incrementDequeueFromUpdatedRecordQueue();
              try {
                queue.enqueue(key.hashCode(), key);
              } catch (Exception e) {
                logger.error("Failed to handle a record. Key:{}", key, e);
                Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(200));
              }
            }
          }
        });
  }
}
