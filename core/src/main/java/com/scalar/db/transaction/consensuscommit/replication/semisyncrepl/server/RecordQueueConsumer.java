package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.UpdatedRecord;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordQueueConsumer {
  private static final Logger logger = LoggerFactory.getLogger(RecordQueueConsumer.class);

  private final RecordHandler recordHandler;
  private final ExecutorService executorService;
  private final Configuration conf;
  private final InMemoryQueue<UpdatedRecord> queue;
  private final MetricsLogger metricsLogger;

  @Immutable
  public static class Configuration {
    final int threadSize;
    final int waitMillisPerPartition;

    public Configuration(int threadSize, int waitMillisPerPartition) {
      this.threadSize = threadSize;
      this.waitMillisPerPartition = waitMillisPerPartition;
    }
  }

  public RecordQueueConsumer(
      Configuration conf,
      RecordHandler recordHandler,
      InMemoryQueue<UpdatedRecord> queue,
      MetricsLogger metricsLogger) {

    if (queue.size() != conf.threadSize) {
      throw new IllegalArgumentException(
          String.format(
              "The size of the queues (%d) should be same as the size of threads (%d)",
              queue.size(), conf.threadSize));
    }
    this.conf = conf;
    this.recordHandler = recordHandler;

    this.executorService =
        Executors.newFixedThreadPool(
            conf.threadSize,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("log-distributor-record-queue-consume-%d")
                .setUncaughtExceptionHandler(
                    (thread, e) -> logger.error("Got an uncaught exception. thread:{}", thread, e))
                .build());

    this.queue = queue;
    this.metricsLogger = metricsLogger;
  }

  public void run() {
    for (int i = 0; i < conf.threadSize; i++) {
      int threadId = i;
      executorService.execute(
          () -> {
            while (true) {
              UpdatedRecord updatedRecord = null;
              try {
                metricsLogger.incrementDequeueFromUpdatedRecordQueue();
                updatedRecord = queue.dequeue(threadId);
                if (!recordHandler.handleUpdatedRecord(updatedRecord)) {
                  metricsLogger.incrementReEnqueueFromUpdatedRecordQueue();
                  queue.enqueue(threadId, updatedRecord);
                }
              } catch (InterruptedException e) {
                // TODO: Error handling.
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
              } catch (Exception e) {
                logger.error("Failed to handle a dequeued UpdatedRecord: {}", updatedRecord, e);
                if (updatedRecord != null) {
                  metricsLogger.incrementReEnqueueFromUpdatedRecordQueue();
                  queue.enqueue(threadId, updatedRecord);
                }
                Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(200));
              }
            }
          });
    }
  }
}
