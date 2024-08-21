package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.UpdatedRecord;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordHandlerInMemoryQueueConsumer {
  private static final Logger logger =
      LoggerFactory.getLogger(RecordHandlerInMemoryQueueConsumer.class);

  private final RecordHandler recordHandler;
  private final ExecutorService executorService;
  private final Configuration conf;
  private final List<BlockingQueue<UpdatedRecord>> queues;

  @Immutable
  public static class Configuration {
    final int threadSize;
    final int waitMillisPerPartition;

    public Configuration(int threadSize, int waitMillisPerPartition) {
      this.threadSize = threadSize;
      this.waitMillisPerPartition = waitMillisPerPartition;
    }
  }

  public RecordHandlerInMemoryQueueConsumer(
      Configuration conf, RecordHandler recordHandler, List<BlockingQueue<UpdatedRecord>> queues) {

    if (queues.size() != conf.threadSize) {
      throw new IllegalArgumentException(
          String.format(
              "The size of the queues (%d) should be same as the size of threads (%d)",
              queues.size(), conf.threadSize));
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

    this.queues = queues;
  }

  protected BlockingQueue<UpdatedRecord> getQueue(int threadIndex) {
    return queues.get(threadIndex);
  }

  public void run() {
    for (int i = 0; i < conf.threadSize; i++) {
      BlockingQueue<UpdatedRecord> queueToConsume = getQueue(i);
      executorService.execute(
          () -> {
            while (true) {
              UpdatedRecord updatedRecord = null;
              try {
                updatedRecord = queueToConsume.take();
                if (recordHandler.handleUpdatedRecord(updatedRecord)) {
                  queueToConsume.add(updatedRecord);
                }
              } catch (InterruptedException e) {
                // TODO: Error handling.
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
              } catch (Exception e) {
                logger.error("Failed to handle a dequeued UpdatedRecord: {}", updatedRecord, e);
                if (updatedRecord != null) {
                  queueToConsume.add(updatedRecord);
                }
                Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(200));
              }
            }
          });
    }
  }
}
