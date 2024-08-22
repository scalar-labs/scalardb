package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Transaction;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: Create `BaseQueueConsumer` and inherit it.
public class TransactionQueueConsumer {
  private static final Logger logger = LoggerFactory.getLogger(TransactionQueueConsumer.class);

  private final TransactionHandler transactionHandler;
  private final ExecutorService executorService;
  private final Configuration conf;
  private final List<BlockingQueue<Transaction>> queues;

  @Immutable
  public static class Configuration {
    final int threadSize;
    final int waitMillisPerPartition;

    public Configuration(int threadSize, int waitMillisPerPartition) {
      this.threadSize = threadSize;
      this.waitMillisPerPartition = waitMillisPerPartition;
    }
  }

  public TransactionQueueConsumer(
      Configuration conf,
      TransactionHandler transactionHandler,
      List<BlockingQueue<Transaction>> queues) {

    if (queues.size() != conf.threadSize) {
      throw new IllegalArgumentException(
          String.format(
              "The size of the queues (%d) should be same as the size of threads (%d)",
              queues.size(), conf.threadSize));
    }
    this.conf = conf;
    this.transactionHandler = transactionHandler;
    this.executorService =
        Executors.newFixedThreadPool(
            conf.threadSize,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("log-distributor-transaction-queue-consume-%d")
                .setUncaughtExceptionHandler(
                    (thread, e) -> logger.error("Got an uncaught exception. thread:{}", thread, e))
                .build());
    this.queues = queues;
  }

  protected BlockingQueue<Transaction> getQueue(int threadIndex) {
    return queues.get(threadIndex);
  }

  public void run() {
    for (int i = 0; i < conf.threadSize; i++) {
      BlockingQueue<Transaction> queue = getQueue(i);
      executorService.execute(
          () -> {
            while (true) {
              Transaction transaction = null;
              try {
                transaction = queue.take();
                if (transactionHandler.handleTransaction(transaction)) {
                  queue.add(transaction);
                }
              } catch (InterruptedException e) {
                // TODO: Error handling.
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
              } catch (Exception e) {
                logger.error("Failed to handle a dequeued Transaction: {}", transaction, e);
                if (transaction != null) {
                  queue.add(transaction);
                }
                Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(200));
              }
            }
          });
    }
  }
}
