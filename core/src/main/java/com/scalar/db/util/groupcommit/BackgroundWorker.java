package com.scalar.db.util.groupcommit;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.Closeable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
abstract class BackgroundWorker<T> implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(BackgroundWorker.class);
  private final BlockingQueue<T> queue;
  private final ExecutorService executorService;
  private final long timeoutCheckIntervalMillis;
  private final RetryMode retryMode;

  enum RetryMode {
    KEEP_AT_HEAD,
    RE_ENQUEUE
  }

  BackgroundWorker(String threadName, long timeoutCheckIntervalMillis, RetryMode retryMode) {
    this.executorService =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat(threadName + "-%d").build());
    this.timeoutCheckIntervalMillis = timeoutCheckIntervalMillis;
    this.retryMode = retryMode;
    this.queue = createQueue();
    startExecutorService();
  }

  abstract BlockingQueue<T> createQueue();

  void add(T item) {
    queue.add(item);
  }

  int size() {
    return queue.size();
  }

  private void startExecutorService() {
    executorService.execute(
        () -> {
          while (!executorService.isShutdown()) {
            try {
              process();
            } catch (Exception e) {
              if (Thread.currentThread().isInterrupted()) {
                logger.warn("Interrupted");
                return;
              }

              logger.warn("Unexpected exception occurred. Retrying...", e);
              Uninterruptibles.sleepUninterruptibly(
                  timeoutCheckIntervalMillis, TimeUnit.MILLISECONDS);
            }
          }
        });
  }

  abstract boolean processItem(T item);

  private void process() {
    T item = queue.peek();

    if (item != null) {
      boolean shouldRemove = processItem(item);
      if (shouldRemove) {
        // Remove the handled item.
        T removed = queue.poll();
        // Check if the removed group is expected just in case.
        if (removed == null || !removed.equals(item)) {
          logger.error(
              "This removed item is unexpectedly different from the item checked before. This might be a bug. Retrying. Expected: {}, Actual: {}",
              item,
              removed);
          // Keep the unexpected fetched item by re-enqueuing it.
          if (removed != null) {
            queue.add(removed);
          }
        }
        // No wait is needed.
        return;
      }

      // Keep the dequeued item.

      if (retryMode == RetryMode.RE_ENQUEUE) {
        // Move the item to the tail if configured.
        T removed = queue.poll();
        // Check if the removed slot is expected just in case.
        if (removed == null || !removed.equals(item)) {
          logger.error(
              "The fetched item isn't same as the item checked before. Expected: {}, Actual: {}",
              item,
              removed);
        }
        // Re-enqueue the fetched.
        if (removed != null) {
          queue.add(removed);
        }
      }
    }

    // TODO: Current behavior to wait for a while if the first item isn't ready works well if
    //       `retryMode` is KEEP_AT_HEAD. But in case of MOVE_TO_TAIL, it's likely the second item
    //       and/or subsequent items are ready, and waiting when the first item isn't ready leads in
    //       slowdown to scan all items. Some performance improvement is needed.
    Uninterruptibles.sleepUninterruptibly(timeoutCheckIntervalMillis, TimeUnit.MILLISECONDS);
  }

  @Override
  public void close() {
    MoreExecutors.shutdownAndAwaitTermination(executorService, 10, TimeUnit.SECONDS);
  }
}
