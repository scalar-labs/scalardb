package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.client;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GroupCommitter<K, V> {
  private static final Logger logger = LoggerFactory.getLogger(GroupCommitter.class);
  private final long retentionTimeInMillis;
  private final int numberOfRetentionValues;
  private Queue<K, V> queue;
  private final ScheduledExecutorService expirationCheckExecutorService;
  private final Consumer<List<V>> emitter;

  private static class Queue<K, V> {
    final K uniqueKey;
    final CountDownLatch countDownLatch = new CountDownLatch(1);
    final Instant createdAt = Instant.now();
    final List<V> values = new CopyOnWriteArrayList<>();

    public Queue(K uniqueId) {
      this.uniqueKey = uniqueId;
    }

    synchronized void add(V value) {
      values.add(value);
    }
  }

  GroupCommitter(
      String label,
      long retentionTimeInMillis,
      int numberOfRetentionValues,
      long expirationCheckIntervalInMillis,
      int numberOfThreads,
      Consumer<List<V>> emitter) {
    this.retentionTimeInMillis = retentionTimeInMillis;
    this.numberOfRetentionValues = numberOfRetentionValues;
    this.emitter = emitter;

    this.expirationCheckExecutorService =
        Executors.newScheduledThreadPool(
            1,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(label + "-group-commit-watch-%d")
                .build());

    expirationCheckExecutorService.scheduleAtFixedRate(
        this::emitIfExpired,
        expirationCheckIntervalInMillis,
        expirationCheckIntervalInMillis,
        TimeUnit.MILLISECONDS);
  }

  // TODO: Remove
  private final AtomicInteger inFlightCount = new AtomicInteger();

  private void emit() {
    Queue<K, V> currentQueue;
    synchronized (this) {
      if (queue == null) {
        return;
      }
      currentQueue = queue;
      queue = null;
    }

    long start = System.currentTimeMillis();
    inFlightCount.incrementAndGet();
    emitter.accept(currentQueue.values);
    currentQueue.countDownLatch.countDown();
    logger.info(
        "Emitted(thread_id:{}, in-flight_count:{}, queued_size:{}): {} ms",
        Thread.currentThread().getId(),
        inFlightCount.getAndDecrement(),
        currentQueue.values.size(),
        System.currentTimeMillis() - start);
  }

  synchronized Queue<K, V> initQueue(K uniqueKeyCandidate) {
    if (queue == null) {
      queue = new Queue<>(uniqueKeyCandidate);
    }
    return queue;
  }

  CountDownLatch addValue(K uniqueKeyCandidate, Function<K, V> valueGeneratorFromUniqueKey) {
    // All values in a queue need to use the same unique key for partition key
    Queue<K, V> currentQueue = initQueue(uniqueKeyCandidate);
    V value = valueGeneratorFromUniqueKey.apply(currentQueue.uniqueKey);
    currentQueue.add(value);

    // Store current CountDownLatch before `queue` is set to null in `emit()`
    CountDownLatch countDownLatch = currentQueue.countDownLatch;
    if (currentQueue.values.size() >= numberOfRetentionValues) {
      emit();
    }
    return countDownLatch;
  }

  void emitIfExpired() {
    Queue<K, V> currentQueue;
    synchronized (this) {
      if (queue == null) {
        return;
      }
      currentQueue = queue;
    }

    if (Instant.now().isAfter(currentQueue.createdAt.plusMillis(retentionTimeInMillis))) {
      emit();
    }
  }
}
