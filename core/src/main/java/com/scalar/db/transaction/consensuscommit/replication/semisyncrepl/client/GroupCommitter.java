package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.client;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

class GroupCommitter<K, V> {
  private final long retentionTimeInMillis;
  private final int numberOfRetentionValues;
  private Queue<K, V> queue;
  private final ExecutorService emitterExecutorService;
  private final ScheduledExecutorService expirationCheckExecutorService;
  private final Consumer<List<V>> emitter;

  private static class Queue<K, V> {
    final K uniqueKey;
    final CountDownLatch countDownLatch = new CountDownLatch(1);
    final Instant createdAt = Instant.now();
    final List<V> values = new ArrayList<>();

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
    this.emitterExecutorService =
        Executors.newFixedThreadPool(
            numberOfThreads,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(label + "-group-commit-%d")
                .build());

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

  private synchronized void emit() {
    List<V> values = queue.values;
    CountDownLatch countDownLatch = queue.countDownLatch;
    emitterExecutorService.execute(
        () -> {
          emitter.accept(values);
          countDownLatch.countDown();
        });
    queue = null;
  }

  synchronized CountDownLatch addValue(
      K uniqueKeyCandidate, Function<K, V> valueGeneratorFromUniqueKey) {
    // All values in a queue need to use the same unique key for partition key
    V value;
    if (queue == null) {
      queue = new Queue<>(uniqueKeyCandidate);
    }
    value = valueGeneratorFromUniqueKey.apply(queue.uniqueKey);
    queue.add(value);

    // Store current CountDownLatch before `queue` is set to null in `emit()`
    CountDownLatch countDownLatch = queue.countDownLatch;
    if (queue.values.size() >= numberOfRetentionValues) {
      emit();
    }
    return countDownLatch;
  }

  synchronized void emitIfExpired() {
    if (queue == null) {
      return;
    }

    if (Instant.now().isAfter(queue.createdAt.plusMillis(retentionTimeInMillis))) {
      emit();
    }
  }
}
