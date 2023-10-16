package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.client;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GroupCommitter<K, V> {
  private static final Logger logger = LoggerFactory.getLogger(GroupCommitter.class);
  private final BlockingQueue<Itemable<K, V>> queue = new LinkedBlockingQueue<>();
  private final AtomicReference<FetchedValues<K, V>> currentFetchedItems = new AtomicReference<>();
  private final long retentionTimeInMillis;
  private final int numberOfRetentionValues;
  private final long expirationCheckIntervalInMillis;
  private final ExecutorService dequeueExecutorService;
  private final ExecutorService emitExecutorService;
  private final ScheduledExecutorService expirationCheckExecutorService;
  private final Consumer<List<V>> emitter;

  private interface Itemable<K, V> {}

  static class Item<K, V> implements Itemable<K, V> {
    public final K keyCandidate;
    public final Function<K, V> valueGenerator;
    public final CompletableFuture<Void> future;

    public Item(K keyCandidate, Function<K, V> valueGenerator, CompletableFuture<Void> future) {
      this.keyCandidate = keyCandidate;
      this.valueGenerator = valueGenerator;
      this.future = future;
    }
  }

  static class WakeupItem<K, V> implements Itemable<K, V> {}

  static class ValueAndFuture<V> {
    public final V value;
    public final CompletableFuture<Void> future;

    public ValueAndFuture(V value, CompletableFuture<Void> future) {
      this.value = value;
      this.future = future;
    }
  }

  static class FetchedValues<K, V> {
    public final K key;
    public final List<ValueAndFuture<V>> values = new ArrayList<>();
    public final Long createdAtInMilli;

    public FetchedValues(K key, Long createdAtInMilli) {
      this.key = key;
      this.createdAtInMilli = createdAtInMilli;
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
    this.expirationCheckIntervalInMillis = expirationCheckIntervalInMillis;
    this.emitter = emitter;

    this.dequeueExecutorService =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(label + "-group-commit-dequeue-%d")
                .build());

    this.emitExecutorService =
        Executors.newFixedThreadPool(
            numberOfThreads,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(label + "-group-commit-emit-%d")
                .build());

    this.expirationCheckExecutorService =
        Executors.newScheduledThreadPool(
            1,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(label + "-group-commit-watch-%d")
                .build());

    startDequeueExecutorService();
    startExpirationCheckExecutorService();
  }

  private void emitFetchedItemsIfNeeded(FetchedValues<K, V> fetchedValues) {
    if (fetchedValues.values.size() >= numberOfRetentionValues
        || fetchedValues.createdAtInMilli + retentionTimeInMillis < System.currentTimeMillis()) {
      currentFetchedItems.set(null);
      emitExecutorService.submit(
          () -> {
            try {
              emitter.accept(
                  fetchedValues.values.stream().map(vf -> vf.value).collect(Collectors.toList()));
              fetchedValues.values.forEach(vf -> vf.future.complete(null));
            } catch (Throwable e) {
              fetchedValues.values.forEach(vf -> vf.future.completeExceptionally(e));
            }
          });
    }
  }

  private void startDequeueExecutorService() {
    dequeueExecutorService.submit(
        () -> {
          while (true) {
            try {
              Itemable<K, V> itemable = queue.take();
              if (itemable instanceof Item) {
                Item<K, V> item = (Item<K, V>) itemable;
                FetchedValues<K, V> fetchedValues =
                    currentFetchedItems.updateAndGet(
                        current -> {
                          if (current == null) {
                            return new FetchedValues<>(
                                item.keyCandidate, System.currentTimeMillis());
                          }
                          return current;
                        });
                // All values in a queue need to use the same unique key for partition key.
                V value = item.valueGenerator.apply(fetchedValues.key);
                fetchedValues.values.add(new ValueAndFuture<>(value, item.future));
                emitFetchedItemsIfNeeded(fetchedValues);
              } else if (itemable instanceof WakeupItem) {
                FetchedValues<K, V> fetchedValues = currentFetchedItems.get();
                if (fetchedValues != null) {
                  emitFetchedItemsIfNeeded(fetchedValues);
                }
              } else {
                throw new AssertionError();
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              logger.warn("Interrupted", e);
              break;
            }
          }
        });
  }

  private void startExpirationCheckExecutorService() {
    expirationCheckExecutorService.scheduleAtFixedRate(
        this::emitIfExpired,
        expirationCheckIntervalInMillis,
        expirationCheckIntervalInMillis,
        TimeUnit.MILLISECONDS);
  }

  void addValue(
      K keyCandidate, Function<K, V> valueGeneratorFromUniqueKey, CompletableFuture<Void> future) {
    queue.add(new Item<>(keyCandidate, valueGeneratorFromUniqueKey, future));
  }

  void emitIfExpired() {
    FetchedValues<K, V> fetchedValues = currentFetchedItems.get();
    if (fetchedValues != null
        && fetchedValues.createdAtInMilli + retentionTimeInMillis < System.currentTimeMillis()) {
      queue.add(new WakeupItem<>());
    }
  }
}
