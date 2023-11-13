package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit;

import com.google.common.base.MoreObjects;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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

public class GroupCommitter<K, V> {
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

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("value", value).add("future", future).toString();
    }
  }

  // TODO: Rename to BufferedValues?
  static class FetchedValues<K, V> {
    public final K key;
    public final List<ValueAndFuture<V>> values = new ArrayList<>();
    public final Long createdAtInMilli;

    public FetchedValues(K key, Long createdAtInMilli) {
      this.key = key;
      this.createdAtInMilli = createdAtInMilli;
    }
  }

  public GroupCommitter(
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
                .setNameFormat(label + "-group-commit-expire-%d")
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
              logger.info(
                  "Emitting (thread_id:{}, num_of_values:{})",
                  Thread.currentThread().getId(),
                  fetchedValues.values.size());
              emitter.accept(
                  fetchedValues.values.stream().map(vf -> vf.value).collect(Collectors.toList()));
              logger.info(
                  "Emitted (thread_id:{}, num_of_values:{})",
                  Thread.currentThread().getId(),
                  fetchedValues.values.size());
              fetchedValues.values.forEach(vf -> vf.future.complete(null));
              logger.info(
                  "Notified (thread_id:{}, num_of_values:{})",
                  Thread.currentThread().getId(),
                  fetchedValues.values.size());
            } catch (Throwable e) {
              fetchedValues.values.forEach(vf -> vf.future.completeExceptionally(e));
            }
          });
    }
  }

  private void handleItem(Itemable<K, V> itemable) {
    if (itemable instanceof Item) {
      Item<K, V> item = (Item<K, V>) itemable;
      FetchedValues<K, V> fetchedValues =
          currentFetchedItems.updateAndGet(
              current -> {
                if (current == null) {
                  return new FetchedValues<>(item.keyCandidate, System.currentTimeMillis());
                }
                return current;
              });
      // All values in a queue need to use the same unique key for partition key.
      V value;
      try {
        value = item.valueGenerator.apply(fetchedValues.key);
      } catch (Throwable e) {
        logger.error("Failed to prepare an item for group commit. item: {}", item, e);
        item.future.completeExceptionally(e);
        List<ValueAndFuture<V>> values = currentFetchedItems.getAndSet(null).values;
        if (!values.isEmpty()) {
          GroupCommitCascadeException gcce =
              new GroupCommitCascadeException(
                  "One of the fetched items failed in group commit. The other items have the same key associated with the failure. All the items will fail",
                  e);
          fetchedValues.values.forEach(vf -> vf.future.completeExceptionally(gcce));
        }
        return;
      }
      fetchedValues.values.add(new ValueAndFuture<>(value, item.future));
      emitFetchedItemsIfNeeded(fetchedValues);
    } else if (itemable instanceof WakeupItem) {
      FetchedValues<K, V> fetchedValues = currentFetchedItems.get();
      if (fetchedValues != null) {
        // FIXME: Wrap an exception
        emitFetchedItemsIfNeeded(fetchedValues);
      }
    } else {
      logger.error("Fetched an unexpected item. Skipping " + itemable);
    }
  }

  private void startDequeueExecutorService() {
    dequeueExecutorService.submit(
        () -> {
          while (true) {
            try {
              handleItem(queue.take());
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

  public void addValue(K keyCandidate, Function<K, V> valueGeneratorFromUniqueKey)
      throws GroupCommitException, GroupCommitCascadeException {
    CompletableFuture<Void> future = new CompletableFuture<>();
    queue.add(new Item<>(keyCandidate, valueGeneratorFromUniqueKey, future));
    try {
      long start = System.currentTimeMillis();
      logger.info("Wait start(thread_id:{})", Thread.currentThread().getId());
      future.get();
      logger.info(
          "Wait end(thread_id:{}): {} ms",
          Thread.currentThread().getId(),
          System.currentTimeMillis() - start);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof GroupCommitCascadeException) {
        throw (GroupCommitCascadeException) e.getCause();
      } else if (e.getCause() instanceof GroupCommitException) {
        throw (GroupCommitException) e.getCause();
      }
      throw new GroupCommitException("Failed to group-commit", e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted", e);
    }
  }

  void emitIfExpired() {
    FetchedValues<K, V> fetchedValues = currentFetchedItems.get();
    if (fetchedValues != null
        && !fetchedValues.values.isEmpty()
        && fetchedValues.createdAtInMilli + retentionTimeInMillis < System.currentTimeMillis()) {
      queue.add(new WakeupItem<>());
    }
  }
}
