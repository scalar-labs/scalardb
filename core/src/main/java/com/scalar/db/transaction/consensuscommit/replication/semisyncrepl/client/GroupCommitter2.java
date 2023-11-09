package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.client;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroupCommitter2<K, V> {
  private static final Logger logger = LoggerFactory.getLogger(GroupCommitter2.class);
  private final BlockingQueue<BufferedValues<K, V>> queueOfBufferedValues =
      new LinkedBlockingQueue<>();
  private final long retentionTimeInMillis;
  private final int numberOfRetentionValues;
  private final long expirationCheckIntervalInMillis;
  private final ExecutorService emitExecutorService;
  private final ExecutorService expirationCheckExecutorService;
  private final Consumer<List<V>> emitter;
  @Nullable private BufferedValues<K, V> bufferedValues;

  private static class ValueSlot<K, V> {
    private final BufferedValues<K, V> parentBuffer;
    private final CompletableFuture<Void> completableFuture = new CompletableFuture<>();
    @Nullable private volatile V value;

    public ValueSlot(BufferedValues<K, V> parentBuffer) {
      this.parentBuffer = parentBuffer;
    }

    public K getKey() {
      return parentBuffer.key;
    }

    public void setValue(V value) {
      this.value = value;
      parentBuffer.notifyOfReadyValue(this);
    }

    public void waitUntilEmit() throws GroupCommitException, GroupCommitCascadeException {
      try {
        completableFuture.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new GroupCommitException("Group commit was interrupted", e);
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof GroupCommitCascadeException) {
          throw (GroupCommitCascadeException) cause;
        } else if (cause instanceof GroupCommitException) {
          throw (GroupCommitException) cause;
        }
        throw new GroupCommitException("Group commit failed", e);
      }
    }

    public void abort(Throwable e) {
      parentBuffer.abortAll(e);
    }
  }

  private static class BufferedValues<K, V> {
    private final ExecutorService executorService;
    private final Consumer<List<V>> emitter;
    private final int capacity;
    private Integer size;
    private final K key;
    public final Instant expiredAt;
    private final AtomicBoolean done = new AtomicBoolean();
    private final List<ValueSlot<K, V>> valueSlots;
    private final Set<ValueSlot<K, V>> readyValueSlots;

    BufferedValues(
        ExecutorService executorService,
        Consumer<List<V>> emitter,
        long retentionTimeInMillis,
        K key,
        int capacity) {
      this.executorService = executorService;
      this.emitter = emitter;
      this.capacity = capacity;
      this.expiredAt = Instant.now().plusMillis(retentionTimeInMillis);
      this.key = key;
      this.valueSlots = new ArrayList<>(capacity);
      this.readyValueSlots = new HashSet<>(capacity);
    }

    public synchronized boolean noMoreSlot() {
      return valueSlots.size() >= capacity;
    }

    public synchronized ValueSlot<K, V> getValueSlot() {
      ValueSlot<K, V> valueSlot = new ValueSlot<>(this);
      valueSlots.add(valueSlot);
      if (noMoreSlot()) {
        size = capacity;
      }
      return valueSlot;
    }

    public synchronized void fixSize() {
      // Current ValueSlot that `index` is pointing is not used yet.
      size = valueSlots.size();
      emitIfReady();
    }

    public synchronized boolean isSizeFixed() {
      return size != null;
    }

    public synchronized boolean isReady() {
      return isSizeFixed() && readyValueSlots.size() >= size;
    }

    public synchronized boolean isDone() {
      return done.get();
    }

    public synchronized void emitIfReady() {
      if (isDone()) {
        return;
      }

      if (isReady()) {
        executorService.execute(
            () -> {
              try {
                logger.info(
                    "Emitting (thread_id:{}, num_of_values:{})",
                    Thread.currentThread().getId(),
                    size);
                emitter.accept(
                    valueSlots.stream().map(vs -> vs.value).collect(Collectors.toList()));
                logger.info(
                    "Emitted (thread_id:{}, num_of_values:{})",
                    Thread.currentThread().getId(),
                    size);
                valueSlots.forEach(vf -> vf.completableFuture.complete(null));
                logger.info(
                    "Notified (thread_id:{}, num_of_values:{})",
                    Thread.currentThread().getId(),
                    size);
              } catch (Throwable e) {
                valueSlots.forEach(
                    vf ->
                        vf.completableFuture.completeExceptionally(
                            new GroupCommitException(
                                "Group commit failed. Aborting all the values", e)));
              }
            });
        done.set(true);
      }
    }

    public synchronized void notifyOfReadyValue(ValueSlot<K, V> valueSlot) {
      readyValueSlots.add(valueSlot);
      emitIfReady();
    }

    public synchronized void abortAll(Throwable cause) {
      for (ValueSlot<K, V> kv : valueSlots) {
        kv.completableFuture.completeExceptionally(
            new GroupCommitCascadeException(
                "One of the fetched items failed in group commit. The other items have the same key associated with the failure. All the items will fail",
                cause));
        done.set(true);
      }
    }
  }

  public GroupCommitter2(
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

    this.emitExecutorService =
        Executors.newFixedThreadPool(
            numberOfThreads,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(label + "-group-commit-emit-%d")
                .build());

    this.expirationCheckExecutorService =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(label + "-group-commit-expire-%d")
                .build());

    startExpirationCheckExecutorService();
  }

  private boolean dequeueAndHandleBufferedValues() {
    BufferedValues<K, V> bufferedValues = queueOfBufferedValues.peek();
    Long retryWaitInMillis = null;

    if (bufferedValues == null) {
      retryWaitInMillis = expirationCheckIntervalInMillis;
    } else if (bufferedValues.isSizeFixed()) {
      // Already expired. Nothing to do
    } else {
      Instant now = Instant.now();
      if (now.isAfter(bufferedValues.expiredAt)) {
        // Expired
        bufferedValues.fixSize();
      } else {
        // Not expired. Retry
        retryWaitInMillis =
            Math.min(
                bufferedValues.expiredAt.minusMillis(now.toEpochMilli()).toEpochMilli(),
                expirationCheckIntervalInMillis);
      }
    }

    if (retryWaitInMillis != null) {
      try {
        TimeUnit.MILLISECONDS.sleep(expirationCheckIntervalInMillis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Interrupted", e);
        return false;
      }
    } else {
      BufferedValues<K, V> removed = queueOfBufferedValues.poll();
      if (removed == null || !removed.equals(bufferedValues)) {
        logger.warn(
            "The queue returned an inconsistent return value. expected:{}, actual:{}",
            bufferedValues,
            removed);
      }
    }
    return true;
  }

  private void startExpirationCheckExecutorService() {
    expirationCheckExecutorService.execute(
        () -> {
          while (!expirationCheckExecutorService.isShutdown()) {
            if (!dequeueAndHandleBufferedValues()) {
              break;
            }
          }
        });
  }

  private synchronized ValueSlot<K, V> getValueSlot(K keyCandidate) {
    if (bufferedValues == null || bufferedValues.noMoreSlot() || bufferedValues.isDone()) {
      bufferedValues =
          new BufferedValues<>(
              emitExecutorService,
              emitter,
              retentionTimeInMillis,
              keyCandidate,
              numberOfRetentionValues);
      queueOfBufferedValues.add(bufferedValues);
    }
    return bufferedValues.getValueSlot();
  }

  public void addValue(K keyCandidate, Function<K, V> valueGeneratorFromUniqueKey)
      throws GroupCommitCascadeException, GroupCommitException {
    ValueSlot<K, V> valueSlot = getValueSlot(keyCandidate);

    V value;
    try {
      long start = System.currentTimeMillis();
      value = valueGeneratorFromUniqueKey.apply(valueSlot.getKey());
      logger.info(
          "Created value(thread_id:{}): {} ms",
          Thread.currentThread().getId(),
          System.currentTimeMillis() - start);
    } catch (Throwable e) {
      logger.error("Failed to prepare a value for group commit. keyCandidate: {}", keyCandidate, e);
      valueSlot.abort(e);
      throw e;
    }
    valueSlot.setValue(value);
    long start = System.currentTimeMillis();
    valueSlot.waitUntilEmit();
    logger.info(
        "Waited(thread_id:{}): {} ms",
        Thread.currentThread().getId(),
        System.currentTimeMillis() - start);
  }
}
