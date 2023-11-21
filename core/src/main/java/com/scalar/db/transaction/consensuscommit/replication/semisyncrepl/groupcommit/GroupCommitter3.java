package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit;

import com.google.common.base.MoreObjects;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.errorprone.annotations.concurrent.LazyInit;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit.KeyManipulator.Keys;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroupCommitter3<K, V> {
  private static final Logger logger = LoggerFactory.getLogger(GroupCommitter3.class);
  private final BlockingQueue<BufferedValues<K, V>> queueOfBufferedValues =
      new LinkedBlockingQueue<>();
  private final long retentionTimeInMillis;
  private final int numberOfRetentionValues;
  private final long expirationCheckIntervalInMillis;
  private final ExecutorService emitExecutorService;
  private final ExecutorService expirationCheckExecutorService;
  private final KeyManipulator<K> keyManipulator;
  @LazyInit private Emittable<K, V> emitter;
  @Nullable private BufferedValues<K, V> currentBufferedValues;
  private final Map<K, BufferedValues<K, V>> bufferedValuesMap = new ConcurrentHashMap<>();

  private static class ValueSlot<K, V> {
    private final BufferedValues<K, V> parentBuffer;
    private final K key;
    private final CompletableFuture<Void> completableFuture = new CompletableFuture<>();
    @Nullable private volatile V value;

    public ValueSlot(K key, BufferedValues<K, V> parentBuffer) {
      this.key = key;
      this.parentBuffer = parentBuffer;
    }

    public K getFullKey() {
      return parentBuffer.getFullKey(key);
    }

    public synchronized void putValue(V value) {
      this.value = Objects.requireNonNull(value);
      parentBuffer.notifyOfReadyValue();
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

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("key", key).toString();
    }
  }

  private static class BufferedValues<K, V> {
    private final ExecutorService executorService;
    private final Emittable<K, V> emitter;
    private final KeyManipulator<K> keyManipulator;
    private final int capacity;
    private final AtomicReference<Integer> size = new AtomicReference<>();
    private final K key;
    public final Instant expiredAt;
    private final AtomicBoolean done = new AtomicBoolean();
    private final Map<K, ValueSlot<K, V>> valueSlots;

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("key", key)
          .add("hashCode", hashCode())
          .add("expiredAt", expiredAt)
          .add("done", isDone())
          .add("ready", isReady())
          .add("sizeFixed", isSizeFixed())
          .add("valueSlots.size", valueSlots.size())
          .toString();
    }

    BufferedValues(
        ExecutorService executorService,
        Emittable<K, V> emitter,
        KeyManipulator<K> keyManipulator,
        long retentionTimeInMillis,
        int capacity) {
      this.executorService = executorService;
      this.emitter = emitter;
      this.keyManipulator = keyManipulator;
      this.capacity = capacity;
      this.expiredAt = Instant.now().plusMillis(retentionTimeInMillis);
      this.key = keyManipulator.createParentKey();
      this.valueSlots = new ConcurrentHashMap<>(capacity);
    }

    public K getFullKey(K childKey) {
      return keyManipulator.createFullKey(key, childKey);
    }

    public synchronized boolean noMoreSlot() {
      return valueSlots.size() >= capacity;
    }

    public synchronized K reserveNewValueSlot(K childKey) {
      if (isSizeFixed()) {
        throw new IllegalStateException("The size of 'valueSlot' is already fixed. buffer:" + this);
      }
      ValueSlot<K, V> valueSlot = new ValueSlot<>(childKey, this);
      valueSlots.put(childKey, valueSlot);
      if (noMoreSlot()) {
        fixSize();
      }
      return valueSlot.getFullKey();
    }

    public void putValueToSlotAndWait(K childKey, V value) throws GroupCommitException {
      if (isDone()) {
        throw new GroupCommitAlreadyClosedException(
            "This value slot buffer is already closed. buffer:" + this);
      }
      ValueSlot<K, V> valueSlot = valueSlots.get(childKey);
      if (valueSlot == null) {
        // TODO: Revisit this exception class (e.g., NotFoundException)
        throw new GroupCommitException(
            "The value slot doesn't exist. fullKey:" + keyManipulator.createFullKey(key, childKey));
      }
      valueSlot.putValue(value);
      asyncEmitIfReady();

      long start = System.currentTimeMillis();
      valueSlot.waitUntilEmit();

      logger.info(
          "Waited(thread_id:{}, key:{}): {} ms",
          getFullKey(childKey),
          Thread.currentThread().getId(),
          System.currentTimeMillis() - start);
    }

    public synchronized void fixSize() {
      ////// FIXME: DEBUG
      logger.info("GC FIX-SIZE: key={}", key);
      ////// FIXME: DEBUG
      // Current ValueSlot that `index` is pointing is not used yet.
      size.set(valueSlots.size());
      asyncEmitIfReady();
    }

    public synchronized boolean isSizeFixed() {
      return size.get() != null;
    }

    public synchronized boolean isReady() {
      return isSizeFixed()
          && valueSlots.values().stream().filter(Objects::nonNull).count() >= size.get();
    }

    public synchronized boolean isDone() {
      return done.get();
    }

    public synchronized void removeValueSlot(K childKey) {
      if (valueSlots.remove(childKey) != null) {
        if (size.get() != null && size.get() > 0) {
          size.set(size.get() - 1);
        }
      }
      ////// FIXME: DEBUG
      logger.info(
          "REMOVE VS: key={}, childKey={}, valueSlotsSize={}, size={}",
          this.key,
          childKey,
          valueSlots.size(),
          size.get());
      ////// FIXME: DEBUG
      asyncEmitIfReady();
    }

    public synchronized void asyncEmitIfReady() {
      if (isDone()) {
        return;
      }

      if (isReady()) {
        if (valueSlots.isEmpty()) {
          logger.warn("'valueSlots' is empty. Nothing to do. bufferedValue:{}", this);
          done.set(true);
          return;
        }
        executorService.execute(
            () -> {
              try {
                ////// FIXME: DEBUG
                logger.info("EMIT: buffer={}", this);
                ////// FIXME: DEBUG
                long startEmit = System.currentTimeMillis();
                emitter.execute(
                    key,
                    valueSlots.values().stream().map(vs -> vs.value).collect(Collectors.toList()));
                logger.info(
                    "Emitted (thread_id:{}, num_of_values:{}): {} ms",
                    Thread.currentThread().getId(),
                    size.get(),
                    System.currentTimeMillis() - startEmit);

                long startNotify = System.currentTimeMillis();
                // Wake up the waiting threads
                valueSlots.values().forEach(vf -> vf.completableFuture.complete(null));
                logger.info(
                    "Notified (thread_id:{}, num_of_values:{}): {} ms",
                    Thread.currentThread().getId(),
                    size.get(),
                    System.currentTimeMillis() - startNotify);
              } catch (Throwable e) {
                logger.error("Group commit failed", e);
                valueSlots
                    .values()
                    .forEach(
                        vf ->
                            vf.completableFuture.completeExceptionally(
                                new GroupCommitException(
                                    "Group commit failed. Aborting all the values", e)));
              }
            });
        done.set(true);
      }
    }

    public synchronized void notifyOfReadyValue() {
      asyncEmitIfReady();
    }

    public synchronized void abortAll(Throwable cause) {
      for (ValueSlot<K, V> kv : valueSlots.values()) {
        kv.completableFuture.completeExceptionally(
            new GroupCommitCascadeException(
                "One of the fetched items failed in group commit. The other items have the same key associated with the failure. All the items will fail",
                cause));
        done.set(true);
      }
    }
  }

  public GroupCommitter3(
      String label,
      long retentionTimeInMillis,
      int numberOfRetentionValues,
      long expirationCheckIntervalInMillis,
      int numberOfThreads,
      KeyManipulator<K> keyManipulator) {
    this.retentionTimeInMillis = retentionTimeInMillis;
    this.numberOfRetentionValues = numberOfRetentionValues;
    this.expirationCheckIntervalInMillis = expirationCheckIntervalInMillis;
    this.keyManipulator = keyManipulator;

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

  public void setEmitter(Emittable<K, V> emitter) {
    this.emitter = emitter;
  }

  ////////// FIXME: DEBUG LOG
  private volatile long lastDebugPrint = 0;
  ////////// FIXME: DEBUG LOG
  private boolean dequeueAndHandleBufferedValues() {
    BufferedValues<K, V> bufferedValues = queueOfBufferedValues.peek();
    Long retryWaitInMillis = null;

    ////////// FIXME: DEBUG LOG
    if (lastDebugPrint + 1000 < System.currentTimeMillis()) {
      logger.info("QUEUE STATUS: size={}", queueOfBufferedValues.size());
      lastDebugPrint = System.currentTimeMillis();
    }
    ////////// FIXME: DEBUG LOG

    if (bufferedValues == null) {
      retryWaitInMillis = expirationCheckIntervalInMillis;
    } else if (bufferedValues.isSizeFixed()) {
      // Already expired. Nothing to do
      ////////// FIXME: DEBUG LOG
      if (bufferedValues.expiredAt.isBefore(Instant.now().minusMillis(5000))) {
        logger.info(
            "TOO OLD BUFFER: buffer.key={}, buffer.values={}",
            bufferedValues.key,
            bufferedValues.valueSlots);
      }
      ////////// FIXME: DEBUG LOG
    } else {
      Instant now = Instant.now();
      if (now.isAfter(bufferedValues.expiredAt)) {
        // Expired
        bufferedValues.fixSize();
      } else {
        // Not expired. Retry
        retryWaitInMillis =
            Math.max(
                bufferedValues.expiredAt.minusMillis(now.toEpochMilli()).toEpochMilli(),
                expirationCheckIntervalInMillis);
      }
    }

    if (retryWaitInMillis != null) {
      ////////// FIXME: DEBUG LOG
      // logger.info("FETCHED BUFFER(RETRY): buffer={}, wait={}", bufferedValues,
      // retryWaitInMillis);
      ////////// FIXME: DEBUG LOG
      try {
        TimeUnit.MILLISECONDS.sleep(retryWaitInMillis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Interrupted", e);
        return false;
      }
    } else {
      ////////// FIXME: DEBUG LOG
      logger.info("FETCHED BUFFER(REMOVE): buffer={}", bufferedValues);
      ////////// FIXME: DEBUG LOG
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

  // Returns full key
  private synchronized K reserveNewValueSlot(K childKey) throws GroupCommitAlreadyClosedException {
    if (currentBufferedValues == null
        || currentBufferedValues.noMoreSlot()
        || currentBufferedValues.isDone()
        || currentBufferedValues.isSizeFixed()) {
      currentBufferedValues =
          new BufferedValues<>(
              emitExecutorService,
              emitter,
              keyManipulator,
              retentionTimeInMillis,
              numberOfRetentionValues);
      queueOfBufferedValues.add(currentBufferedValues);
      bufferedValuesMap.put(currentBufferedValues.key, currentBufferedValues);
    }
    return currentBufferedValues.reserveNewValueSlot(childKey);
  }

  // Returns the full key
  public K reserve(K childKey) {
    int counterForDebug = 0;
    while (true) {
      try {
        return reserveNewValueSlot(childKey);
      } catch (GroupCommitAlreadyClosedException e) {
        logger.info("Failed to reserve a new value slot. Retrying. key:{}", childKey);
        counterForDebug++;
        if (counterForDebug > 1000) {
          throw new IllegalStateException("Too many retries. Something is wrong, key:" + childKey);
        }
      } catch (Throwable e) {
        ///////// FIXME: DEBUG
        logger.error("FAILED TO RESERVE SLOT #2: UNEXPECTED key={}", childKey);
        ///////// FIXME: DEBUG
        throw e;
      }
    }
  }

  public void ready(K fullKey, V value) throws GroupCommitException {
    Keys<K> keys = keyManipulator.fromFullKey(fullKey);

    BufferedValues<K, V> bufferedValues = bufferedValuesMap.get(keys.parentKey);
    if (bufferedValues == null) {
      // TODO: Revisit this exception class
      throw new GroupCommitException(
          "The buffer for the reserved value slot doesn't exist. fullKey:" + fullKey);
    }

    try {
      bufferedValues.putValueToSlotAndWait(keys.childKey, value);
    } catch (GroupCommitAlreadyClosedException e) {
      // TODO: Move the slow transaction to a new small buffer
      throw e;
    }
  }
}
