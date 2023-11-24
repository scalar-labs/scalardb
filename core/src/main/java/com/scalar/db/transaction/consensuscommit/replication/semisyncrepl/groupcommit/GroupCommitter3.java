package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit;

import com.google.common.base.MoreObjects;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.errorprone.annotations.concurrent.LazyInit;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit.KeyManipulator.Keys;
import java.time.Instant;
import java.util.Map;
import java.util.Map.Entry;
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
  // Queues
  private final BlockingQueue<NormalBufferedValues<K, V>> queueForSizeFix =
      new LinkedBlockingQueue<>();
  private final BlockingQueue<NormalBufferedValues<K, V>> queueForTimeout =
      new LinkedBlockingQueue<>();
  // Parameters
  // TODO: Separate this for size-fix and timeout
  private final long expirationCheckIntervalInMillis;
  private final long retentionTimeInMillis;
  private final int numberOfRetentionValues;
  // Executors
  private final ExecutorService emitExecutorService;
  private final ExecutorService sizeFixExecutorService;
  private final ExecutorService timeoutExecutorService;
  // Custom operations injected by the client
  private final KeyManipulator<K> keyManipulator;
  @LazyInit private Emittable<K, V> emitter;
  private final BuffersManager buffersManager;

  // This class is just for encapsulation of buffer accesses
  private class BuffersManager {
    // Buffers
    @Nullable private NormalBufferedValues<K, V> currentBufferedValues;
    private final Map<K, NormalBufferedValues<K, V>> bufferedValuesMap = new ConcurrentHashMap<>();
    private final Map<K, SlowBufferedValue<K, V>> slowBufferedValueMap = new ConcurrentHashMap<>();

    // Returns full key
    private synchronized K reserveNewValueSlot(K childKey)
        throws GroupCommitAlreadySizeFixedException {
      if (currentBufferedValues == null
          || currentBufferedValues.noMoreSlot()
          || currentBufferedValues.isDone()
          || currentBufferedValues.isSizeFixed()) {
        ///////// FIXME: DEBUG
        logger.info("OLD BUFFER VALUES:{}, CHILDKEY:{}", currentBufferedValues, childKey);
        ///////// FIXME: DEBUG
        currentBufferedValues =
            new NormalBufferedValues<>(
                emitExecutorService,
                emitter,
                keyManipulator,
                retentionTimeInMillis,
                numberOfRetentionValues);
        queueForSizeFix.add(currentBufferedValues);
        bufferedValuesMap.put(currentBufferedValues.key, currentBufferedValues);
        ///////// FIXME: DEBUG
        logger.info("NEW BUFFER VALUES:{}, CHILDKEY:{}", currentBufferedValues, childKey);
        ///////// FIXME: DEBUG
      }
      return currentBufferedValues.reserveNewValueSlot(childKey);
    }

    private BufferedValues<K, V> getBufferedValues(Keys<K> keys) throws GroupCommitException {
      NormalBufferedValues<K, V> bufferedValues = bufferedValuesMap.get(keys.parentKey);
      if (bufferedValues != null) {
        return bufferedValues;
      }

      SlowBufferedValue<K, V> slowBufferedValue =
          slowBufferedValueMap.get(keyManipulator.createFullKey(keys.parentKey, keys.childKey));
      if (slowBufferedValue != null) {
        return slowBufferedValue;
      }

      // TODO: Revisit this exception class
      throw new GroupCommitException(
          "The buffer for the reserved value slot doesn't exist. keys:" + keys);
    }

    private void moveDelayedValueSlotsToDelayedBufferValues(
        NormalBufferedValues<K, V> bufferedValues) {
      for (Entry<K, ValueSlot<K, V>> entry : bufferedValues.valueSlots.entrySet()) {
        ValueSlot<K, V> valueSlot = entry.getValue();
        K childKey = valueSlot.key;
        K fullKey = valueSlot.getFullKey();
        if (valueSlot.value == null) {
          // FIXME: Copy the completeFuture to the new buffer slot
          SlowBufferedValue<K, V> newSlowBufferedValue =
              new SlowBufferedValue<>(
                  emitExecutorService, emitter, keyManipulator, retentionTimeInMillis, valueSlot);
          SlowBufferedValue<K, V> old = slowBufferedValueMap.put(fullKey, newSlowBufferedValue);
          if (old != null) {
            logger.warn("The slow buffer value map already has the same key buffer. {}", old);
          }
          bufferedValues.removeValueSlot(childKey);
          logger.info(
              "Moved a value slot from bufferedValues to slow buffered values. valueSlot:{}",
              valueSlot);
        }
      }
      if (bufferedValues.valueSlots.values().stream().noneMatch(v -> v.value != null)) {
        bufferedValuesMap.remove(bufferedValues.key);
        logger.info("Removed a bufferedValues as it's empty. bufferedValues:{}", bufferedValues);
      }
    }
  }

  private static class ValueSlot<K, V> {
    private final NormalBufferedValues<K, V> parentBuffer;
    private final K key;
    private final CompletableFuture<Void> completableFuture = new CompletableFuture<>();
    @Nullable private volatile V value;

    public ValueSlot(K key, NormalBufferedValues<K, V> parentBuffer) {
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

  protected abstract static class BufferedValues<K, V> {
    private final ExecutorService executorService;
    private final Emittable<K, V> emitter;
    private final KeyManipulator<K> keyManipulator;
    private final int capacity;
    private final AtomicReference<Integer> size = new AtomicReference<>();
    protected final K key;
    public final Instant sizeFixedAt;
    public final Instant timeoutAt;
    private final AtomicBoolean done = new AtomicBoolean();
    protected final Map<K, ValueSlot<K, V>> valueSlots;

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("key", key)
          .add("hashCode", hashCode())
          .add("expiredAt", sizeFixedAt)
          .add("done", isDone())
          .add("ready", isReady())
          .add("sizeFixed", isSizeFixed())
          .add("valueSlots.size", valueSlots.size())
          .add(
              "valueSlots.size(ready)",
              valueSlots.values().stream().filter(v -> v.value != null).count())
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
      this.sizeFixedAt = Instant.now().plusMillis(retentionTimeInMillis);
      // FIXME
      this.timeoutAt = Instant.now().plusMillis(retentionTimeInMillis * 4);
      this.key = keyManipulator.createParentKey();
      this.valueSlots = new ConcurrentHashMap<>(capacity);
    }

    public K getFullKey(K childKey) {
      return keyManipulator.createFullKey(key, childKey);
    }

    public synchronized boolean noMoreSlot() {
      return valueSlots.size() >= capacity;
    }

    protected synchronized K reserveNewValueSlot(ValueSlot<K, V> valueSlot)
        throws GroupCommitAlreadySizeFixedException {
      if (isSizeFixed()) {
        throw new GroupCommitAlreadySizeFixedException(
            "The size of 'valueSlot' is already fixed. buffer:" + this);
      }
      valueSlots.put(valueSlot.key, valueSlot);
      if (noMoreSlot()) {
        fixSize();
      }
      ///////// FIXME: DEBUG
      logger.info("RESERVE:{}, CHILDKEY:{}", this, valueSlot.key);
      ///////// FIXME: DEBUG
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
      ///////// FIXME: DEBUG
      logger.info("PUT VALUE:{}, CHILDKEY:{}", this, childKey);
      ///////// FIXME: DEBUG

      long start = System.currentTimeMillis();
      valueSlot.waitUntilEmit();

      logger.info(
          "Waited(thread_id:{}, key:{}): {} ms",
          getFullKey(childKey),
          Thread.currentThread().getId(),
          System.currentTimeMillis() - start);
    }

    public synchronized void fixSize() {
      // Current ValueSlot that `index` is pointing is not used yet.
      size.set(valueSlots.size());
      ////// FIXME: DEBUG
      logger.info("GC FIX-SIZE: buffer={}", this);
      ////// FIXME: DEBUG
      asyncEmitIfReady();
    }

    public synchronized boolean isSizeFixed() {
      return size.get() != null;
    }

    public synchronized boolean isReady() {
      return isSizeFixed()
          && valueSlots.values().stream().filter(v -> v.value != null).count() >= size.get();
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

  private static class NormalBufferedValues<K, V> extends BufferedValues<K, V> {

    NormalBufferedValues(
        ExecutorService executorService,
        Emittable<K, V> emitter,
        KeyManipulator<K> keyManipulator,
        long retentionTimeInMillis,
        int capacity) {
      super(executorService, emitter, keyManipulator, retentionTimeInMillis, capacity);
    }

    public synchronized K reserveNewValueSlot(K childKey)
        throws GroupCommitAlreadySizeFixedException {
      return reserveNewValueSlot(new ValueSlot<>(childKey, this));
    }
  }

  private static class SlowBufferedValue<K, V> extends BufferedValues<K, V> {
    SlowBufferedValue(
        ExecutorService executorService,
        Emittable<K, V> emitter,
        KeyManipulator<K> keyManipulator,
        long retentionTimeInMillis,
        ValueSlot<K, V> valueSlot) {
      super(executorService, emitter, keyManipulator, retentionTimeInMillis, 1);
      try {
        super.reserveNewValueSlot(valueSlot);
      } catch (GroupCommitAlreadySizeFixedException e) {
        // FIXME Message
        throw new IllegalStateException(
            "Failed to reserve a value slot. This shouldn't happen. valueSlot:" + valueSlot, e);
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

    this.sizeFixExecutorService =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(label + "-group-commit-sizefix-%d")
                .build());

    startSizeFixExecutorService();

    this.timeoutExecutorService =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(label + "-group-commit-timeout-%d")
                .build());

    startTimeoutExecutorService();

    this.buffersManager = new BuffersManager();
  }

  public void setEmitter(Emittable<K, V> emitter) {
    this.emitter = emitter;
  }

  ////////// FIXME: DEBUG LOG
  private volatile long lastDebugPrint = 0;
  ////////// FIXME: DEBUG LOG
  private boolean handleQueueForSizeFix() {
    NormalBufferedValues<K, V> bufferedValues = queueForSizeFix.peek();
    Long retryWaitInMillis = null;

    ////////// FIXME: DEBUG LOG
    if (lastDebugPrint + 1000 < System.currentTimeMillis()) {
      logger.info("[SIZE-FIX] QUEUE STATUS: size={}", queueForSizeFix.size());
      lastDebugPrint = System.currentTimeMillis();
    }
    ////////// FIXME: DEBUG LOG

    if (bufferedValues == null) {
      retryWaitInMillis = expirationCheckIntervalInMillis;
    } else if (bufferedValues.isSizeFixed()) {
      // Already the size is fixed. Nothing to do. Handle a next element immediately
      ////////// FIXME: DEBUG LOG
      if (bufferedValues.sizeFixedAt.isBefore(Instant.now().minusMillis(5000))) {
        logger.info(
            "[SIZE-FIX] TOO OLD BUFFER: buffer.key={}, buffer.values={}",
            bufferedValues.key,
            bufferedValues.valueSlots);
      }
      ////////// FIXME: DEBUG LOG
    } else {
      Instant now = Instant.now();
      if (now.isAfter(bufferedValues.sizeFixedAt)) {
        // Expired. Fix the size
        bufferedValues.fixSize();
      } else {
        // Not expired. Retry
        retryWaitInMillis =
            Math.max(
                bufferedValues.sizeFixedAt.minusMillis(now.toEpochMilli()).toEpochMilli(),
                expirationCheckIntervalInMillis);
      }
    }

    if (retryWaitInMillis != null) {
      try {
        TimeUnit.MILLISECONDS.sleep(retryWaitInMillis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Interrupted", e);
        return false;
      }
    } else {
      ////////// FIXME: DEBUG LOG
      logger.info("[SIZE-FIX] FETCHED BUFFER(REMOVE): buffer={}", bufferedValues);
      ////////// FIXME: DEBUG LOG
      // Move the size-fixed buffer but not ready to the timeout queue
      if (!bufferedValues.isReady()) {
        queueForTimeout.add(bufferedValues);
      }
      NormalBufferedValues<K, V> removed = queueForSizeFix.poll();
      if (removed == null || !removed.equals(bufferedValues)) {
        logger.warn(
            "The queue returned an inconsistent return value. expected:{}, actual:{}",
            bufferedValues,
            removed);
      }
    }
    return true;
  }

  private boolean handleQueueForTimeout() {
    NormalBufferedValues<K, V> bufferedValues = queueForTimeout.peek();
    Long retryWaitInMillis = null;

    if (bufferedValues == null) {
      retryWaitInMillis = expirationCheckIntervalInMillis;
    } else if (bufferedValues.isReady()) {
      // Already ready. Nothing to do. Handle a next element immediately
    } else {
      Instant now = Instant.now();
      if (now.isAfter(bufferedValues.timeoutAt)) {
        buffersManager.moveDelayedValueSlotsToDelayedBufferValues(bufferedValues);
      } else {
        // Not expired. Retry
        retryWaitInMillis =
            Math.max(
                bufferedValues.timeoutAt.minusMillis(now.toEpochMilli()).toEpochMilli(),
                expirationCheckIntervalInMillis);
      }
    }

    if (retryWaitInMillis != null) {
      try {
        TimeUnit.MILLISECONDS.sleep(retryWaitInMillis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Interrupted", e);
        return false;
      }
    } else {
      ////////// FIXME: DEBUG LOG
      logger.info("[TIMEOUT] FETCHED BUFFER(REMOVE): buffer={}", bufferedValues);
      ////////// FIXME: DEBUG LOG
      NormalBufferedValues<K, V> removed = queueForTimeout.poll();
      if (removed == null || !removed.equals(bufferedValues)) {
        logger.warn(
            "The queue returned an inconsistent return value. expected:{}, actual:{}",
            bufferedValues,
            removed);
      }
    }
    return true;
  }

  private void startSizeFixExecutorService() {
    sizeFixExecutorService.execute(
        () -> {
          while (!sizeFixExecutorService.isShutdown()) {
            if (!handleQueueForSizeFix()) {
              break;
            }
          }
        });
  }

  private void startTimeoutExecutorService() {
    timeoutExecutorService.execute(
        () -> {
          while (!timeoutExecutorService.isShutdown()) {
            if (!handleQueueForTimeout()) {
              break;
            }
          }
        });
  }

  // Returns the full key
  public K reserve(K childKey) {
    int counterForDebug = 0;
    while (true) {
      try {
        return buffersManager.reserveNewValueSlot(childKey);
      } catch (GroupCommitAlreadySizeFixedException e) {
        logger.info("Failed to reserve a new value slot. Retrying. key:{}", childKey);
        ///////// FIXME: DEBUG
        counterForDebug++;
        if (counterForDebug > 1000) {
          throw new IllegalStateException("Too many retries. Something is wrong, key:" + childKey);
        }
        ///////// FIXME: DEBUG
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
    BufferedValues<K, V> bufferedValues = buffersManager.getBufferedValues(keys);
    try {
      bufferedValues.putValueToSlotAndWait(keys.childKey, value);
    } catch (GroupCommitAlreadyClosedException e) {
      // TODO: Move the slow transaction to a new small buffer
      throw e;
    }
  }

  public void remove(K fullKey) throws GroupCommitException {
    Keys<K> keys = keyManipulator.fromFullKey(fullKey);
    BufferedValues<K, V> bufferedValues = buffersManager.getBufferedValues(keys);
    bufferedValues.removeValueSlot(keys.childKey);
  }
}
