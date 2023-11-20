package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit;

import com.google.common.base.MoreObjects;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroupCommitter3<V> {
  private static final Logger logger = LoggerFactory.getLogger(GroupCommitter3.class);
  private final BlockingQueue<BufferedValues<V>> queueOfBufferedValues =
      new LinkedBlockingQueue<>();
  private final long retentionTimeInMillis;
  private final int numberOfRetentionValues;
  private final long expirationCheckIntervalInMillis;
  private final ExecutorService emitExecutorService;
  private final ExecutorService expirationCheckExecutorService;
  // FIXME
  private final Supplier<String> parentKeyGenerator = () -> UUID.randomUUID().toString();
  private final Emittable<V> emitter;
  private BufferedValues<V> currentBufferedValues;
  private Map<String, BufferedValues<V>> bufferedValuesMap;

  private static class ValueSlot<V> {
    private final BufferedValues<V> parentBuffer;
    private final String key;
    private final CompletableFuture<Void> completableFuture = new CompletableFuture<>();
    @Nullable private volatile V value;

    public ValueSlot(String key, BufferedValues<V> parentBuffer) {
      this.key = key;
      this.parentBuffer = parentBuffer;
    }

    public String getParentKey() {
      return parentBuffer.key;
    }

    public String getFullKey() {
      // FIXME
      return parentBuffer.key + ":" + key;
    }

    public synchronized void setValue(V value) throws GroupCommitException {
      if (value == null) {
        throw new AssertionError("'value' is null. key=" + getParentKey());
      }
      if (parentBuffer.isDone()) {
        throw new GroupCommitAlreadyClosedException(
            String.format(
                "The parent buffer is already closed. parentBuffer:%s, value:%s",
                parentBuffer, value));
      }
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

    public void remove() {
      parentBuffer.removeValueSlot(key);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("value", value).toString();
    }
  }

  private static class BufferedValues<V> {
    private final ExecutorService executorService;
    private final Emittable<V> emitter;
    private final int capacity;
    private final AtomicReference<Integer> size = new AtomicReference<>();
    private final String key;
    public final Instant expiredAt;
    private final AtomicBoolean done = new AtomicBoolean();
    private final Map<String, ValueSlot<V>> valueSlots;

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
        Emittable<V> emitter,
        long retentionTimeInMillis,
        String key,
        int capacity) {
      this.executorService = executorService;
      this.emitter = emitter;
      this.capacity = capacity;
      this.expiredAt = Instant.now().plusMillis(retentionTimeInMillis);
      if (key == null) {
        throw new IllegalArgumentException("`key` can't be null");
      }
      this.key = key;
      // TODO: Revisit the type
      this.valueSlots = new HashMap<>(capacity);
    }

    public synchronized boolean noMoreSlot() {
      return valueSlots.size() >= capacity;
    }

    public synchronized ValueSlot<V> getValueSlot(String origKey)
        throws GroupCommitAlreadyClosedException {
      if (isSizeFixed()) {
        throw new GroupCommitAlreadyClosedException(
            "The size of 'valueSlot' is already fixed. buffer:" + this);
      }
      ValueSlot<V> valueSlot = new ValueSlot<>(origKey, this);
      valueSlots.put(origKey, valueSlot);
      if (noMoreSlot()) {
        fixSize();
      }
      return valueSlot;
    }

    public synchronized void fixSize() {
      ////// FIXME: DEBUG
      logger.info("GC FIX-SIZE: key={}", key);
      ////// FIXME: DEBUG
      // Current ValueSlot that `index` is pointing is not used yet.
      size.set(valueSlots.size());
      emitIfReady();
    }

    public synchronized boolean isSizeFixed() {
      return size.get() != null;
    }

    public synchronized boolean isReady() {
      return isSizeFixed() && valueSlots.size() >= size.get();
    }

    public synchronized boolean isDone() {
      return done.get();
    }

    public synchronized void removeValueSlot(String childKey) {
      valueSlots.remove(childKey);
      if (size.get() != null) {
        size.set(size.get() - 1);
      }
      ////// FIXME: DEBUG
      logger.info(
          "REMOVE VS: key={}, childKey={}, valueSlotsSize={}, size={}",
          this.key,
          childKey,
          valueSlots.size(),
          size.get());
      ////// FIXME: DEBUG
      emitIfReady();
    }

    public synchronized void emitIfReady() {
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
                    valueSlots.values().stream().map(vs -> vs.value).collect(Collectors.toList()));
                logger.info(
                    "Emitted (thread_id:{}, num_of_values:{}): {} ms",
                    Thread.currentThread().getId(),
                    size.get(),
                    System.currentTimeMillis() - startEmit);

                long startNotify = System.currentTimeMillis();
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

    public synchronized void notifyOfReadyValue(ValueSlot<V> valueSlot) {
      // FIXME
      // readyValueSlots.add(valueSlot);
      emitIfReady();
    }

    public synchronized void abortAll(Throwable cause) {
      for (ValueSlot<V> kv : valueSlots.values()) {
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
      Emittable<V> emitter) {
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

  ////////// FIXME: DEBUG LOG
  private volatile long lastDebugPrint = 0;
  ////////// FIXME: DEBUG LOG
  private boolean dequeueAndHandleBufferedValues() {
    BufferedValues<V> bufferedValues = queueOfBufferedValues.peek();
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
      BufferedValues<V> removed = queueOfBufferedValues.poll();
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

  private synchronized ValueSlot<V> getValueSlot(String origKey)
      throws GroupCommitAlreadyClosedException {
    if (currentBufferedValues == null
        || currentBufferedValues.noMoreSlot()
        || currentBufferedValues.isDone()) {
      currentBufferedValues =
          new BufferedValues<>(
              emitExecutorService,
              emitter,
              retentionTimeInMillis,
              parentKeyGenerator.get(),
              numberOfRetentionValues);
      queueOfBufferedValues.add(currentBufferedValues);
      bufferedValuesMap.put(currentBufferedValues.key, currentBufferedValues);
    }
    return currentBufferedValues.getValueSlot(origKey);
  }

  private ValueSlot<V> getValueSlotContainingValue(
      String origKey, ValueGenerator<String, V> valueGeneratorFromUniqueKey)
      throws GroupCommitException {
    ValueSlot<V> valueSlot = null;
    try {
      valueSlot = getValueSlot(origKey);

      long start = System.currentTimeMillis();
      V value = valueGeneratorFromUniqueKey.execute(valueSlot.getParentKey());
      logger.info(
          "Created value(thread_id:{}): {} ms, bufferKey={}, key={}",
          Thread.currentThread().getId(),
          System.currentTimeMillis() - start,
          valueSlot.parentBuffer.key,
          origKey);
      valueSlot.setValue(value);
      return valueSlot;
    } catch (GroupCommitAlreadyClosedException e) {
      ///////// FIXME: DEBUG
      logger.info(
          "FAILED TO CREATE VALUE #1: , bufferKey={}, key={}",
          valueSlot == null ? "NULL" : valueSlot.parentBuffer.key,
          origKey);
      ///////// FIXME: DEBUG

      if (valueSlot != null) {
        valueSlot.remove();
      }
      throw e;
    } catch (Throwable e) {
      ///////// FIXME: DEBUG
      logger.info(
          "FAILED TO CREATE VALUE #2: , bufferKey={}, key={}",
          valueSlot == null ? "NULL" : valueSlot.parentBuffer.key,
          origKey);
      ///////// FIXME: DEBUG
      GroupCommitException gce =
          new GroupCommitException(
              String.format("Failed to prepare a value for group commit. origKey: %s", origKey), e);
      if (valueSlot != null) {
        valueSlot.remove();
      }
      throw gce;
    }
  }

  public String reserveSlot(String origKey) throws GroupCommitException {
    try {
      ValueSlot<V> valueSlot = getValueSlot(origKey);
      return valueSlot.getFullKey();
    } catch (GroupCommitAlreadyClosedException e) {
      ///////// FIXME: DEBUG
      logger.info("FAILED TO RESERVE SLOT #1: key={}", origKey);
      ///////// FIXME: DEBUG
      throw e;
    } catch (Throwable e) {
      ///////// FIXME: DEBUG
      logger.error("FAILED TO RESERVE SLOT #2: UNEXPECTED key={}", origKey);
      throw e;
    }
  }

  // FIXME: Change this to `setValue(String fullKey, V value)`
  public void addValue(String origKey, ValueGenerator<String, V> valueGeneratorFromUniqueKey)
      throws GroupCommitException {
    ValueSlot<V> valueSlot = getValueSlotContainingValue(origKey, valueGeneratorFromUniqueKey);

    long start = System.currentTimeMillis();
    valueSlot.waitUntilEmit();
    logger.info(
        "Waited(thread_id:{}): {} ms",
        Thread.currentThread().getId(),
        System.currentTimeMillis() - start);
  }
}
