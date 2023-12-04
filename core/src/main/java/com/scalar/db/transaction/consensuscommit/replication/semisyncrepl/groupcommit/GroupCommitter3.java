package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit;

import com.google.common.base.MoreObjects;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.errorprone.annotations.concurrent.LazyInit;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit.KeyManipulator.Keys;
import java.io.Closeable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: K should be separate into PARENT_KEY, CHILD_KEY and FULL_KEY
public class GroupCommitter3<K, V> implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(GroupCommitter3.class);
  // Queues
  private final BlockingQueue<NormalBufferedValues<K, V>> queueForSizeFix =
      new LinkedBlockingQueue<>();
  private final BlockingQueue<NormalBufferedValues<K, V>> queueForTimeout =
      new LinkedBlockingQueue<>();
  // Parameters
  private final long expirationCheckIntervalInMillis;
  private final long sizeFixExpirationInMillis;
  private final long timeoutExpirationInMillis;
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
    // Using ConcurrentHashMap results in less performance.
    private final Map<K, NormalBufferedValues<K, V>> bufferedValuesMap = new HashMap<>();
    private final Map<K, DelayedBufferedValue<K, V>> delayedBufferedValueMap = new HashMap<>();

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
                sizeFixExpirationInMillis,
                timeoutExpirationInMillis,
                numberOfRetentionValues);
        queueForSizeFix.add(currentBufferedValues);
        bufferedValuesMap.put(currentBufferedValues.key, currentBufferedValues);
        ///////// FIXME: DEBUG
        logger.info("NEW BUFFER VALUES:{}, CHILDKEY:{}", currentBufferedValues, childKey);
        ///////// FIXME: DEBUG
      }
      return currentBufferedValues.reserveNewValueSlot(childKey);
    }

    private synchronized BufferedValues<K, V> getBufferedValues(Keys<K> keys)
        throws GroupCommitException {
      // TODO: The following logic can be simplified since it's in synchronized block
      NormalBufferedValues<K, V> bufferedValues = bufferedValuesMap.get(keys.parentKey);
      DelayedBufferedValue<K, V> delayedBufferedValue =
          delayedBufferedValueMap.get(keyManipulator.createFullKey(keys.parentKey, keys.childKey));
      // Avoid the following cases to find the value slot corresponding to pk1:ck11
      // Case:1
      // - bufValMap:{pk1 => buf1:{ck11 => vs11}}, slowBufValMap:{}
      // - bufValMap:{pk1 => buf1:{ck11 => vs11}}, slowBufValMap:{pk1:ck11 => buf1:{ck11 => vs11}}
      // - bufValMap:{pk1 => buf1:{}}, slowBufValMap:{pk1:ck11 => buf1:{ck11 => vs11}}
      // - bufValMap.get(pk1) and check if it contains ck11, but not found
      // - (slowBufValMap.get(pk1:ck11) should be called even if bufValMap.get(pk1) is found)
      // - return NONE
      // Case:2
      // - bufValMap:{pk1 => buf1:{ck11 => vs11}}, slowBufValMap:{}
      // - slowBufValMap.get(pk1:ck11), but not found
      // - bufValMap:{pk1 => buf1:{ck11 => vs11}}, slowBufValMap:{pk1:ck11 => buf1:{ck11 => vs11}}
      // - bufValMap:{pk1 => buf1:{}}, slowBufValMap:{pk1:ck11 => buf1:{ck11 => vs11}}
      // - bufValMap.get(pk1) and check if it contains ck11, but not found
      // - (slowBufValMap.get(pk1:ck11) should be called after bufValMap.get(pk1))
      // - return NONE
      if (delayedBufferedValue != null) {
        return delayedBufferedValue;
      }
      if (bufferedValues != null) {
        return bufferedValues;
      }

      // TODO: Revisit this exception class
      throw new GroupCommitException(
          "The buffer for the reserved value slot doesn't exist. keys:" + keys);
    }

    // TODO: Create cleanup queue and worker to call this
    private synchronized void unregisterBufferedValues(Keys<K> keys) throws GroupCommitException {
      K fullKey = keyManipulator.createFullKey(keys.parentKey, keys.childKey);
      NormalBufferedValues<K, V> bufferedValues = bufferedValuesMap.get(keys.parentKey);
      if (bufferedValues != null) {
        synchronized (bufferedValues) {
          if (bufferedValues.isDone()) {
            bufferedValuesMap.remove(fullKey);
          }
        }
      }

      DelayedBufferedValue<K, V> delayedBufferedValue = delayedBufferedValueMap.get(fullKey);
      if (delayedBufferedValue != null) {
        synchronized (delayedBufferedValue) {
          if (delayedBufferedValue.isDone()) {
            delayedBufferedValueMap.remove(fullKey);
          }
        }
      }

      if (bufferedValues == null && delayedBufferedValue == null) {
        // TODO: Revisit this exception class
        throw new GroupCommitException(
            "The buffer for the reserved value slot doesn't exist. keys:" + keys);
      }
    }

    private synchronized void moveDelayedValueSlotsToDelayedBufferValues(
        NormalBufferedValues<K, V> bufferedValues) {
      List<ValueSlot<K, V>> notReadyValueSlots = bufferedValues.removeNotReadyValueSlots();
      for (ValueSlot<K, V> notReadyValueSlot : notReadyValueSlots) {
        K fullKey = notReadyValueSlot.getFullKey();
        DelayedBufferedValue<K, V> newDelayedBufferedValue =
            new DelayedBufferedValue<>(
                fullKey,
                emitExecutorService,
                emitter,
                keyManipulator,
                sizeFixExpirationInMillis,
                timeoutExpirationInMillis,
                notReadyValueSlot);
        DelayedBufferedValue<K, V> old =
            delayedBufferedValueMap.put(fullKey, newDelayedBufferedValue);
        if (old != null) {
          logger.warn("The slow buffer value map already has the same key buffer. {}", old);
        }
      }
      // TODO: Replace this with cleanup queue and worker
      // FIXME: stream() is a bit slow
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

    public void putValue(V value) {
      this.value = Objects.requireNonNull(value);
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
    protected final KeyManipulator<K> keyManipulator;
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
          .add("timeoutAt", timeoutAt)
          .add("done", isDone())
          .add("ready", isReady())
          .add("sizeFixed", isSizeFixed())
          .add("valueSlots.size", valueSlots.size())
          //          .add(
          //              "valueSlots.size(ready)",
          //              valueSlots.values().stream().filter(v -> v.value != null).count())
          .toString();
    }

    BufferedValues(
        K key,
        ExecutorService executorService,
        Emittable<K, V> emitter,
        KeyManipulator<K> keyManipulator,
        long sizeFixExpirationInMillis,
        long timeoutExpirationInMillis,
        int capacity) {
      this.executorService = executorService;
      this.emitter = emitter;
      this.keyManipulator = keyManipulator;
      this.capacity = capacity;
      this.sizeFixedAt = Instant.now().plusMillis(sizeFixExpirationInMillis);
      this.timeoutAt = Instant.now().plusMillis(timeoutExpirationInMillis);
      this.key = key;
      this.valueSlots = new HashMap<>(capacity);
    }

    public abstract K getFullKey(K childKey);

    public boolean noMoreSlot() {
      return valueSlots.size() >= capacity;
    }

    protected K reserveNewValueSlot(ValueSlot<K, V> valueSlot)
        throws GroupCommitAlreadySizeFixedException {
      synchronized (this) {
        if (isSizeFixed()) {
          throw new GroupCommitAlreadySizeFixedException(
              "The size of 'valueSlot' is already fixed. buffer:" + this);
        }
        valueSlots.put(valueSlot.key, valueSlot);
      }
      ///////// FIXME: DEBUG
      logger.info("RESERVE:{}, CHILDKEY:{}", this, valueSlot.key);
      ///////// FIXME: DEBUG
      if (noMoreSlot()) {
        fixSize();
      }
      return valueSlot.getFullKey();
    }

    // This sync is for moving timed-out value slot from a normal buf to a new delayed buf.
    private synchronized ValueSlot<K, V> putValueToSlot(K childKey, V value)
        throws GroupCommitException {
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
      return valueSlot;
    }

    public void putValueToSlotAndWait(K childKey, V value) throws GroupCommitException {
      ValueSlot<K, V> valueSlot;
      synchronized (this) {
        valueSlot = putValueToSlot(childKey, value);

        // This is in this block since it results in better performance
        asyncEmitIfReady();
      }
      ///////// FIXME: DEBUG
      logger.info("PUT VALUE:{}, CHILDKEY:{}", this, childKey);
      ///////// FIXME: DEBUG

      long start = System.currentTimeMillis();
      valueSlot.waitUntilEmit();

      logger.info(
          "Waited(thread_id:{}, childKey:{}): {} ms",
          Thread.currentThread().getId(),
          childKey,
          System.currentTimeMillis() - start);
    }

    public void fixSize() {
      synchronized (this) {
        // Current ValueSlot that `index` is pointing is not used yet.
        size.set(valueSlots.size());
        ////// FIXME: DEBUG
        logger.info("GC FIX-SIZE: buffer={}", this);
        ////// FIXME: DEBUG
        // This is in this block since it results in better performance
        asyncEmitIfReady();
      }
    }

    public boolean isSizeFixed() {
      return size.get() != null;
    }

    public synchronized boolean isReady() {
      if (isSizeFixed()) {
        int readySlotCount = 0;
        for (ValueSlot<K, V> valueSlot : valueSlots.values()) {
          if (valueSlot.value != null) {
            readySlotCount++;
            if (readySlotCount >= size.get()) {
              return true;
            }
          }
        }
      }
      return false;
    }

    public boolean isDone() {
      return done.get();
    }

    public void removeValueSlot(K childKey) {
      synchronized (this) {
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
        // This is in this block since it results in better performance
        asyncEmitIfReady();
      }
    }

    public synchronized void asyncEmitIfReady() {
      if (isDone()) {
        return;
      }

      if (isReady()) {
        if (valueSlots.isEmpty()) {
          // In this case, each transaction has aborted with the full transaction ID.
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
                List<V> values = new ArrayList<>(valueSlots.size());
                // Avoid using java.util.Collection.stream since it's a bit slow
                for (ValueSlot<K, V> valueSlot : valueSlots.values()) {
                  values.add(valueSlot.value);
                }
                emitter.execute(key, values);
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
        long sizeFixExpirationInMillis,
        long timeoutExpirationInMillis,
        int capacity) {
      super(
          keyManipulator.createParentKey(),
          executorService,
          emitter,
          keyManipulator,
          sizeFixExpirationInMillis,
          timeoutExpirationInMillis,
          capacity);
    }

    public K getFullKey(K childKey) {
      return keyManipulator.createFullKey(key, childKey);
    }

    public K reserveNewValueSlot(K childKey) throws GroupCommitAlreadySizeFixedException {
      return reserveNewValueSlot(new ValueSlot<>(childKey, this));
    }

    public synchronized List<ValueSlot<K, V>> removeNotReadyValueSlots() {
      // Lazy instantiation might be better, but it's likely there is a not-ready value slot since
      // it's already timed-out.
      List<ValueSlot<K, V>> removed = new ArrayList<>();
      for (Entry<K, ValueSlot<K, V>> entry : valueSlots.entrySet()) {
        ValueSlot<K, V> valueSlot = entry.getValue();
        K childKey = valueSlot.key;
        if (valueSlot.value == null) {
          removed.add(valueSlot);
        }
      }

      for (ValueSlot<K, V> valueSlot : removed) {
        removeValueSlot(valueSlot.key);
        logger.info(
            "Removed a value slot from bufferedValues to move it to slow buffered values. valueSlot:{}",
            valueSlot);
      }
      return removed;
    }
  }

  private static class DelayedBufferedValue<K, V> extends BufferedValues<K, V> {
    DelayedBufferedValue(
        K fullKey,
        ExecutorService executorService,
        Emittable<K, V> emitter,
        KeyManipulator<K> keyManipulator,
        long sizeFixExpirationInMillis,
        long timeoutExpirationInMillis,
        ValueSlot<K, V> valueSlot) {
      super(
          fullKey,
          executorService,
          emitter,
          keyManipulator,
          sizeFixExpirationInMillis,
          timeoutExpirationInMillis,
          1);
      try {
        super.reserveNewValueSlot(valueSlot);
      } catch (GroupCommitAlreadySizeFixedException e) {
        // FIXME Message
        throw new IllegalStateException(
            "Failed to reserve a value slot. This shouldn't happen. valueSlot:" + valueSlot, e);
      }
    }

    public K getFullKey(K childKey) {
      throw new AssertionError("This method must not be called in this class");
    }
  }

  public GroupCommitter3(
      String label,
      long sizeFixExpirationInMillis,
      long timeoutExpirationInMillis,
      int numberOfRetentionValues,
      long expirationCheckIntervalInMillis,
      int numberOfThreads,
      KeyManipulator<K> keyManipulator) {
    this.sizeFixExpirationInMillis = sizeFixExpirationInMillis;
    this.timeoutExpirationInMillis = timeoutExpirationInMillis;
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
  private volatile long lastDebugPrintForSizeFixQueue = 0;
  ////////// FIXME: DEBUG LOG
  private boolean handleQueueForSizeFix() {
    NormalBufferedValues<K, V> bufferedValues = queueForSizeFix.peek();
    Long retryWaitInMillis = null;

    ////////// FIXME: DEBUG LOG
    if (lastDebugPrintForSizeFixQueue + 1000 < System.currentTimeMillis()) {
      logger.info("[SIZE-FIX] QUEUE STATUS: size={}", queueForSizeFix.size());
      lastDebugPrintForSizeFixQueue = System.currentTimeMillis();
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
            // FIXME
            //            Math.max(
            //
            // bufferedValues.sizeFixedAt.minusMillis(now.toEpochMilli()).toEpochMilli(),
            //                expirationCheckIntervalInMillis);
            expirationCheckIntervalInMillis;
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

  ////////// FIXME: DEBUG LOG
  private volatile long lastDebugPrintForTimeoutQueue = 0;
  ////////// FIXME: DEBUG LOG
  private boolean handleQueueForTimeout() {
    NormalBufferedValues<K, V> bufferedValues = queueForTimeout.peek();
    Long retryWaitInMillis = null;

    ////////// FIXME: DEBUG LOG
    logger.info("[TIMEOUT] NEW BV:{}, SIZE:{}", bufferedValues, queueForTimeout.size());
    if (lastDebugPrintForTimeoutQueue + 1000 < System.currentTimeMillis()) {
      logger.info("[TIMEOUT] QUEUE STATUS: size={}", queueForTimeout.size());
      lastDebugPrintForTimeoutQueue = System.currentTimeMillis();
    }
    ////////// FIXME: DEBUG LOG

    if (bufferedValues == null) {
      retryWaitInMillis = expirationCheckIntervalInMillis * 2;
    } else if (bufferedValues.isReady()) {
      // Already ready. Nothing to do. Handle a next element immediately
    } else {
      Instant now = Instant.now();
      if (now.isAfter(bufferedValues.timeoutAt)) {
        buffersManager.moveDelayedValueSlotsToDelayedBufferValues(bufferedValues);
      } else {
        // Not expired. Retry
        retryWaitInMillis =
            // FIXME
            //            Math.max(
            //
            // bufferedValues.timeoutAt.minusMillis(now.toEpochMilli()).toEpochMilli(),
            //                expirationCheckIntervalInMillis);
            expirationCheckIntervalInMillis * 2;
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

  public boolean isGroupCommitFullKey(K fullKey) {
    return keyManipulator.isFullKey(fullKey);
  }

  public void ready(K fullKey, V value) throws GroupCommitException {
    Keys<K> keys = keyManipulator.fromFullKey(fullKey);
    BufferedValues<K, V> bufferedValues = buffersManager.getBufferedValues(keys);
    try {
      // TODO: This can throw an exception in a race condition when the value slot is moved to
      // delayed buffer values. So, retry should be needed.
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

  // The ExecutorServices are created as daemon, so calling this method isn't needed.
  // But for testing, this should be called for resources.
  @Override
  public void close() {
    if (emitExecutorService != null) {
      MoreExecutors.shutdownAndAwaitTermination(emitExecutorService, 5, TimeUnit.SECONDS);
    }
    if (sizeFixExecutorService != null) {
      MoreExecutors.shutdownAndAwaitTermination(sizeFixExecutorService, 5, TimeUnit.SECONDS);
    }
    if (timeoutExecutorService != null) {
      MoreExecutors.shutdownAndAwaitTermination(timeoutExecutorService, 5, TimeUnit.SECONDS);
    }
  }
}
