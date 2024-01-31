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
  private final BlockingQueue<NormalGroup<K, V>> queueForNormalGroupClose =
      new LinkedBlockingQueue<>();
  private final BlockingQueue<NormalGroup<K, V>> queueForDelayedSlotMove =
      new LinkedBlockingQueue<>();
  private final BlockingQueue<DelayedGroup<K, V>> queueForDelayedGroupEmit =
      new LinkedBlockingQueue<>();
  // Parameters
  private final long expirationCheckIntervalInMillis;
  private final long sizeFixExpirationInMillis;
  private final long timeoutExpirationInMillis;
  private final int numberOfRetentionValues;
  // Executors
  private final ExecutorService emitExecutorService;
  private final ExecutorService normalGroupCloseExecutorService;
  private final ExecutorService delayedSlotMoveExecutorService;
  private final ExecutorService delayedGroupEmitExecutorService;
  // Custom operations injected by the client
  private final KeyManipulator<K> keyManipulator;
  @LazyInit private Emittable<K, V> emitter;
  private final GroupManager groupManager;

  // FIXME: This single instance can be a performance bottleneck. Try multi-partitions.
  // This class is just for encapsulation of buffer accesses
  private class GroupManager {
    // Buffers
    @Nullable private NormalGroup<K, V> currentGroup;
    // Using ConcurrentHashMap results in less performance.
    private final Map<K, NormalGroup<K, V>> bufferedValuesMap = new HashMap<>();
    private final Map<K, DelayedGroup<K, V>> delayedBufferedValueMap = new HashMap<>();

    // Returns full key
    private K reserveNewValueSlot(K childKey) throws GroupCommitAlreadySizeFixedException {
      // TODO:
      //   - add a flag to represent if a new buffer must be created
      //   - move the log out of the synchronized block
      boolean isNewBufferedValuesCreated = false;
      NormalGroup<K, V> oldBufferedValues = null;
      NormalGroup<K, V> newBufferedValues = null;
      synchronized (this) {
        if (currentGroup == null || currentGroup.isClosed()) {
          isNewBufferedValuesCreated = true;
          oldBufferedValues = currentGroup;
          currentGroup =
              new NormalGroup<>(
                  emitExecutorService,
                  emitter,
                  keyManipulator,
                  sizeFixExpirationInMillis,
                  timeoutExpirationInMillis,
                  numberOfRetentionValues);
          newBufferedValues = currentGroup;
          // TODO: This can be a faster queue?
          queueForNormalGroupClose.add(currentGroup);
          bufferedValuesMap.put(currentGroup.key, currentGroup);
        }
      }
      if (isNewBufferedValuesCreated) {
        ///////// FIXME: DEBUG
        logger.info(
            "NEW BV:{}, OLD BV:{}, CHILD_KEY:{}", newBufferedValues, oldBufferedValues, childKey);
        ///////// FIXME: DEBUG
      }
      return currentGroup.reserveNewValueSlot(childKey);
    }

    private synchronized Group<K, V> getBufferedValues(Keys<K> keys) throws GroupCommitException {
      // TODO: The following logic can be simplified since it's in synchronized block
      NormalGroup<K, V> bufferedValues = bufferedValuesMap.get(keys.parentKey);
      DelayedGroup<K, V> delayedBufferedValue =
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

      throw new GroupCommitTargetNotFoundException(
          "The buffer for the reserved value slot doesn't exist. keys:" + keys);
    }

    // TODO: Create cleanup queue and worker to call this
    private synchronized void unregisterBufferedValues(Keys<K> keys) throws GroupCommitException {
      K fullKey = keyManipulator.createFullKey(keys.parentKey, keys.childKey);
      NormalGroup<K, V> bufferedValues = bufferedValuesMap.get(keys.parentKey);
      if (bufferedValues != null) {
        synchronized (bufferedValues) {
          if (bufferedValues.isDone()) {
            bufferedValuesMap.remove(fullKey);
          }
        }
      }

      DelayedGroup<K, V> delayedBufferedValue = delayedBufferedValueMap.get(fullKey);
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
        NormalGroup<K, V> bufferedValues) {
      logger.info(
          "[DELAYED-SLOT-MOVE] moveDelayedValueSlotsToDelayedBufferValues#1 BV:{}", bufferedValues);
      List<Slot<K, V>> notReadyValueSlots = bufferedValues.removeNotReadyValueSlots();
      for (Slot<K, V> notReadyValueSlot : notReadyValueSlots) {
        K fullKey = notReadyValueSlot.getFullKey();
        DelayedGroup<K, V> newDelayedBufferedValue =
            new DelayedGroup<>(
                fullKey,
                emitExecutorService,
                emitter,
                keyManipulator,
                sizeFixExpirationInMillis,
                timeoutExpirationInMillis,
                notReadyValueSlot);
        DelayedGroup<K, V> old = delayedBufferedValueMap.put(fullKey, newDelayedBufferedValue);
        if (old != null) {
          logger.warn("The slow buffer value map already has the same key buffer. {}", old);
        }
      }
      logger.info(
          "[DELAYED-SLOT-MOVE] moveDelayedValueSlotsToDelayedBufferValues#2 BV:{}", bufferedValues);
      if (bufferedValues.valueSlots.values().stream().noneMatch(v -> v.value != null)) {
        bufferedValuesMap.remove(bufferedValues.key);
        logger.info("Removed a bufferedValues as it's empty. bufferedValues:{}", bufferedValues);
      }
      logger.info(
          "[DELAYED-SLOT-MOVE] moveDelayedValueSlotsToDelayedBufferValues#3 BV:{}", bufferedValues);
    }
  }

  private static class Slot<K, V> {
    private final NormalGroup<K, V> parentBuffer;
    private final K key;
    private final CompletableFuture<Void> completableFuture = new CompletableFuture<>();
    @Nullable private volatile V value;

    public Slot(K key, NormalGroup<K, V> parentBuffer) {
      this.key = key;
      this.parentBuffer = parentBuffer;
    }

    public K getFullKey() {
      return parentBuffer.getFullKey(key);
    }

    public void putValue(V value) {
      this.value = Objects.requireNonNull(value);
    }

    public void waitUntilEmit() throws GroupCommitException {
      try {
        completableFuture.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        // TODO: Unified the error message
        throw new RuntimeException("Group commit was interrupted", e);
      } catch (ExecutionException e) {
        // TODO: Sort these exceptions
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

  protected abstract static class Group<K, V> {
    private final ExecutorService executorService;
    private final Emittable<K, V> emitter;
    protected final KeyManipulator<K> keyManipulator;
    private final int capacity;
    private final AtomicReference<Integer> size = new AtomicReference<>();
    protected final K key;
    public final Instant sizeFixedAt;
    public final Instant timeoutAt;
    private final AtomicBoolean done = new AtomicBoolean();
    protected final Map<K, Slot<K, V>> valueSlots;
    // Whether to reject a new value slot.
    protected final AtomicBoolean closed = new AtomicBoolean();

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

    Group(
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

    protected K reserveNewValueSlot(Slot<K, V> valueSlot)
        throws GroupCommitAlreadySizeFixedException {
      return reserveNewValueSlot(valueSlot, true);
    }

    protected K reserveNewValueSlot(Slot<K, V> valueSlot, boolean autoEmit)
        throws GroupCommitAlreadySizeFixedException {
      synchronized (this) {
        if (isSizeFixed()) {
          throw new GroupCommitAlreadySizeFixedException(
              "The size of 'valueSlot' is already fixed. buffer:" + this);
        }
        valueSlots.put(valueSlot.key, valueSlot);
        updateIsClosed();
      }
      ///////// FIXME: DEBUG
      logger.info("RESERVE:{}, CHILDKEY:{}", this, valueSlot.key);
      ///////// FIXME: DEBUG
      if (noMoreSlot()) {
        fixSize(autoEmit);
      }
      return valueSlot.getFullKey();
    }

    // This sync is for moving timed-out value slot from a normal buf to a new delayed buf.
    private synchronized Slot<K, V> putValueToSlot(K childKey, V value)
        throws GroupCommitAlreadyClosedException, GroupCommitTargetNotFoundException {
      if (isDone()) {
        throw new GroupCommitAlreadyClosedException(
            "This value slot buffer is already closed. buffer:" + this);
      }

      Slot<K, V> valueSlot = valueSlots.get(childKey);
      if (valueSlot == null) {
        throw new GroupCommitTargetNotFoundException(
            "The value slot doesn't exist. fullKey:" + keyManipulator.createFullKey(key, childKey));
      }
      valueSlot.putValue(value);
      return valueSlot;
    }

    public void putValueToSlotAndWait(K childKey, V value) throws GroupCommitException {
      Slot<K, V> valueSlot;
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
          "Waited(thread_id:{}, parentKey:{}, childKey:{}): {} ms",
          Thread.currentThread().getId(),
          key,
          childKey,
          System.currentTimeMillis() - start);
    }

    public void fixSize() {
      fixSize(true);
    }

    public void fixSize(boolean autoEmit) {
      synchronized (this) {
        // Current ValueSlot that `index` is pointing is not used yet.
        size.set(valueSlots.size());
        updateIsClosed();
        ////// FIXME: DEBUG
        logger.info("Fixed size: buffer={}", this);
        ////// FIXME: DEBUG
        // This is in this block since it results in better performance
        if (autoEmit) {
          asyncEmitIfReady();
        }
      }
    }

    public boolean isSizeFixed() {
      return size.get() != null;
    }

    public synchronized boolean isReady() {
      if (isSizeFixed()) {
        int readySlotCount = 0;
        for (Slot<K, V> valueSlot : valueSlots.values()) {
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

    public boolean isClosed() {
      return closed.get();
    }

    public synchronized void updateIsClosed() {
      closed.set(noMoreSlot() || isDone() || isSizeFixed());
    }

    public void removeValueSlot(K childKey) {
      synchronized (this) {
        if (valueSlots.remove(childKey) != null) {
          if (size.get() != null && size.get() > 0) {
            size.set(size.get() - 1);
          }
          updateIsClosed();
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
          updateIsClosed();
          return;
        }
        executorService.execute(
            () -> {
              try {
                ////// FIXME: DEBUG
                logger.info("Emitting: buffer={}", this);
                ////// FIXME: DEBUG
                long startEmit = System.currentTimeMillis();
                List<V> values = new ArrayList<>(valueSlots.size());
                // Avoid using java.util.Collection.stream since it's a bit slow
                for (Slot<K, V> valueSlot : valueSlots.values()) {
                  values.add(valueSlot.value);
                }
                emitter.execute(key, values);
                logger.info(
                    "Emitted (thread_id:{}, key:{}, num_of_values:{}): {} ms",
                    Thread.currentThread().getId(),
                    key,
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
        updateIsClosed();
      }
    }

    public synchronized void notifyOfReadyValue() {
      asyncEmitIfReady();
    }

    public synchronized void abortAll(Throwable cause) {
      for (Slot<K, V> kv : valueSlots.values()) {
        kv.completableFuture.completeExceptionally(
            new GroupCommitCascadeException(
                "One of the fetched items failed in group commit. The other items have the same key associated with the failure. All the items will fail",
                cause));
        done.set(true);
        updateIsClosed();
      }
    }
  }

  private static class NormalGroup<K, V> extends Group<K, V> {

    NormalGroup(
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
      return reserveNewValueSlot(new Slot<>(childKey, this));
    }

    public synchronized List<Slot<K, V>> removeNotReadyValueSlots() {
      // Lazy instantiation might be better, but it's likely there is a not-ready value slot since
      // it's already timed-out.
      List<Slot<K, V>> removed = new ArrayList<>();
      for (Entry<K, Slot<K, V>> entry : valueSlots.entrySet()) {
        Slot<K, V> valueSlot = entry.getValue();
        K childKey = valueSlot.key;
        if (valueSlot.value == null) {
          removed.add(valueSlot);
        }
      }

      for (Slot<K, V> valueSlot : removed) {
        removeValueSlot(valueSlot.key);
        logger.info(
            "Removed a value slot from bufferedValues to move it to slow buffered values. valueSlot:{}",
            valueSlot);
      }
      return removed;
    }
  }

  private static class DelayedGroup<K, V> extends Group<K, V> {
    DelayedGroup(
        K fullKey,
        ExecutorService executorService,
        Emittable<K, V> emitter,
        KeyManipulator<K> keyManipulator,
        long sizeFixExpirationInMillis,
        long timeoutExpirationInMillis,
        Slot<K, V> valueSlot) {
      super(
          fullKey,
          executorService,
          emitter,
          keyManipulator,
          sizeFixExpirationInMillis,
          timeoutExpirationInMillis,
          1);
      try {
        // Auto emit should be disabled since:
        // - the queue and worker for delayed values will emit this if it's ready
        // - to avoid taking time in synchronized blocks
        super.reserveNewValueSlot(valueSlot, false);
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

    this.normalGroupCloseExecutorService =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(label + "-group-commit-normal-group-close-%d")
                .build());

    startNormalGroupCloseExecutorService();

    this.delayedSlotMoveExecutorService =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(label + "-group-commit-delayed-slot-move-%d")
                .build());

    startDelayedSlotMoveExecutorService();

    this.delayedGroupEmitExecutorService =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(label + "-group-commit-delayed-group-emit-%d")
                .build());

    startDelayedGroupEmitExecutorService();

    this.groupManager = new GroupManager();
  }

  public void setEmitter(Emittable<K, V> emitter) {
    this.emitter = emitter;
  }

  ////////// FIXME: DEBUG LOG
  private volatile long lastDebugPrintForNormalGroupCloseQueue = 0;
  ////////// FIXME: DEBUG LOG
  private boolean handleQueueForNormalGroupClose() {
    NormalGroup<K, V> bufferedValues = queueForNormalGroupClose.peek();
    Long retryWaitInMillis = null;

    ////////// FIXME: DEBUG LOG
    if (lastDebugPrintForNormalGroupCloseQueue + 1000 < System.currentTimeMillis()) {
      logger.info("[NORMAL-GROUP-CLOSE] QUEUE STATUS: size={}", queueForNormalGroupClose.size());
      lastDebugPrintForNormalGroupCloseQueue = System.currentTimeMillis();
    }
    ////////// FIXME: DEBUG LOG

    if (bufferedValues == null) {
      retryWaitInMillis = expirationCheckIntervalInMillis;
    } else if (bufferedValues.isSizeFixed()) {
      // Already the size is fixed. Nothing to do. Handle a next element immediately
      ////////// FIXME: DEBUG LOG
      if (bufferedValues.sizeFixedAt.isBefore(Instant.now().minusMillis(5000))) {
        logger.info(
            "[NORMAL-GROUP-CLOSE] TOO OLD BUFFER: buffer.key={}, buffer.values={}",
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
        retryWaitInMillis = expirationCheckIntervalInMillis;
      }
    }

    if (retryWaitInMillis != null) {
      try {
        TimeUnit.MILLISECONDS.sleep(retryWaitInMillis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        // TODO: Unified the error message
        logger.warn("Interrupted", e);
        return false;
      }
    } else {
      ////////// FIXME: DEBUG LOG
      logger.info("[NORMAL-GROUP-CLOSE] FETCHED BUFFER(REMOVE): buffer={}", bufferedValues);
      ////////// FIXME: DEBUG LOG
      // Move the size-fixed buffer but not ready to the timeout queue
      if (!bufferedValues.isReady()) {
        queueForDelayedSlotMove.add(bufferedValues);
      }
      NormalGroup<K, V> removed = queueForNormalGroupClose.poll();
      // Check if the removed buffered value is expected just in case.
      if (removed == null || !removed.equals(bufferedValues)) {
        logger.error(
            "The queue for size-fix returned an inconsistent return value. expected:{}, actual:{}",
            bufferedValues,
            removed);
        if (removed != null) {
          queueForNormalGroupClose.add(removed);
        }
      }
    }
    return true;
  }

  ////////// FIXME: DEBUG LOG
  private volatile long lastDebugPrintForDelayedSlotMoveQueue = 0;
  ////////// FIXME: DEBUG LOG
  private boolean handleQueueForDelayedSlotMove() {
    NormalGroup<K, V> bufferedValues = queueForDelayedSlotMove.peek();
    Long retryWaitInMillis = null;

    ////////// FIXME: DEBUG LOG
    logger.info(
        "[DELAYED-SLOT-MOVE] NEW BV:{}, SIZE:{}", bufferedValues, queueForDelayedSlotMove.size());
    if (lastDebugPrintForDelayedSlotMoveQueue + 1000 < System.currentTimeMillis()) {
      logger.info("[DELAYED-SLOT-MOVE] QUEUE STATUS: size={}", queueForDelayedSlotMove.size());
      lastDebugPrintForDelayedSlotMoveQueue = System.currentTimeMillis();
    }
    ////////// FIXME: DEBUG LOG

    if (bufferedValues == null) {
      retryWaitInMillis = expirationCheckIntervalInMillis * 2;
    } else if (bufferedValues.isReady()) {
      // Already ready. Nothing to do. Handle a next element immediately
    } else {
      Instant now = Instant.now();
      if (now.isAfter(bufferedValues.timeoutAt)) {
        ////////// FIXME: DEBUG LOG
        long start = System.currentTimeMillis();
        groupManager.moveDelayedValueSlotsToDelayedBufferValues(bufferedValues);
        logger.info(
            "[DELAYED-SLOT-MOVE] MOVED BV:{} TO DELAYED BUFFERS, DURATION:{}ms, SIZE:{}",
            bufferedValues,
            (System.currentTimeMillis() - start),
            queueForDelayedSlotMove.size());
      } else {
        // Not expired. Retry
        retryWaitInMillis = expirationCheckIntervalInMillis * 2;
      }
    }

    if (retryWaitInMillis != null) {
      try {
        TimeUnit.MILLISECONDS.sleep(retryWaitInMillis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        // TODO: Unified the error message
        logger.warn("Interrupted", e);
        return false;
      }
    } else {
      ////////// FIXME: DEBUG LOG
      logger.info("[DELAYED-SLOT-MOVE] FETCHED BUFFER(REMOVE): buffer={}", bufferedValues);
      ////////// FIXME: DEBUG LOG
      NormalGroup<K, V> removed = queueForDelayedSlotMove.poll();
      // Check if the removed buffered value is expected just in case.
      if (removed == null || !removed.equals(bufferedValues)) {
        logger.error(
            "The queue for timeout returned an inconsistent return value. expected:{}, actual:{}",
            bufferedValues,
            removed);
        if (removed != null) {
          queueForDelayedSlotMove.add(removed);
        }
      }
    }
    return true;
  }

  private volatile long lastDebugPrintForDelayedGroupEmitQueue = 0;
  ////////// FIXME: DEBUG LOG
  private boolean handleQueueForDelayedGroupEmit() {
    DelayedGroup<K, V> bufferedValues = queueForDelayedGroupEmit.peek();
    Long waitInMillis = expirationCheckIntervalInMillis;

    ////////// FIXME: DEBUG LOG
    if (lastDebugPrintForDelayedGroupEmitQueue + 1000 < System.currentTimeMillis()) {
      logger.info("[DELAYED-GROUP-EMIT] QUEUE STATUS: size={}", queueForDelayedGroupEmit.size());
      lastDebugPrintForDelayedGroupEmitQueue = System.currentTimeMillis();
    }
    ////////// FIXME: DEBUG LOG

    ////////// FIXME: DEBUG LOG
    logger.info("[DELAYED-GROUP-EMIT] FETCHED BUFFER(REMOVE): buffer={}", bufferedValues);
    ////////// FIXME: DEBUG LOG
    if (bufferedValues == null) {
      // The queue is empty, so wait for a longer time.
      waitInMillis = expirationCheckIntervalInMillis * 2;
    } else {
      DelayedGroup<K, V> removed = queueForDelayedGroupEmit.poll();
      // Check if the removed buffered value is expected just in case.
      if (removed == null || !removed.equals(bufferedValues)) {
        logger.error(
            "The queue for delayed values returned an inconsistent return value. expected:{}, actual:{}",
            bufferedValues,
            removed);
        if (removed != null) {
          queueForDelayedGroupEmit.add(removed);
        }
        return true;
      } else if (bufferedValues.isReady()) {
        // Send the ready buffer asynchronously and check the result later.
        bufferedValues.asyncEmitIfReady();
      } else if (bufferedValues.isDone()) {
        // Don't need retries.
        return true;
      }
      // Buffered values in the queue for delayed ones could contain very delayed ones.
      // Those delayed ones should be handled later.
      queueForDelayedGroupEmit.add(bufferedValues);
    }

    try {
      TimeUnit.MILLISECONDS.sleep(waitInMillis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      // TODO: Unified the error message
      logger.warn("Interrupted", e);
      return false;
    }

    return true;
  }

  private void startNormalGroupCloseExecutorService() {
    normalGroupCloseExecutorService.execute(
        () -> {
          while (!normalGroupCloseExecutorService.isShutdown()) {
            if (!handleQueueForNormalGroupClose()) {
              break;
            }
          }
        });
  }

  private void startDelayedSlotMoveExecutorService() {
    delayedSlotMoveExecutorService.execute(
        () -> {
          while (!delayedSlotMoveExecutorService.isShutdown()) {
            if (!handleQueueForDelayedSlotMove()) {
              break;
            }
          }
        });
  }

  private void startDelayedGroupEmitExecutorService() {
    delayedGroupEmitExecutorService.execute(
        () -> {
          while (!delayedGroupEmitExecutorService.isShutdown()) {
            if (!handleQueueForDelayedGroupEmit()) {
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
        return groupManager.reserveNewValueSlot(childKey);
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
    int retry = 0;
    while (true) {
      Group<K, V> bufferedValues = groupManager.getBufferedValues(keys);
      try {
        bufferedValues.putValueToSlotAndWait(keys.childKey, value);
        return;
      } catch (GroupCommitAlreadyClosedException | GroupCommitTargetNotFoundException e) {
        // This can throw an exception in a race condition when the value slot is moved to
        // delayed buffer values. So, retry should be needed.
        if (bufferedValues instanceof GroupCommitter3.NormalGroup) {
          if (++retry >= 4) {
            throw new GroupCommitException(
                String.format("Retry over for putting a value to the slot. fullKey=%s", fullKey),
                e);
          }
          try {
            TimeUnit.MILLISECONDS.sleep(10);
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            // TODO: Unified the error message
            throw new RuntimeException(ex);
          }
          continue;
        }
        throw e;
      }
    }
  }

  public void remove(K fullKey) throws GroupCommitException {
    Keys<K> keys = keyManipulator.fromFullKey(fullKey);
    Group<K, V> bufferedValues = groupManager.getBufferedValues(keys);
    bufferedValues.removeValueSlot(keys.childKey);
  }

  // The ExecutorServices are created as daemon, so calling this method isn't needed.
  // But for testing, this should be called for resources.
  @Override
  public void close() {
    if (delayedGroupEmitExecutorService != null) {
      MoreExecutors.shutdownAndAwaitTermination(
          delayedGroupEmitExecutorService, 5, TimeUnit.SECONDS);
    }
    if (delayedSlotMoveExecutorService != null) {
      MoreExecutors.shutdownAndAwaitTermination(
          delayedSlotMoveExecutorService, 5, TimeUnit.SECONDS);
    }
    if (normalGroupCloseExecutorService != null) {
      MoreExecutors.shutdownAndAwaitTermination(
          normalGroupCloseExecutorService, 5, TimeUnit.SECONDS);
    }
    if (emitExecutorService != null) {
      MoreExecutors.shutdownAndAwaitTermination(emitExecutorService, 5, TimeUnit.SECONDS);
    }
  }
}
