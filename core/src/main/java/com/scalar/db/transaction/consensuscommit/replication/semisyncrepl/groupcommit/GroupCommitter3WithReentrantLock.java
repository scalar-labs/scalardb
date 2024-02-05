package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit;

import com.google.common.base.MoreObjects;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.errorprone.annotations.concurrent.LazyInit;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit.KeyManipulator.Keys;
import java.io.Closeable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
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
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: K should be separate into PARENT_KEY, CHILD_KEY and FULL_KEY
public class GroupCommitter3WithReentrantLock<K, V> implements Closeable {
  private static final Logger logger =
      LoggerFactory.getLogger(GroupCommitter3WithReentrantLock.class);
  // Queues
  private final BlockingQueue<NormalGroup<K, V>> queueForNormalGroupClose =
      new LinkedBlockingQueue<>();
  private final BlockingQueue<NormalGroup<K, V>> queueForDelayedSlotMove =
      new LinkedBlockingQueue<>();
  //  private final BlockingQueue<DelayedGroup<K, V>> queueForDelayedGroupEmit =
  //      new LinkedBlockingQueue<>();
  // Parameters
  private final long queueCheckIntervalInMillis;
  private final long normalGroupCloseExpirationInMillis;
  private final long delayedSlotMoveExpirationInMillis;
  private final int numberOfRetentionValues;
  // Executors
  private final ExecutorService normalGroupCloseExecutorService;
  private final ExecutorService delayedSlotMoveExecutorService;
  //  private final ExecutorService delayedGroupEmitExecutorService;
  private final ExecutorService monitorExecutorService;
  // Custom operations injected by the client
  private final KeyManipulator<K> keyManipulator;
  @LazyInit private Emittable<K, V> emitter;
  private final GroupManager groupManager;

  @FunctionalInterface
  private interface GarbageGroupCollector<K, V> {
    void collect(Group<K, V> group);
  }

  // FIXME: This single instance can be a performance bottleneck. Try multi-partitions.
  // This class is just for encapsulation of accesses to Groups
  private class GroupManager {
    // Groups
    @Nullable private NormalGroup<K, V> currentGroup;
    // Using ConcurrentHashMap results in less performance.
    private final Map<K, NormalGroup<K, V>> normalGroupMap = new HashMap<>();
    private final Map<K, DelayedGroup<K, V>> delayedGroupMap = new HashMap<>();
    private final ReentrantReadWriteLock lockOnNormalGroupMap = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock lockOnDelayedGroupMap = new ReentrantReadWriteLock();

    // Returns full key
    private K reserveNewSlot(K childKey) throws GroupCommitAlreadySizeFixedException {
      boolean isNewGroupCreated = false;
      NormalGroup<K, V> oldGroup = null;
      NormalGroup<K, V> newGroup = null;
      try {
        lockOnNormalGroupMap.writeLock().lock();
        if (currentGroup == null || currentGroup.isClosed()) {
          isNewGroupCreated = true;
          oldGroup = currentGroup;
          currentGroup =
              new NormalGroup<>(
                  emitter,
                  keyManipulator,
                  normalGroupCloseExpirationInMillis,
                  delayedSlotMoveExpirationInMillis,
                  numberOfRetentionValues,
                  this::unregisterNormalGroup);
          newGroup = currentGroup;
          // TODO: This can be a faster queue?
          queueForNormalGroupClose.add(currentGroup);
          normalGroupMap.put(currentGroup.key, currentGroup);
        }
      } finally {
        lockOnNormalGroupMap.writeLock().unlock();
      }

      if (isNewGroupCreated) {
        ///////// FIXME: DEBUG
        logger.info("New group:{}, old group:{}, child key:{}", newGroup, oldGroup, childKey);
        ///////// FIXME: DEBUG
      }
      return currentGroup.reserveNewValueSlot(childKey);
    }

    private Group<K, V> getGroup(Keys<K> keys) throws GroupCommitException {
      try {
        lockOnNormalGroupMap.readLock().lock();
        lockOnDelayedGroupMap.readLock().lock();

        DelayedGroup<K, V> delayedGroup =
            delayedGroupMap.get(keyManipulator.createFullKey(keys.parentKey, keys.childKey));
        if (delayedGroup != null) {
          return delayedGroup;
        }

        NormalGroup<K, V> normalGroup = normalGroupMap.get(keys.parentKey);
        if (normalGroup != null) {
          return normalGroup;
        }
      } finally {
        lockOnDelayedGroupMap.readLock().unlock();
        lockOnNormalGroupMap.readLock().unlock();
      }

      throw new GroupCommitTargetNotFoundException(
          "The group for the reserved value slot doesn't exist. keys:" + keys);
    }

    private synchronized void unregisterNormalGroup(Group<K, V> group) {
      try {
        lockOnNormalGroupMap.writeLock().lock();
        normalGroupMap.remove(group.key);
      } finally {
        lockOnNormalGroupMap.writeLock().unlock();
      }
    }

    private synchronized void unregisterDelayedGroup(Group<K, V> group) {
      try {
        lockOnDelayedGroupMap.writeLock().lock();
        delayedGroupMap.remove(group.key);
      } finally {
        lockOnDelayedGroupMap.writeLock().unlock();
      }
    }

    private void moveDelayedSlotToDelayedGroup(NormalGroup<K, V> normalGroup) {
      // Already tried to move this code inside NormalGroup.removeNotReadyValueSlots() to remove
      // the `synchronized` keyword on this method. But the performance was degraded.
      logger.info("[DELAYED-SLOT-MOVE] moveDelayedSlotToDelayedGroup#1 BV:{}", normalGroup);
      try {
        lockOnNormalGroupMap.writeLock().lock();
        lockOnDelayedGroupMap.writeLock().lock();
        List<Slot<K, V>> notReadyValueSlots = normalGroup.removeNotReadyValueSlots();
        for (Slot<K, V> notReadyValueSlot : notReadyValueSlots) {
          K fullKey = notReadyValueSlot.getFullKey();
          DelayedGroup<K, V> delayedGroup =
              new DelayedGroup<>(
                  fullKey,
                  emitter,
                  keyManipulator,
                  normalGroupCloseExpirationInMillis,
                  delayedSlotMoveExpirationInMillis,
                  notReadyValueSlot,
                  this::unregisterDelayedGroup);

          // Delegate the value to the client thread
          notReadyValueSlot.completableFuture.complete(
              () -> {
                try {
                  emitter.execute(fullKey, Collections.singletonList(notReadyValueSlot.value));
                } finally {
                  unregisterDelayedGroup(delayedGroup);
                }
              });

          DelayedGroup<K, V> old = delayedGroupMap.put(fullKey, delayedGroup);
          if (old != null) {
            logger.warn("The slow group value map already has the same key group. {}", old);
          }
        }
        logger.info("[DELAYED-SLOT-MOVE] moveDelayedSlotToDelayedGroup#2 BV:{}", normalGroup);
        if (normalGroup.slots.values().stream().noneMatch(v -> v.value != null)) {
          normalGroupMap.remove(normalGroup.key);
          logger.info("Removed a group as it's empty. normalGroup:{}", normalGroup);
        }
      } finally {
        lockOnDelayedGroupMap.writeLock().unlock();
        lockOnNormalGroupMap.writeLock().unlock();
      }
      logger.info("[DELAYED-SLOT-MOVE] moveDelayedSlotToDelayedGroup#3 BV:{}", normalGroup);
    }
  }

  private static class Slot<K, V> {
    private final NormalGroup<K, V> parentGroup;
    private final K key;
    // If a result value is null, the value is already emitted.
    // Otherwise, the result lambda must be emitted by the receiver's thread.
    private final CompletableFuture<Runnable> completableFuture = new CompletableFuture<>();
    @Nullable private volatile V value;

    public Slot(K key, NormalGroup<K, V> parentGroup) {
      this.key = key;
      this.parentGroup = parentGroup;
    }

    public K getFullKey() {
      return parentGroup.getFullKey(key);
    }

    public void putValue(V value) {
      this.value = Objects.requireNonNull(value);
    }

    public void waitUntilEmit() throws GroupCommitException {
      try {
        // If a result value is null, the value is already emitted.
        // Otherwise, the result lambda must be emitted by the receiver's thread.
        Runnable emittable = completableFuture.get();
        if (emittable != null) {
          // TODO: Enhance the error handling?
          emittable.run();
        }
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
    protected final Emittable<K, V> emitter;
    protected final KeyManipulator<K> keyManipulator;
    private final int capacity;
    private final AtomicReference<Integer> size = new AtomicReference<>();
    protected final K key;
    public final Instant sizeFixedAt;
    public final Instant timeoutAt;
    private final AtomicBoolean done = new AtomicBoolean();
    protected final Map<K, Slot<K, V>> slots;
    // Whether to reject a new value slot.
    protected final AtomicBoolean closed = new AtomicBoolean();
    protected final GarbageGroupCollector<K, V> garbageGroupCollector;

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
          .add("valueSlots.size", slots.size())
          //          .add(
          //              "valueSlots.size(ready)",
          //              valueSlots.values().stream().filter(v -> v.value != null).count())
          .toString();
    }

    Group(
        K key,
        Emittable<K, V> emitter,
        KeyManipulator<K> keyManipulator,
        long sizeFixExpirationInMillis,
        long timeoutExpirationInMillis,
        int capacity,
        GarbageGroupCollector<K, V> garbageGroupCollector) {
      this.emitter = emitter;
      this.keyManipulator = keyManipulator;
      this.capacity = capacity;
      this.sizeFixedAt = Instant.now().plusMillis(sizeFixExpirationInMillis);
      this.timeoutAt = Instant.now().plusMillis(timeoutExpirationInMillis);
      this.key = key;
      this.slots = new HashMap<>(capacity);
      this.garbageGroupCollector = garbageGroupCollector;
    }

    public boolean noMoreSlot() {
      return slots.size() >= capacity;
    }

    protected K reserveNewSlot(Slot<K, V> slot) throws GroupCommitAlreadySizeFixedException {
      return reserveNewSlot(slot, true);
    }

    protected K reserveNewSlot(Slot<K, V> slot, boolean autoEmit)
        throws GroupCommitAlreadySizeFixedException {
      synchronized (this) {
        if (isSizeFixed()) {
          throw new GroupCommitAlreadySizeFixedException(
              "The size of 'valueSlot' is already fixed. Group:" + this);
        }
        reserveSlot(slot);
        ///////// FIXME: DEBUG
        if (noMoreSlot()) {
          fixSize(autoEmit);
        }
      }
      ///////// FIXME: DEBUG
      logger.info("RESERVE:{}, CHILDKEY:{}", this, slot.key);
      return slot.getFullKey();
    }

    private synchronized void reserveSlot(Slot<K, V> slot) {
      // TODO: Check if no existing slot?
      slots.put(slot.key, slot);
      updateIsClosed();
    }

    // This sync is for moving timed-out value slot from a normal buf to a new delayed buf.
    private synchronized Slot<K, V> putValueToSlot(K childKey, V value)
        throws GroupCommitAlreadyClosedException, GroupCommitTargetNotFoundException {
      if (isDone()) {
        throw new GroupCommitAlreadyClosedException("This group is already closed. group:" + this);
      }

      Slot<K, V> slot = slots.get(childKey);
      if (slot == null) {
        throw new GroupCommitTargetNotFoundException(
            "The slot doesn't exist. fullKey:" + keyManipulator.createFullKey(key, childKey));
      }
      slot.putValue(value);
      return slot;
    }

    public void putValueToSlotAndWait(K childKey, V value) throws GroupCommitException {
      Slot<K, V> valueSlot;
      synchronized (this) {
        valueSlot = putValueToSlot(childKey, value);

        // This is in this block since it results in better performance
        asyncEmitIfReady();
      }
      ///////// FIXME: DEBUG
      logger.info("Put value:{}, childKey:{}", this, childKey);
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
        size.set(slots.size());
        updateIsClosed();
        ////// FIXME: DEBUG
        logger.info("Fixed size: group={}", this);
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

    protected int getSize() {
      return size.get();
    }

    public synchronized boolean isReady() {
      if (isSizeFixed()) {
        int readySlotCount = 0;
        for (Slot<K, V> valueSlot : slots.values()) {
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

    protected synchronized void setDone() {
      done.set(true);
      updateIsClosed();
    }

    public void removeValueSlot(K childKey) {
      synchronized (this) {
        if (slots.remove(childKey) != null) {
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
            slots.size(),
            size.get());
        ////// FIXME: DEBUG
        // This is in this block since it results in better performance
        asyncEmitIfReady();
      }
    }

    protected abstract void asyncEmit();

    public synchronized void asyncEmitIfReady() {
      if (isDone()) {
        return;
      }

      if (isReady()) {
        if (slots.isEmpty()) {
          // In this case, each transaction has aborted with the full transaction ID.
          logger.warn("slots are empty. Nothing to do. group:{}", this);
          setDone();
          dismiss();
          return;
        }
        asyncEmit();
        setDone();
      }
    }

    protected void dismiss() {
      garbageGroupCollector.collect(this);
    }
  }

  private static class NormalGroup<K, V> extends Group<K, V> {
    NormalGroup(
        Emittable<K, V> emitter,
        KeyManipulator<K> keyManipulator,
        long sizeFixExpirationInMillis,
        long timeoutExpirationInMillis,
        int capacity,
        GarbageGroupCollector<K, V> garbageGroupCollector) {
      super(
          keyManipulator.createParentKey(),
          emitter,
          keyManipulator,
          sizeFixExpirationInMillis,
          timeoutExpirationInMillis,
          capacity,
          garbageGroupCollector);
    }

    public K getFullKey(K childKey) {
      return keyManipulator.createFullKey(key, childKey);
    }

    public K reserveNewValueSlot(K childKey) throws GroupCommitAlreadySizeFixedException {
      return reserveNewSlot(new Slot<>(childKey, this));
    }

    public synchronized List<Slot<K, V>> removeNotReadyValueSlots() {
      // Lazy instantiation might be better, but it's likely there is a not-ready value slot since
      // it's already timed-out.
      List<Slot<K, V>> removed = new ArrayList<>();
      for (Entry<K, Slot<K, V>> entry : slots.entrySet()) {
        Slot<K, V> valueSlot = entry.getValue();
        K childKey = valueSlot.key;
        if (valueSlot.value == null) {
          removed.add(valueSlot);
        }
      }

      for (Slot<K, V> valueSlot : removed) {
        removeValueSlot(valueSlot.key);
        logger.info(
            "Removed a value slot from group to move it to delayed group. valueSlot:{}", valueSlot);
      }
      return removed;
    }

    @Override
    public synchronized void asyncEmit() {
      ////// FIXME: DEBUG
      logger.info("Delegating emits: group={}", this);
      ////// FIXME: DEBUG

      if (slots.isEmpty()) {
        return;
      }

      final AtomicReference<Slot<K, V>> emitterSlot = new AtomicReference<>();

      boolean isFirst = true;
      List<V> values = new ArrayList<>(slots.size());
      // Avoid using java.util.Collection.stream since it's a bit slow.
      for (Slot<K, V> slot : slots.values()) {
        // Use the first slot as an emitter.
        if (isFirst) {
          isFirst = false;
          emitterSlot.set(slot);
        }
        values.add(slot.value);
      }

      long startDelegate = System.currentTimeMillis();
      Runnable taskForEmitterSlot =
          () -> {
            try {
              logger.info(
                  "Delegated (thread_id:{}, key:{}, num_of_values:{}): {} ms",
                  Thread.currentThread().getId(),
                  key,
                  getSize(),
                  System.currentTimeMillis() - startDelegate);

              long startEmit = System.currentTimeMillis();
              emitter.execute(key, values);
              logger.info(
                  "Emitted (thread_id:{}, key:{}, num_of_values:{}): {} ms",
                  Thread.currentThread().getId(),
                  key,
                  getSize(),
                  System.currentTimeMillis() - startEmit);

              long startNotify = System.currentTimeMillis();
              // Wake up the other waiting threads.
              // Pass null since the value is already emitted by the thread of `firstSlot`.
              for (Slot<K, V> slot : slots.values()) {
                if (slot != emitterSlot.get()) {
                  slot.completableFuture.complete(null);
                }
              }
              logger.info(
                  "Notified (thread_id:{}, num_of_values:{}): {} ms",
                  Thread.currentThread().getId(),
                  getSize(),
                  System.currentTimeMillis() - startNotify);
            } catch (Throwable e) {
              logger.error("Group commit failed", e);
              GroupCommitException exception =
                  new GroupCommitException("Group commit failed. Aborting all the values", e);

              // Let other threads know the exception.
              for (Slot<K, V> slot : slots.values()) {
                if (slot != emitterSlot.get()) {
                  slot.completableFuture.completeExceptionally(exception);
                }
              }

              // Throw the exception for the thread of `firstSlot`.
              throw e;
            } finally {
              dismiss();
            }
          };

      emitterSlot.get().completableFuture.complete(taskForEmitterSlot);
    }
  }

  private static class DelayedGroup<K, V> extends Group<K, V> {
    DelayedGroup(
        K fullKey,
        Emittable<K, V> emitter,
        KeyManipulator<K> keyManipulator,
        long sizeFixExpirationInMillis,
        long timeoutExpirationInMillis,
        Slot<K, V> valueSlot,
        GarbageGroupCollector<K, V> garbageGroupCollector) {
      super(
          fullKey,
          emitter,
          keyManipulator,
          sizeFixExpirationInMillis,
          timeoutExpirationInMillis,
          1,
          garbageGroupCollector);
      try {
        // Auto emit should be disabled since:
        // - the queue and worker for delayed values will emit this if it's ready
        // - to avoid taking time in synchronized blocks
        super.reserveNewSlot(valueSlot, false);
      } catch (GroupCommitAlreadySizeFixedException e) {
        // FIXME Message
        throw new IllegalStateException(
            "Failed to reserve a value slot. This shouldn't happen. valueSlot:" + valueSlot, e);
      }
    }

    @Override
    protected void asyncEmit() {
      for (Entry<K, Slot<K, V>> entry : slots.entrySet()) {
        Slot<K, V> slot = entry.getValue();
        // Pass `emitter` to ask the receiver's thread to emit the value
        slot.completableFuture.complete(
            () -> emitter.execute(key, Collections.singletonList(slot.value)));
        // The number of the slots is only 1.
        dismiss();
        return;
      }
    }
  }

  public GroupCommitter3WithReentrantLock(
      String label,
      long sizeFixExpirationInMillis,
      long timeoutExpirationInMillis,
      int numberOfRetentionValues,
      long expirationCheckIntervalInMillis,
      KeyManipulator<K> keyManipulator) {
    this.normalGroupCloseExpirationInMillis = sizeFixExpirationInMillis;
    this.delayedSlotMoveExpirationInMillis = timeoutExpirationInMillis;
    this.numberOfRetentionValues = numberOfRetentionValues;
    this.queueCheckIntervalInMillis = expirationCheckIntervalInMillis;
    this.keyManipulator = keyManipulator;
    this.groupManager = new GroupManager();

    this.monitorExecutorService =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(label + "-group-commit-monitor-%d")
                .build());
    startMonitorExecutorService();

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

    /*
    this.delayedGroupEmitExecutorService =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(label + "-group-commit-delayed-group-emit-%d")
                .build());
    startDelayedGroupEmitExecutorService();
     */
  }

  public void setEmitter(Emittable<K, V> emitter) {
    this.emitter = emitter;
  }

  private boolean handleQueueForNormalGroupClose() {
    NormalGroup<K, V> normalGroup = queueForNormalGroupClose.peek();
    ////////// FIXME: DEBUG LOG
    logger.info("[NORMAL-GROUP-CLOSE] Fetched group={}", normalGroup);
    ////////// FIXME: DEBUG LOG

    Long retryWaitInMillis = null;

    if (normalGroup == null) {
      retryWaitInMillis = queueCheckIntervalInMillis;
    } else if (normalGroup.isSizeFixed()) {
      // Already the size is fixed. Nothing to do. Handle a next element immediately
      ////////// FIXME: DEBUG LOG
      if (normalGroup.sizeFixedAt.isBefore(Instant.now().minusMillis(5000))) {
        logger.info(
            "[NORMAL-GROUP-CLOSE] Too old group: group.key={}, group.values={}",
            normalGroup.key,
            normalGroup.slots);
      }
      ////////// FIXME: DEBUG LOG
    } else {
      Instant now = Instant.now();
      if (now.isAfter(normalGroup.sizeFixedAt)) {
        // Expired. Fix the size
        normalGroup.fixSize();
      } else {
        // Not expired. Retry
        retryWaitInMillis = queueCheckIntervalInMillis;
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
      logger.info("[NORMAL-GROUP-CLOSE] Fetched group={}", normalGroup);
      ////////// FIXME: DEBUG LOG
      // Move the size-fixed group but not ready to the timeout queue
      if (!normalGroup.isReady()) {
        queueForDelayedSlotMove.add(normalGroup);
      }
      NormalGroup<K, V> removed = queueForNormalGroupClose.poll();
      // Check if the removed group is expected just in case.
      if (removed == null || !removed.equals(normalGroup)) {
        logger.error(
            "The queue for size-fix returned an inconsistent return value. expected:{}, actual:{}",
            normalGroup,
            removed);
        if (removed != null) {
          queueForNormalGroupClose.add(removed);
        }
      }
    }
    return true;
  }

  private boolean handleQueueForDelayedSlotMove() {
    NormalGroup<K, V> normalGroup = queueForDelayedSlotMove.peek();
    Long retryWaitInMillis = null;

    ////////// FIXME: DEBUG LOG
    logger.info(
        "[DELAYED-SLOT-MOVE] New group:{}, size:{}", normalGroup, queueForDelayedSlotMove.size());

    if (normalGroup == null) {
      retryWaitInMillis = queueCheckIntervalInMillis * 2;
    } else if (normalGroup.isReady()) {
      // Already ready. Nothing to do. Handle a next element immediately
    } else {
      Instant now = Instant.now();
      if (now.isAfter(normalGroup.timeoutAt)) {
        ////////// FIXME: DEBUG LOG
        long start = System.currentTimeMillis();
        groupManager.moveDelayedSlotToDelayedGroup(normalGroup);
        logger.info(
            "[DELAYED-SLOT-MOVE] Moved group:{} to delayed group, duration:{}ms, size:{}",
            normalGroup,
            (System.currentTimeMillis() - start),
            queueForDelayedSlotMove.size());
      } else {
        // Not expired. Retry
        retryWaitInMillis = queueCheckIntervalInMillis * 2;
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
      logger.info("[DELAYED-SLOT-MOVE] Removing fetched group: group={}", normalGroup);
      ////////// FIXME: DEBUG LOG
      NormalGroup<K, V> removed = queueForDelayedSlotMove.poll();
      // Check if the removed slot is expected just in case.
      if (removed == null || !removed.equals(normalGroup)) {
        logger.error(
            "The queue for timeout returned an inconsistent return value. expected:{}, actual:{}",
            normalGroup,
            removed);
        if (removed != null) {
          queueForDelayedSlotMove.add(removed);
        }
      }
    }
    return true;
  }

  /*
  private boolean handleQueueForDelayedGroupEmit() {
    DelayedGroup<K, V> delayedGroup = queueForDelayedGroupEmit.peek();
    Long waitInMillis = queueCheckIntervalInMillis;

    ////////// FIXME: DEBUG LOG
    logger.info("[DELAYED-GROUP-EMIT] Fetched group={}", delayedGroup);
    ////////// FIXME: DEBUG LOG
    if (delayedGroup == null) {
      // The queue is empty, so wait for a longer time.
      waitInMillis = queueCheckIntervalInMillis * 2;
    } else {
      DelayedGroup<K, V> removed = queueForDelayedGroupEmit.poll();
      // Check if the removed group is expected just in case.
      if (removed == null || !removed.equals(delayedGroup)) {
        logger.error(
            "The queue for delayed values returned an inconsistent return value. expected:{}, actual:{}",
            delayedGroup,
            removed);
        if (removed != null) {
          queueForDelayedGroupEmit.add(removed);
        }
        return true;
      } else if (delayedGroup.isReady()) {
        // Send the ready group asynchronously and check the result later.
        delayedGroup.asyncEmitIfReady();
      } else if (delayedGroup.isDone()) {
        // Don't need retries.
        return true;
      }
      // Group in the queue for delayed ones could contain very delayed ones.
      // Those delayed ones should be handled later.
      queueForDelayedGroupEmit.add(delayedGroup);
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
   */

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

  /*
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
   */

  private void startMonitorExecutorService() {
    monitorExecutorService.execute(
        () -> {
          while (!monitorExecutorService.isShutdown()) {
            logger.info(
                // "[MONITOR] Timestamp={}, NormalGroupClose.queue.size={},
                // DelayedSlotMove.queue.size={}, DelayedGroupEmit.queue.size={},
                // NormalGroupMap.size={}, DelayedGroupMap.size={}",
                "[MONITOR] Timestamp={}, NormalGroupClose.queue.size={}, DelayedSlotMove.queue.size={}, NormalGroupMap.size={}, DelayedGroupMap.size={}",
                Instant.now(),
                queueForNormalGroupClose.size(),
                queueForDelayedSlotMove.size(),
                // queueForDelayedGroupEmit.size(),
                groupManager.normalGroupMap.size(),
                groupManager.delayedGroupMap.size());
            try {
              TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              logger.warn("Interrupted", e);
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
        return groupManager.reserveNewSlot(childKey);
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
        logger.error("Failed to reserve slot #2: Unexpected key={}", childKey);
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
      Group<K, V> group = groupManager.getGroup(keys);
      try {
        group.putValueToSlotAndWait(keys.childKey, value);
        return;
      } catch (GroupCommitAlreadyClosedException | GroupCommitTargetNotFoundException e) {
        // This can throw an exception in a race condition when the value slot is moved to
        // delayed group. So, retry should be needed.
        if (group instanceof GroupCommitter3WithReentrantLock.NormalGroup) {
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
    Group<K, V> group = groupManager.getGroup(keys);
    group.removeValueSlot(keys.childKey);
  }

  // The ExecutorServices are created as daemon, so calling this method isn't needed.
  // But for testing, this should be called for resources.
  @Override
  public void close() {
    /*
    if (delayedGroupEmitExecutorService != null) {
      MoreExecutors.shutdownAndAwaitTermination(
          delayedGroupEmitExecutorService, 5, TimeUnit.SECONDS);
    }
     */
    if (delayedSlotMoveExecutorService != null) {
      MoreExecutors.shutdownAndAwaitTermination(
          delayedSlotMoveExecutorService, 5, TimeUnit.SECONDS);
    }
    if (normalGroupCloseExecutorService != null) {
      MoreExecutors.shutdownAndAwaitTermination(
          normalGroupCloseExecutorService, 5, TimeUnit.SECONDS);
    }
    if (monitorExecutorService != null) {
      MoreExecutors.shutdownAndAwaitTermination(monitorExecutorService, 5, TimeUnit.SECONDS);
    }
  }
}
