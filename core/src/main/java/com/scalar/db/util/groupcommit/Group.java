package com.scalar.db.util.groupcommit;

import com.google.common.base.MoreObjects;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class Group<K, V> {
  private static final Logger logger = LoggerFactory.getLogger(NormalGroup.class);

  protected final Emittable<K, V> emitter;
  protected final KeyManipulator<K> keyManipulator;
  private final int capacity;
  private final AtomicReference<Integer> size = new AtomicReference<>();
  protected final K key;
  private final long moveDelayedSlotExpirationInMillis;
  private final Instant groupClosedAt;
  private final AtomicReference<Instant> delayedSlotMovedAt;
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
        .add("groupClosedAt", groupClosedAt)
        .add("delayedSlotMovedAt", delayedSlotMovedAt)
        .add("done", isDone())
        .add("ready", isReady())
        .add("sizeFixed", isSizeFixed())
        .add("valueSlots.size", slots.size())
        .toString();
  }

  Group(
      K key,
      Emittable<K, V> emitter,
      KeyManipulator<K> keyManipulator,
      long groupCloseExpirationInMillis,
      long moveDelayedSlotExpirationInMillis,
      int capacity,
      GarbageGroupCollector<K, V> garbageGroupCollector) {
    this.emitter = emitter;
    this.keyManipulator = keyManipulator;
    this.capacity = capacity;
    this.moveDelayedSlotExpirationInMillis = moveDelayedSlotExpirationInMillis;
    this.groupClosedAt = Instant.now().plusMillis(groupCloseExpirationInMillis);
    this.delayedSlotMovedAt = new AtomicReference<>();
    updateDelayedSlotMovedAt();
    this.key = key;
    this.slots = new HashMap<>(capacity);
    this.garbageGroupCollector = garbageGroupCollector;
  }

  void updateDelayedSlotMovedAt() {
    delayedSlotMovedAt.set(Instant.now().plusMillis(moveDelayedSlotExpirationInMillis));
  }

  boolean noMoreSlot() {
    return slots.size() >= capacity;
  }

  K reserveNewSlot(Slot<K, V> slot) throws GroupCommitAlreadyClosedException {
    return reserveNewSlot(slot, true);
  }

  protected K reserveNewSlot(Slot<K, V> slot, boolean autoEmit)
      throws GroupCommitAlreadyClosedException {
    synchronized (this) {
      if (isSizeFixed()) {
        throw new GroupCommitAlreadyClosedException(
            "The size of 'slot' is already fixed. Group:" + this);
      }
      reserveSlot(slot);
      ///////// FIXME: DEBUG
      if (noMoreSlot()) {
        fixSize(autoEmit);
      }
    }
    ///////// FIXME: DEBUG
    logger.info("RESERVE:{}, CHILDKEY:{}", this, slot.getKey());
    return slot.getFullKey();
  }

  private synchronized void reserveSlot(Slot<K, V> slot) {
    // TODO: Check if no existing slot?
    slots.put(slot.getKey(), slot);
    updateIsClosed();
  }

  // This sync is for moving timed-out value slot from a normal buf to a new delayed buf.
  private synchronized Slot<K, V> putValueToSlot(K childKey, V value)
      throws GroupCommitAlreadyCompletedException, GroupCommitTargetNotFoundException {
    if (isDone()) {
      throw new GroupCommitAlreadyCompletedException("This group is already closed. group:" + this);
    }

    Slot<K, V> slot = slots.get(childKey);
    if (slot == null) {
      throw new GroupCommitTargetNotFoundException(
          "The slot doesn't exist. fullKey:" + keyManipulator.createFullKey(key, childKey));
    }
    slot.putValue(value);
    return slot;
  }

  void putValueToSlotAndWait(K childKey, V value) throws GroupCommitException {
    Slot<K, V> slot;
    synchronized (this) {
      slot = putValueToSlot(childKey, value);

      // This is in this block since it results in better performance
      asyncEmitIfReady();
    }

    long start = System.currentTimeMillis();
    slot.waitUntilEmit();

    logger.info(
        "Waited(thread_id:{}, parentKey:{}, childKey:{}): {} ms",
        Thread.currentThread().getId(),
        key,
        childKey,
        System.currentTimeMillis() - start);
  }

  Instant groupClosedAt() {
    return groupClosedAt;
  }

  Instant delayedSlotMovedAt() {
    return delayedSlotMovedAt.get();
  }

  void fixSize() {
    fixSize(true);
  }

  void fixSize(boolean autoEmit) {
    synchronized (this) {
      // Current Slot that `index` is pointing is not used yet.
      size.set(slots.size());
      updateIsClosed();
      if (autoEmit) {
        asyncEmitIfReady();
      }
    }
  }

  boolean isSizeFixed() {
    return size.get() != null;
  }

  int getSize() {
    return size.get();
  }

  synchronized boolean isReady() {
    if (isSizeFixed()) {
      int readySlotCount = 0;
      for (Slot<K, V> slot : slots.values()) {
        if (slot.getValue() != null) {
          readySlotCount++;
          if (readySlotCount >= size.get()) {
            return true;
          }
        }
      }
    }
    return false;
  }

  boolean isDone() {
    return done.get();
  }

  boolean isClosed() {
    return closed.get();
  }

  synchronized void updateIsClosed() {
    closed.set(noMoreSlot() || isDone() || isSizeFixed());
  }

  protected synchronized void setDone() {
    done.set(true);
    updateIsClosed();
  }

  void removeSlot(K childKey) {
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

  synchronized void asyncEmitIfReady() {
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
