package com.scalar.db.util.groupcommit;

import com.google.common.base.MoreObjects;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class Group<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> {
  private static final Logger logger = LoggerFactory.getLogger(NormalGroup.class);

  protected final Emittable<EMIT_KEY, V> emitter;
  protected final KeyManipulator<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY> keyManipulator;
  private final int capacity;
  private final AtomicReference<Integer> size = new AtomicReference<>();
  private final AtomicBoolean done = new AtomicBoolean();
  protected final Map<CHILD_KEY, Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>> slots;
  // Whether to reject a new value slot.
  protected final AtomicBoolean closed = new AtomicBoolean();

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("hashCode", hashCode())
        .add("done", isDone())
        .add("ready", isReady())
        .add("sizeFixed", isSizeFixed())
        .add("valueSlots.size", slots.size())
        .toString();
  }

  Group(
      Emittable<EMIT_KEY, V> emitter,
      KeyManipulator<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY> keyManipulator,
      int capacity) {
    this.emitter = emitter;
    this.keyManipulator = keyManipulator;
    this.capacity = capacity;
    this.slots = new HashMap<>(capacity);
  }

  boolean noMoreSlot() {
    return slots.size() >= capacity;
  }

  // If it returns null, the Group is already closed and a retry is needed.
  @Nullable
  FULL_KEY reserveNewSlot(Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> slot) {
    return reserveNewSlot(slot, true);
  }

  abstract FULL_KEY fullKey(CHILD_KEY childKey);

  // If it returns null, the Group is already closed and a retry is needed.
  @Nullable
  protected FULL_KEY reserveNewSlot(
      Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> slot, boolean autoEmit) {
    synchronized (this) {
      if (isSizeFixed()) {
        return null;
      }
      reserveSlot(slot);
      if (noMoreSlot()) {
        fixSize(autoEmit);
      }
    }
    return slot.fullKey();
  }

  private synchronized void reserveSlot(Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> slot) {
    Slot<?, ?, ?, ?, ?> oldSlot = slots.put(slot.key(), slot);
    if (oldSlot != null) {
      logger.warn("An old slot exist unexpectedly. {}", oldSlot.fullKey());
    }
    updateIsClosed();
  }

  // This sync is for moving timed-out value slot from a normal buf to a new delayed buf.
  private synchronized Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> putValueToSlot(
      CHILD_KEY childKey, V value)
      throws GroupCommitAlreadyCompletedException, GroupCommitTargetNotFoundException {
    if (isDone()) {
      throw new GroupCommitAlreadyCompletedException("This group is already closed. group:" + this);
    }

    Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> slot = slots.get(childKey);
    if (slot == null) {
      throw new GroupCommitTargetNotFoundException(
          "The slot doesn't exist. fullKey:" + fullKey(childKey));
    }
    slot.setValue(value);
    return slot;
  }

  void putValueToSlotAndWait(CHILD_KEY childKey, V value) throws GroupCommitException {
    Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> slot;
    synchronized (this) {
      slot = putValueToSlot(childKey, value);

      // This is in this block since it results in better performance
      asyncEmitIfReady();
    }

    slot.waitUntilEmit();
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

  int size() {
    return size.get();
  }

  synchronized boolean isReady() {
    if (isSizeFixed()) {
      int readySlotCount = 0;
      for (Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> slot : slots.values()) {
        if (slot.value() != null) {
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

  protected synchronized void markAsDone() {
    done.set(true);
    updateIsClosed();
  }

  void removeSlot(CHILD_KEY childKey) {
    synchronized (this) {
      if (slots.remove(childKey) != null) {
        if (size.get() != null && size.get() > 0) {
          size.set(size.get() - 1);
        }
        updateIsClosed();
      }
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
        markAsDone();
        dismiss();
        return;
      }
      asyncEmit();
      markAsDone();
    }
  }

  protected abstract void dismiss();
}
