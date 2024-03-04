package com.scalar.db.util.groupcommit;

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

  static class GroupStatus {
    public final boolean isClosed;
    public final boolean isReady;
    public final boolean isDone;

    public GroupStatus(boolean isClosed, boolean isReady, boolean isDone) {
      this.isClosed = isClosed;
      this.isReady = isReady;
      this.isDone = isDone;
    }
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

  abstract FULL_KEY fullKey(CHILD_KEY childKey);

  // If it returns null, the Group is already closed and a retry is needed.
  @Nullable
  protected synchronized FULL_KEY reserveNewSlot(
      Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> slot) {
    if (isSizeFixed()) {
      return null;
    }
    reserveSlot(slot);
    if (noMoreSlot()) {
      fixSize();
    }
    return slot.fullKey();
  }

  private void reserveSlot(Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> slot) {
    Slot<?, ?, ?, ?, ?> oldSlot = slots.put(slot.key(), slot);
    if (oldSlot != null) {
      logger.warn("An old slot exist unexpectedly. {}", oldSlot.fullKey());
    }
    updateIsClosed();
  }

  // This sync is for moving timed-out value slot from a normal buf to a new delayed buf.
  @Nullable
  // Returns null if the state of the group is changed.
  private synchronized Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> putValueToSlot(
      CHILD_KEY childKey, V value) {
    if (isDone()) {
      return null;
    }

    Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> slot = slots.get(childKey);
    if (slot == null) {
      return null;
    }
    slot.setValue(value);
    return slot;
  }

  boolean putValueToSlotAndWait(CHILD_KEY childKey, V value) throws GroupCommitException {
    Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> slot;
    synchronized (this) {
      slot = putValueToSlot(childKey, value);
      if (slot == null) {
        // Needs a retry since the state of the group is changed.
        return false;
      }

      asyncEmitIfReady();
    }
    slot.waitUntilEmit();
    return true;
  }

  synchronized GroupStatus status() {
    return new GroupStatus(isClosed(), isReady(), isDone());
  }

  void fixSize() {
    synchronized (this) {
      // Current Slot that `index` is pointing is not used yet.
      size.set(slots.size());
      updateIsClosed();
      asyncEmitIfReady();
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
        }
      }
      return readySlotCount >= size.get();
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
    size.set(slots.size());
    done.set(true);
    updateIsClosed();
  }

  synchronized boolean removeSlot(CHILD_KEY childKey) {
    boolean removed = false;
    if (slots.remove(childKey) != null) {
      removed = true;
      if (size.get() != null && size.get() > 0) {
        size.set(size.get() - 1);
      }
      updateIsClosed();
    }

    asyncEmitIfReady();
    return removed;
  }

  protected abstract void asyncEmit();

  synchronized void asyncEmitIfReady() {
    // Must not return even if the group is done since all the client threads need to get the result
    // from the slot.

    if (slots.isEmpty()) {
      markAsDone();
      return;
    }

    if (isReady()) {
      try {
        asyncEmit();
      } finally {
        markAsDone();
      }
    }
  }
}
