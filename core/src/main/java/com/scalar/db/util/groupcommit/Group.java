package com.scalar.db.util.groupcommit;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// An abstract class that has logics and implementations to manage slots and trigger to emit it.
@ThreadSafe
abstract class Group<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> {
  private static final Logger logger = LoggerFactory.getLogger(Group.class);

  protected final Emittable<EMIT_KEY, V> emitter;
  protected final KeyManipulator<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY> keyManipulator;
  private final int capacity;
  private final AtomicReference<Integer> size = new AtomicReference<>();
  protected final Map<CHILD_KEY, Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>> slots;
  // Whether to reject a new value slot.
  protected final AtomicReference<Status> status = new AtomicReference<>(Status.OPEN);

  // Status of the group.
  enum Status {
    // Accepting new slot reservation since the number of slots isn't fixed yet.
    //
    // Initial status of groups. No group can move back to OPEN.
    OPEN(false, false, false),

    // Not accepting new slot reservations since the number of slots is already fixed.
    // Waiting all the slots are set with values.
    //
    // Groups with OPEN status can move to CLOSED.
    CLOSED(true, false, false),

    // All the slots are set with values. Ready to commit.
    //
    // Groups with OPEN or CLOSED status can move to READY.
    READY(true, true, false),

    // Group commit is done and all the clients have get the results.
    //
    // Groups with OPEN, CLOSED or READY status can move to DONE.
    DONE(true, true, true);

    final boolean isClosed;
    final boolean isReady;
    final boolean isDone;

    Status(boolean isClosed, boolean isReady, boolean isDone) {
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

  private boolean noMoreSlot() {
    return slots.size() >= capacity;
  }

  abstract FULL_KEY fullKey(CHILD_KEY childKey);

  // If it returns null, the Group is already closed and a retry is needed.
  @Nullable
  protected synchronized FULL_KEY reserveNewSlot(
      Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> slot) {
    if (isClosed()) {
      return null;
    }
    reserveSlot(slot);
    if (noMoreSlot()) {
      close();
    }
    return slot.fullKey();
  }

  private void reserveSlot(Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> slot) {
    Slot<?, ?, ?, ?, ?> oldSlot = slots.put(slot.key(), slot);
    if (oldSlot != null) {
      throw new AssertionError(
          String.format("An old slot exist unexpectedly. Slot: %s, Old slot: %s", slot, oldSlot));
    }
    updateStatus();
  }

  // This sync is for moving timed-out value slot from a normal buf to a new delayed buf.
  // Returns null if the state of the group is changed (e.g., the slot is moved to another group).
  @Nullable
  private synchronized Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> putValueToSlot(
      CHILD_KEY childKey, V value) {
    if (isReady()) {
      logger.debug(
          "This group is already ready, but trying to put a value to the slot. Probably the slot is moved to a DelayedGroup. Retrying... Group: {}, Child key: {}",
          this,
          childKey);
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
      updateStatus();

      asyncEmitIfReady();
    }
    slot.waitUntilEmit();
    return true;
  }

  private synchronized void fixSize() {
    size.set(slots.size());
  }

  synchronized void close() {
    fixSize();
    updateStatus();
    asyncEmitIfReady();
  }

  @Nullable
  protected Integer size() {
    return size.get();
  }

  boolean isClosed() {
    return status.get().isClosed;
  }

  boolean isReady() {
    return status.get().isReady;
  }

  boolean isDone() {
    return status.get().isDone;
  }

  synchronized void updateStatus() {
    if (slots.isEmpty()) {
      fixSize();
      status.set(Status.DONE);
      return;
    }

    Status newStatus = status.get();

    if (newStatus == Status.OPEN) {
      if (size.get() != null) {
        newStatus = Status.CLOSED;
      }
    }

    if (newStatus == Status.CLOSED) {
      int readySlotCount = 0;
      for (Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> slot : slots.values()) {
        if (slot.isReady()) {
          readySlotCount++;
        }
      }
      if (readySlotCount >= size.get()) {
        newStatus = Status.READY;
      }
    }

    if (newStatus == Status.READY) {
      int doneSlotCount = 0;
      for (Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> slot : slots.values()) {
        if (slot.isDone()) {
          doneSlotCount++;
        }
      }
      if (doneSlotCount >= size.get()) {
        newStatus = Status.DONE;
      }
    }
    status.set(newStatus);
  }

  synchronized boolean removeSlot(CHILD_KEY childKey) {
    Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> slot = slots.get(childKey);
    if (slot == null) {
      return false;
    }

    if (slot.isReady()) {
      // Actually, the ready slot can be removed from the group since the client thread is waiting
      // on the slot already. But removing it might cause more complicated state, so leave it as-is.
      logger.debug(
          "Attempted to remove this slot, but it will not be removed because it is already ready. Group: {}, Slot: {}",
          this,
          slot);
      return false;
    }

    Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> removed = slots.remove(childKey);
    assert removed != null;

    if (size.get() != null && size.get() > 0) {
      size.set(size.get() - 1);
    }
    updateStatus();
    asyncEmitIfReady();

    return true;
  }

  protected abstract void asyncEmit();

  synchronized void asyncEmitIfReady() {
    if (isDone()) {
      return;
    }

    if (isReady()) {
      try {
        asyncEmit();
      } finally {
        updateStatus();
      }
    }
  }
}
