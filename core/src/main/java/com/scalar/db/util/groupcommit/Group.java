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
abstract class Group<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V> {
  private static final Logger logger = LoggerFactory.getLogger(Group.class);

  protected final Emittable<EMIT_PARENT_KEY, EMIT_FULL_KEY, V> emitter;
  protected final KeyManipulator<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY>
      keyManipulator;
  private final int capacity;
  private final AtomicReference<Integer> size = new AtomicReference<>();
  protected final Map<
          CHILD_KEY, Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V>>
      slots;
  // Whether to reject a new value slot.
  protected final AtomicReference<Status> status = new AtomicReference<>(Status.OPEN);
  private final long oldGroupAbortTimeoutAtMillis;

  // Status of the group.
  enum Status {
    // Accepting new slot reservation since the number of slots isn't fixed yet.
    //
    // Initial status of groups. No group can move back to OPEN.
    OPEN(false, false, false),

    // Not accepting new slot reservations since the number of slots is already fixed.
    // Waiting all the slots are set with values.
    //
    // Groups with OPEN status can move to SIZE_FIXED.
    SIZE_FIXED(true, false, false),

    // All the slots are set with values. Ready to commit.
    //
    // Groups with OPEN or SIZE_FIXED status can move to READY.
    READY(true, true, false),

    // Group commit is done and all the clients have get the results.
    //
    // Groups with OPEN, SIZE_FIXED or READY status can move to DONE.
    DONE(true, true, true);

    final boolean isSizeFixed;
    final boolean isReady;
    final boolean isDone;

    Status(boolean isSizeFixed, boolean isReady, boolean isDone) {
      this.isSizeFixed = isSizeFixed;
      this.isReady = isReady;
      this.isDone = isDone;
    }
  }

  Group(
      Emittable<EMIT_PARENT_KEY, EMIT_FULL_KEY, V> emitter,
      KeyManipulator<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY>
          keyManipulator,
      int capacity,
      long oldGroupAbortTimeoutMillis) {
    this.emitter = emitter;
    this.keyManipulator = keyManipulator;
    this.capacity = capacity;
    this.slots = new HashMap<>(capacity);
    this.oldGroupAbortTimeoutAtMillis = System.currentTimeMillis() + oldGroupAbortTimeoutMillis;
  }

  private boolean noMoreSlot() {
    return slots.size() >= capacity;
  }

  abstract FULL_KEY fullKey(CHILD_KEY childKey);

  // If it returns null, the Group is already size-fixed and a retry is needed.
  @Nullable
  protected synchronized FULL_KEY reserveNewSlot(
      Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V> slot) {
    if (isSizeFixed()) {
      return null;
    }
    reserveSlot(slot);
    if (noMoreSlot()) {
      fixSize();
    }
    return slot.fullKey();
  }

  private void reserveSlot(
      Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V> slot) {
    Slot<?, ?, ?, ?, ?, ?> oldSlot = slots.put(slot.key(), slot);
    if (oldSlot != null) {
      throw new AssertionError(
          String.format("An old slot exist unexpectedly. Slot: %s, Old slot: %s", slot, oldSlot));
    }
    updateStatus();
  }

  // This sync is for moving timed-out value slot from a normal buf to a new delayed buf.
  // Returns null if the state of the group is changed (e.g., the slot is moved to another group).
  @Nullable
  private synchronized Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V>
      putValueToSlot(CHILD_KEY childKey, V value) {
    if (isReady()) {
      logger.debug(
          "This group is already ready, but trying to put a value to the slot. Probably the slot is moved to a DelayedGroup. Retrying... Group: {}, Child key: {}",
          this,
          childKey);
      return null;
    }

    Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V> slot =
        slots.get(childKey);
    if (slot == null) {
      return null;
    }
    slot.setValue(value);
    return slot;
  }

  boolean putValueToSlotAndWait(CHILD_KEY childKey, V value) throws GroupCommitException {
    Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V> slot;
    synchronized (this) {
      slot = putValueToSlot(childKey, value);
      if (slot == null) {
        // Needs a retry since the state of the group is changed.
        return false;
      }
      updateStatus();

      delegateEmitTaskToWaiterIfReady();
    }
    slot.waitUntilEmit();
    return true;
  }

  private synchronized void updateSlotsSize() {
    size.set(slots.size());
  }

  synchronized void fixSize() {
    updateSlotsSize();
    updateStatus();
    delegateEmitTaskToWaiterIfReady();
  }

  @Nullable
  protected Integer size() {
    return size.get();
  }

  boolean isSizeFixed() {
    return status.get().isSizeFixed;
  }

  boolean isReady() {
    return status.get().isReady;
  }

  boolean isDone() {
    return status.get().isDone;
  }

  synchronized void updateStatus() {
    if (slots.isEmpty()) {
      updateSlotsSize();
      status.set(Status.DONE);
      return;
    }

    Status newStatus = status.get();

    if (newStatus == Status.OPEN) {
      if (size.get() != null) {
        newStatus = Status.SIZE_FIXED;
      }
    }

    if (newStatus == Status.SIZE_FIXED) {
      int readySlotCount = 0;
      for (Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V> slot :
          slots.values()) {
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
      for (Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V> slot :
          slots.values()) {
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
    Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V> slot =
        slots.get(childKey);
    if (slot == null) {
      // Probably, the slot is already removed by the client or moved from NormalGroup to
      // DelayedGroup.
      return false;
    }

    if (slot.isReady()) {
      // Technically, it's possible to remove the ready slot from the group since the client thread
      // is waiting on the slot already. But removing it might cause more complicated state,
      // so leave it as-is.
      logger.debug(
          "Attempted to remove this slot, but it will not be removed because it is already ready. Group: {}, Slot: {}",
          this,
          slot);
      return false;
    }

    Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V> removed =
        slots.remove(childKey);
    assert removed != null;

    if (size.get() != null && size.get() > 0) {
      size.set(size.get() - 1);
    }
    updateStatus();
    delegateEmitTaskToWaiterIfReady();

    return true;
  }

  synchronized void abort() {
    for (Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V> slot :
        slots.values()) {
      // Tell the clients that the slots are aborted.
      slot.markAsFailed(
          new GroupCommitException(
              String.format(
                  "The slot in the old group is timed out and aborted. Group:%s, Slot:%s",
                  this, slot)));
    }
  }

  protected abstract void delegateEmitTaskToWaiter();

  synchronized void delegateEmitTaskToWaiterIfReady() {
    if (isDone()) {
      return;
    }

    if (isReady()) {
      try {
        delegateEmitTaskToWaiter();
      } finally {
        updateStatus();
      }
    }
  }

  public long oldGroupAbortTimeoutAtMillis() {
    return oldGroupAbortTimeoutAtMillis;
  }
}
