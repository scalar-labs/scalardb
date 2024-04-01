package com.scalar.db.util.groupcommit;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// A group for multiple slots that will be group-committed at once.
@ThreadSafe
class NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>
    extends Group<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> {
  private static final Logger logger = LoggerFactory.getLogger(NormalGroup.class);

  private final PARENT_KEY parentKey;
  private final int delayedSlotMoveTimeoutMillis;
  private final long groupSizeFixTimeoutAtMillis;
  private final AtomicLong delayedSlotMoveTimeoutAtMillis = new AtomicLong();

  NormalGroup(
      GroupCommitConfig config,
      Emittable<EMIT_KEY, V> emitter,
      KeyManipulator<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY> keyManipulator) {
    super(emitter, keyManipulator, config.slotCapacity(), config.oldGroupAbortTimeoutSeconds());
    this.delayedSlotMoveTimeoutMillis = config.delayedSlotMoveTimeoutMillis();
    this.groupSizeFixTimeoutAtMillis =
        System.currentTimeMillis() + config.groupSizeFixTimeoutMillis();
    updateDelayedSlotMoveTimeoutAt();
    this.parentKey = keyManipulator.generateParentKey();
  }

  PARENT_KEY parentKey() {
    return parentKey;
  }

  @Override
  FULL_KEY fullKey(CHILD_KEY childKey) {
    return keyManipulator.fullKey(parentKey, childKey);
  }

  // If it returns null, the Group is already size-fixed and a retry is needed.
  @Nullable
  FULL_KEY reserveNewSlot(CHILD_KEY childKey) {
    return reserveNewSlot(new Slot<>(childKey, this));
  }

  @Nullable
  synchronized List<Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>> removeNotReadySlots() {
    if (!isSizeFixed()) {
      logger.info(
          "No need to remove any slot since the size isn't fixed yet. Too early. Group: {}", this);
      return null;
    }

    // Lazy instantiation might be better, but it's likely there is a not-ready value slot since
    // it's already timed-out.
    List<Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>> removed = new ArrayList<>();
    for (Entry<CHILD_KEY, Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>> entry :
        slots.entrySet()) {
      Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> slot = entry.getValue();
      if (!slot.isReady()) {
        removed.add(slot);
      }
    }

    // The size must be already fixed since the group is already size-fixed.
    Integer size = size();
    assert size != null;
    if (removed.size() >= size) {
      logger.debug("No need to remove any slot since all the slots are not ready. Group: {}", this);
      return null;
    }

    for (Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> slot : removed) {
      removeSlot(slot.key());
      logger.debug(
          "Removed a value slot from group to move it to delayed group. Group: {}, Slot: {}",
          this,
          slot);
    }
    return removed;
  }

  @Override
  public synchronized void asyncEmit() {
    final AtomicReference<Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>> emitterSlot =
        new AtomicReference<>();

    boolean isFirst = true;
    List<V> values = new ArrayList<>(slots.size());
    for (Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> slot : slots.values()) {
      // Use the first slot as an emitter.
      if (isFirst) {
        isFirst = false;
        emitterSlot.set(slot);
      }
      values.add(slot.value());
    }

    // This task is passed to only the first slot, so the slot will be resumed.
    // Other slots will be blocked until `markAsXxxx()` is called.
    ThrowableRunnable taskForEmitterSlot =
        () -> {
          try {
            if (isDone()) {
              logger.info("This group is already done, but trying to emit. Group: {}", this);
              return;
            }
            emitter.execute(keyManipulator.emitKeyFromParentKey(parentKey), values);

            synchronized (this) {
              // Wake up the other waiting threads.
              // Pass null since the value is already emitted by the thread of `firstSlot`.
              for (Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> slot : slots.values()) {
                if (slot != emitterSlot.get()) {
                  slot.markAsSuccess();
                }
              }
            }
          } catch (Exception e) {
            GroupCommitException exception =
                new GroupCommitException(String.format("Group commit failed. Group: %s", this), e);

            // Let other threads know the exception.
            synchronized (this) {
              for (Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> slot : slots.values()) {
                if (slot != emitterSlot.get()) {
                  slot.markAsFailed(exception);
                }
              }
            }

            // Throw the exception for the thread which executed the group commit.
            throw exception;
          }
        };

    emitterSlot.get().delegateTaskToWaiter(taskForEmitterSlot);
  }

  void updateDelayedSlotMoveTimeoutAt() {
    delayedSlotMoveTimeoutAtMillis.set(System.currentTimeMillis() + delayedSlotMoveTimeoutMillis);
  }

  long groupSizeFixTimeoutAtMillis() {
    return groupSizeFixTimeoutAtMillis;
  }

  long delayedSlotMoveTimeoutAtMillis() {
    return delayedSlotMoveTimeoutAtMillis.get();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof NormalGroup)) return false;
    NormalGroup<?, ?, ?, ?, ?> that = (NormalGroup<?, ?, ?, ?, ?>) o;
    return Objects.equal(parentKey, that.parentKey);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(parentKey);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("parentKey", parentKey)
        .add("status", status)
        .add("valueSlots.size", slots.size())
        .toString();
  }
}
