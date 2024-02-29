package com.scalar.db.util.groupcommit;

import com.google.common.base.Objects;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// A group for multiple slots that will be group-committed at once.
class NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>
    extends Group<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> {
  private static final Logger logger = LoggerFactory.getLogger(NormalGroup.class);

  private final PARENT_KEY parentKey;
  private final long moveDelayedSlotExpirationInMillis;
  private final Instant groupClosedAt;
  private final AtomicReference<Instant> delayedSlotMovedAt;
  private final GarbageNormalGroupCollector<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>
      garbageGroupCollector;

  NormalGroup(
      Emittable<EMIT_KEY, V> emitter,
      KeyManipulator<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY> keyManipulator,
      long groupCloseExpirationInMillis,
      long timeoutExpirationInMillis,
      int capacity,
      GarbageNormalGroupCollector<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>
          garbageGroupCollector) {
    super(emitter, keyManipulator, capacity);
    this.moveDelayedSlotExpirationInMillis = timeoutExpirationInMillis;
    this.groupClosedAt = Instant.now().plusMillis(groupCloseExpirationInMillis);
    this.delayedSlotMovedAt = new AtomicReference<>();
    updateDelayedSlotMovedAt();
    this.parentKey = keyManipulator.generateParentKey();
    this.garbageGroupCollector = garbageGroupCollector;
  }

  PARENT_KEY getParentKey() {
    return parentKey;
  }

  @Override
  FULL_KEY fullKey(CHILD_KEY childKey) {
    return keyManipulator.fullKey(parentKey, childKey);
  }

  // If it returns null, the Group is already closed and a retry is needed.
  @Nullable
  FULL_KEY reserveNewSlot(CHILD_KEY childKey) {
    return reserveNewSlot(new Slot<>(childKey, this));
  }

  @Nullable
  synchronized List<Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>> removeNotReadySlots() {
    if (!isSizeFixed()) {
      logger.info(
          "No need to remove any slot since the size isn't fixed yet. Too early. group:{}", this);
      return null;
    }

    // Lazy instantiation might be better, but it's likely there is a not-ready value slot since
    // it's already timed-out.
    List<Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>> removed = new ArrayList<>();
    for (Entry<CHILD_KEY, Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>> entry :
        slots.entrySet()) {
      Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> slot = entry.getValue();
      if (slot.value() == null) {
        removed.add(slot);
      }
    }

    if (removed.size() >= size()) {
      logger.debug("No need to remove any slot since all the slots are not ready. group:{}", this);
      return null;
    }

    for (Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> slot : removed) {
      removeSlot(slot.key());
      logger.info(
          "Removed a value slot from group to move it to delayed group. group:{}, slot:{}",
          this,
          slot);
    }
    return removed;
  }

  @Override
  public synchronized void asyncEmit() {
    if (slots.isEmpty()) {
      return;
    }

    final AtomicReference<Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>> emitterSlot =
        new AtomicReference<>();

    boolean isFirst = true;
    List<V> values = new ArrayList<>(slots.size());
    // Avoid using java.util.Collection.stream since it's a bit slow.
    for (Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> slot : slots.values()) {
      // Use the first slot as an emitter.
      if (isFirst) {
        isFirst = false;
        emitterSlot.set(slot);
      }
      values.add(slot.value());
    }

    ThrowableRunnable taskForEmitterSlot =
        () -> {
          try {
            emitter.execute(keyManipulator.emitKeyFromParentKey(parentKey), values);

            // Wake up the other waiting threads.
            // Pass null since the value is already emitted by the thread of `firstSlot`.
            for (Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> slot : slots.values()) {
              if (slot != emitterSlot.get()) {
                slot.markAsSuccess();
              }
            }
          } catch (Exception e) {
            String msg = "Group commit failed";
            logger.error(msg, e);
            GroupCommitFailureException exception = new GroupCommitFailureException(msg, e);

            // Let other threads know the exception.
            for (Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> slot : slots.values()) {
              if (slot != emitterSlot.get()) {
                slot.markAsFail(exception);
              }
            }

            // Throw the exception for the thread which executed the group commit.
            throw exception;
          } finally {
            dismiss();
          }
        };

    emitterSlot.get().delegateTaskToWaiter(taskForEmitterSlot);
  }

  void updateDelayedSlotMovedAt() {
    delayedSlotMovedAt.set(Instant.now().plusMillis(moveDelayedSlotExpirationInMillis));
  }

  Instant groupClosedAt() {
    return groupClosedAt;
  }

  Instant delayedSlotMovedAt() {
    return delayedSlotMovedAt.get();
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
  protected void dismiss() {
    garbageGroupCollector.collect(this);
  }
}
