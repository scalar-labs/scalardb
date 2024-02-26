package com.scalar.db.util.groupcommit;

import com.google.common.base.Objects;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>
    extends Group<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> {
  private static final Logger logger = LoggerFactory.getLogger(NormalGroup.class);

  private final PARENT_KEY parentKey;
  private final GarbageNormalGroupCollector<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>
      garbageGroupCollector;

  NormalGroup(
      Emittable<EMIT_KEY, V> emitter,
      KeyManipulator<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY> keyManipulator,
      long sizeFixExpirationInMillis,
      long timeoutExpirationInMillis,
      int capacity,
      GarbageNormalGroupCollector<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>
          garbageGroupCollector) {
    super(emitter, keyManipulator, sizeFixExpirationInMillis, timeoutExpirationInMillis, capacity);
    this.parentKey = keyManipulator.createParentKey();
    this.garbageGroupCollector = garbageGroupCollector;
  }

  PARENT_KEY getParentKey() {
    return parentKey;
  }

  @Override
  String getKeyName() {
    return parentKey.toString();
  }

  @Override
  FULL_KEY getFullKey(CHILD_KEY childKey) {
    return keyManipulator.createFullKey(parentKey, childKey);
  }

  FULL_KEY reserveNewSlot(CHILD_KEY childKey) throws GroupCommitAlreadyClosedException {
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
      if (slot.getValue() == null) {
        removed.add(slot);
      }
    }

    if (removed.size() >= getSize()) {
      logger.info("No need to remove any slot since all the slots are not ready. group:{}", this);
      return null;
    }

    for (Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> slot : removed) {
      removeSlot(slot.getKey());
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
      values.add(slot.getValue());
    }

    ThrowableRunnable taskForEmitterSlot =
        () -> {
          try {
            long startEmit = System.currentTimeMillis();
            emitter.execute(keyManipulator.getEmitKeyFromParentKey(parentKey), values);
            logger.info(
                "Emitted (thread_id:{}, key:{}, num_of_values:{}): {} ms",
                Thread.currentThread().getId(),
                getKeyName(),
                getSize(),
                System.currentTimeMillis() - startEmit);

            // Wake up the other waiting threads.
            // Pass null since the value is already emitted by the thread of `firstSlot`.
            for (Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> slot : slots.values()) {
              if (slot != emitterSlot.get()) {
                slot.success();
              }
            }
          } catch (Exception e) {
            String msg = "Group commit failed";
            logger.error(msg, e);
            GroupCommitFailureException exception = new GroupCommitFailureException(msg, e);

            // Let other threads know the exception.
            for (Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> slot : slots.values()) {
              if (slot != emitterSlot.get()) {
                slot.fail(exception);
              }
            }

            // Throw the exception for the thread which executed the group commit.
            throw exception;
          } finally {
            dismiss();
          }
        };

    emitterSlot.get().delegateTask(taskForEmitterSlot);
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
