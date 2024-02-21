package com.scalar.db.util.groupcommit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NormalGroup<K, V> extends Group<K, V> {
  private static final Logger logger = LoggerFactory.getLogger(NormalGroup.class);

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

  K getFullKey(K childKey) {
    return keyManipulator.createFullKey(key, childKey);
  }

  K reserveNewSlot(K childKey) throws GroupCommitAlreadyClosedException {
    return reserveNewSlot(new Slot<>(childKey, this));
  }

  @Nullable
  synchronized List<Slot<K, V>> removeNotReadySlots() {
    if (!isSizeFixed()) {
      logger.info(
          "No need to remove any slot since the size isn't fixed yet. Too early. group:{}", this);
      return null;
    }

    // Lazy instantiation might be better, but it's likely there is a not-ready value slot since
    // it's already timed-out.
    List<Slot<K, V>> removed = new ArrayList<>();
    for (Entry<K, Slot<K, V>> entry : slots.entrySet()) {
      Slot<K, V> slot = entry.getValue();
      if (slot.getValue() == null) {
        removed.add(slot);
      }
    }

    if (removed.size() >= getSize()) {
      logger.info("No need to remove any slot since all the slots are not ready. group:{}", this);
      return null;
    }

    for (Slot<K, V> slot : removed) {
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
      values.add(slot.getValue());
    }

    long startDelegate = System.currentTimeMillis();
    ThrowableRunnable taskForEmitterSlot =
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
                slot.success();
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
                slot.fail(exception);
              }
            }

            // Throw the exception for the thread of `firstSlot`.
            throw e;
          } finally {
            dismiss();
          }
        };

    emitterSlot.get().delegateTask(taskForEmitterSlot);
  }
}
