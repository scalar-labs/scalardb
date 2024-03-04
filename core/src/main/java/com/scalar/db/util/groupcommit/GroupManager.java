package com.scalar.db.util.groupcommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.errorprone.annotations.concurrent.LazyInit;
import com.scalar.db.util.groupcommit.KeyManipulator.Keys;
import java.io.Closeable;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.StampedLock;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GroupManager<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(GroupCommitter.class);

  // Queues
  private final QueueForClosingNormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>
      queueForClosingNormalGroup;
  private final QueueForMovingDelayedSlot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>
      queueForMovingDelayedSlot;
  private final QueueForCleaningUpGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>
      queueForCleaningUpGroup;

  // Executors
  private final ExecutorService monitorExecutorService;

  // Groups
  @Nullable private NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> currentGroup;
  // Note: Using ConcurrentHashMap results in less performance.
  private final Map<PARENT_KEY, NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>>
      normalGroupMap = new HashMap<>();
  private final Map<FULL_KEY, DelayedGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>>
      delayedGroupMap = new HashMap<>();
  private final StampedLock lock = new StampedLock();

  // Custom operations injected by the client
  private final KeyManipulator<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY> keyManipulator;
  @LazyInit private Emittable<EMIT_KEY, V> emitter;

  private final long normalGroupCloseExpirationInMillis;
  private final long delayedSlotMoveExpirationInMillis;
  private final int numberOfRetentionValues;

  public GroupManager(
      String label,
      GroupCommitConfig config,
      KeyManipulator<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY> keyManipulator) {
    this.keyManipulator = keyManipulator;
    this.normalGroupCloseExpirationInMillis = config.groupCloseExpirationInMillis();
    this.delayedSlotMoveExpirationInMillis = config.delayedSlotExpirationInMillis();
    this.numberOfRetentionValues = config.retentionSlotsCount();
    this.queueForCleaningUpGroup =
        new QueueForCleaningUpGroup<>(label, config.checkIntervalInMillis(), this);
    this.queueForMovingDelayedSlot =
        new QueueForMovingDelayedSlot<>(
            label, config.checkIntervalInMillis(), this, queueForCleaningUpGroup);
    this.queueForClosingNormalGroup =
        new QueueForClosingNormalGroup<>(
            label,
            config.checkIntervalInMillis(),
            queueForMovingDelayedSlot,
            queueForCleaningUpGroup);

    // TODO: This should be replaced by other metrics mechanism.
    this.monitorExecutorService =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(label + "-group-commit-monitor-%d")
                .build());
    startMonitorExecutorService();
  }

  // Reserves a new slot in the current NormalGroup. A new NormalGroup will be created and
  // registered to `normalGroupMap` if the current NormalGroup is already closed.
  //
  // If it returns null, the Group is already closed and a retry is needed.
  @Nullable
  FULL_KEY reserveNewSlot(CHILD_KEY childKey) {
    long stamp = lock.writeLock();
    try {
      if (currentGroup == null || currentGroup.isClosed()) {
        currentGroup =
            new NormalGroup<>(
                emitter,
                keyManipulator,
                normalGroupCloseExpirationInMillis,
                delayedSlotMoveExpirationInMillis,
                numberOfRetentionValues);
        queueForClosingNormalGroup.add(currentGroup);
        normalGroupMap.put(currentGroup.parentKey(), currentGroup);
      }
    } finally {
      lock.unlockWrite(stamp);
    }

    return currentGroup.reserveNewSlot(childKey);
  }

  // Gets the corresponding group associated with the given key.
  Group<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> getGroup(
      Keys<PARENT_KEY, CHILD_KEY, FULL_KEY> keys) throws GroupCommitTargetNotFoundException {
    long stamp = lock.writeLock();
    try {
      DelayedGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> delayedGroup =
          delayedGroupMap.get(keys.fullKey);
      if (delayedGroup != null) {
        return delayedGroup;
      }

      NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> normalGroup =
          normalGroupMap.get(keys.parentKey);
      if (normalGroup != null) {
        return normalGroup;
      }
    } finally {
      lock.unlockWrite(stamp);
    }

    throw new GroupCommitTargetNotFoundException(
        "The group for the reserved value slot doesn't exist. Keys:" + keys);
  }

  // Remove the specified group from group map.
  boolean removeGroupFromMap(Group<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> group) {
    long stamp = lock.writeLock();
    try {
      if (group instanceof NormalGroup) {
        NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> normalGroup =
            (NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>) group;
        return normalGroupMap.remove(normalGroup.parentKey()) != null;
      } else {
        assert group instanceof DelayedGroup;
        DelayedGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> delayedGroup =
            (DelayedGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>) group;
        return delayedGroupMap.remove(delayedGroup.fullKey()) != null;
      }
    } finally {
      lock.unlockWrite(stamp);
    }
  }

  // Remove the specified slot from the associated group.
  boolean removeSlotFromGroup(Keys<PARENT_KEY, CHILD_KEY, FULL_KEY> keys) {
    long stamp = lock.writeLock();
    try {
      boolean removed = false;

      DelayedGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> delayedGroup =
          delayedGroupMap.get(keys.fullKey);
      if (delayedGroup != null) {
        removed = delayedGroup.removeSlot(keys.childKey);
        // TODO: Ask the cleanup queue for this deletion from the map?
        if (delayedGroup.slots.isEmpty()) {
          delayedGroupMap.remove(keys.fullKey);
        }
      }

      NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> normalGroup =
          normalGroupMap.get(keys.parentKey);
      if (normalGroup != null) {
        removed = normalGroup.removeSlot(keys.childKey) || removed;
        if (normalGroup.slots.isEmpty()) {
          // TODO: Ask the cleanup queue for this deletion from the map?
          normalGroupMap.remove(keys.parentKey);
        }
      }

      return removed;
    } finally {
      lock.unlockWrite(stamp);
    }
  }

  // Moves delayed slots from the NormalGroup to a new DelayedGroup so that the NormalGroup can be
  // ready. The new one is also
  // registered to the group map and the cleanup queue.
  //
  // Returns true if any delayed slot is moved, false otherwise.
  boolean moveDelayedSlotToDelayedGroup(
      NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> normalGroup) {
    long stamp = lock.writeLock();
    try {
      // Remove delayed tasks from the NormalGroup so that it can be ready.
      List<Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>> notReadySlots =
          normalGroup.removeNotReadySlots();
      if (notReadySlots == null) {
        normalGroup.updateDelayedSlotMovedAt();
        logger.debug(
            "This group isn't needed to remove slots. Updated the expiration timing. group:{}",
            normalGroup);
        return false;
      }
      for (Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> notReadySlot : notReadySlots) {
        // Create a new DelayedGroup
        FULL_KEY fullKey = notReadySlot.fullKey();
        DelayedGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> delayedGroup =
            new DelayedGroup<>(fullKey, emitter, keyManipulator);
        notReadySlot.changeParentGroupToDelayedGroup(delayedGroup);

        // Register the new DelayedGroup to the map and cleanup queue.
        DelayedGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> old =
            delayedGroupMap.put(fullKey, delayedGroup);
        if (old != null) {
          logger.warn("The slow group value map already has the same key group. {}", old);
        }
        queueForCleaningUpGroup.add(delayedGroup);

        // Set the slot stored in the NormalGroup into the new DelayedGroup.
        // Internally delegate the emit-task to the client thread.
        checkNotNull(delayedGroup.reserveNewSlot(notReadySlot));
      }
    } finally {
      lock.unlockWrite(stamp);
    }

    return true;
  }

  Metrics getMetrics() {
    return new Metrics(
        queueForClosingNormalGroup.size(),
        queueForMovingDelayedSlot.size(),
        queueForCleaningUpGroup.size(),
        normalGroupMap.size(),
        delayedGroupMap.size());
  }

  // TODO: This should be replaced by other metrics mechanism.
  private void startMonitorExecutorService() {
    Runnable print =
        () -> logger.info("[MONITOR] Timestamp={}, Metrics={}", Instant.now(), getMetrics());

    monitorExecutorService.execute(
        () -> {
          while (!monitorExecutorService.isShutdown()) {
            try {
              print.run();
              TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              logger.warn("Interrupted", e);
              break;
            }
          }
          print.run();
        });
  }

  void setEmitter(Emittable<EMIT_KEY, V> emitter) {
    this.emitter = emitter;
  }

  /**
   * Closes the resources. The ExecutorServices are created as daemon, so calling this method isn't
   * needed. But for testing, this should be called for resources.
   */
  @Override
  public void close() {
    if (monitorExecutorService != null) {
      MoreExecutors.shutdownAndAwaitTermination(monitorExecutorService, 5, TimeUnit.SECONDS);
    }
    queueForMovingDelayedSlot.close();
    queueForClosingNormalGroup.close();
    queueForCleaningUpGroup.close();
  }
}
