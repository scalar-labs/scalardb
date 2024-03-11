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

  // Background workers
  private final GroupCloseWorker<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> groupCloseWorker;
  private final DelayedSlotMoveWorker<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>
      delayedSlotMoveWorker;
  private final GroupCleanupWorker<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> groupCleanupWorker;

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

  private final long groupCloseTimeoutMillis;
  private final long delayedSlotMoveTimeoutMillis;
  private final int slotCapacity;
  private final CurrentTime currentTime;

  public GroupManager(
      String label,
      GroupCommitConfig config,
      KeyManipulator<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY> keyManipulator,
      CurrentTime currentTime) {
    this.keyManipulator = keyManipulator;
    this.groupCloseTimeoutMillis = config.groupCloseTimeoutMillis();
    this.delayedSlotMoveTimeoutMillis = config.delayedSlotMoveTimeoutMillis();
    this.slotCapacity = config.slotCapacity();
    this.groupCleanupWorker =
        new GroupCleanupWorker<>(label, config.timeoutCheckIntervalMillis(), this, currentTime);
    this.delayedSlotMoveWorker =
        new DelayedSlotMoveWorker<>(
            label, config.timeoutCheckIntervalMillis(), this, groupCleanupWorker, currentTime);
    this.groupCloseWorker =
        new GroupCloseWorker<>(
            label,
            config.timeoutCheckIntervalMillis(),
            delayedSlotMoveWorker,
            groupCleanupWorker,
            currentTime);
    this.currentTime = currentTime;

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
                groupCloseTimeoutMillis,
                delayedSlotMoveTimeoutMillis,
                slotCapacity,
                currentTime);
        groupCloseWorker.add(currentGroup);
        normalGroupMap.put(currentGroup.parentKey(), currentGroup);
      }
    } finally {
      lock.unlockWrite(stamp);
    }
    return currentGroup.reserveNewSlot(childKey);
  }

  // Gets the corresponding group associated with the given key.
  Group<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> getGroup(
      Keys<PARENT_KEY, CHILD_KEY, FULL_KEY> keys) throws GroupCommitException {
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

    throw new GroupCommitException(
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
      }

      NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> normalGroup =
          normalGroupMap.get(keys.parentKey);
      if (normalGroup != null) {
        removed = normalGroup.removeSlot(keys.childKey) || removed;
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
      // TODO: NormalGroup.removeNotReadySlots() calls updateStatus() potentially resulting in
      //       asyncEmit(). Maybe it should be called outside the lock.

      // Remove delayed tasks from the NormalGroup so that it can be ready.
      List<Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>> notReadySlots =
          normalGroup.removeNotReadySlots();
      if (notReadySlots == null) {
        normalGroup.updateDelayedSlotMovedAt();
        logger.debug(
            "This group isn't needed to remove slots. Updated the timeout. group:{}", normalGroup);
        return false;
      }
      for (Slot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> notReadySlot : notReadySlots) {
        // Create a new DelayedGroup
        FULL_KEY fullKey = notReadySlot.fullKey();
        DelayedGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> delayedGroup =
            new DelayedGroup<>(fullKey, emitter, keyManipulator, currentTime);

        // Register the new DelayedGroup to the map and cleanup queue.
        DelayedGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> old =
            delayedGroupMap.put(fullKey, delayedGroup);
        if (old != null) {
          logger.warn("The slow group value map already has the same key group. {}", old);
        }
        groupCleanupWorker.add(delayedGroup);

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
        groupCloseWorker.size(),
        delayedSlotMoveWorker.size(),
        groupCleanupWorker.size(),
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
    delayedSlotMoveWorker.close();
    groupCloseWorker.close();
    groupCleanupWorker.close();
  }
}
