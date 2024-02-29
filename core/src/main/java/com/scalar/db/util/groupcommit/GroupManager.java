package com.scalar.db.util.groupcommit;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.errorprone.annotations.concurrent.LazyInit;
import com.scalar.db.util.groupcommit.KeyManipulator.Keys;
import java.io.Closeable;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.StampedLock;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GroupManager<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(GroupCommitter.class);

  // Queues
  private final QueueForClosingNormalGroup queueForClosingNormalGroup;
  private final QueueForMovingDelayedSlot queueForMovingDelayedSlot;

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

  // Parameters
  private final long queueCheckIntervalInMillis;
  private final long normalGroupCloseExpirationInMillis;
  private final long delayedSlotMoveExpirationInMillis;
  private final int numberOfRetentionValues;

  public GroupManager(
      String label,
      GroupCommitConfig config,
      KeyManipulator<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY> keyManipulator) {
    this.keyManipulator = keyManipulator;
    this.queueCheckIntervalInMillis = config.checkIntervalInMillis();
    this.normalGroupCloseExpirationInMillis = config.groupCloseExpirationInMillis();
    this.delayedSlotMoveExpirationInMillis = config.delayedSlotExpirationInMillis();
    this.numberOfRetentionValues = config.retentionSlotsCount();
    this.queueForMovingDelayedSlot = new QueueForMovingDelayedSlot(label);
    this.queueForClosingNormalGroup =
        new QueueForClosingNormalGroup(label, queueForMovingDelayedSlot);

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
  FULL_KEY reserveNewSlot(CHILD_KEY childKey) throws GroupCommitAlreadyClosedException {
    long stamp = lock.writeLock();
    try {
      if (currentGroup == null || currentGroup.isClosed()) {
        currentGroup =
            new NormalGroup<>(
                emitter,
                keyManipulator,
                normalGroupCloseExpirationInMillis,
                delayedSlotMoveExpirationInMillis,
                numberOfRetentionValues,
                this::unregisterNormalGroup);
        queueForClosingNormalGroup.add(currentGroup);
        normalGroupMap.put(currentGroup.getParentKey(), currentGroup);
      }
    } finally {
      lock.unlockWrite(stamp);
    }

    return currentGroup.reserveNewSlot(childKey);
  }

  // Gets the corresponding group associated with the given key.
  Group<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> getGroup(Keys<PARENT_KEY, CHILD_KEY> keys)
      throws GroupCommitTargetNotFoundException {
    long stamp = lock.writeLock();
    try {
      DelayedGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> delayedGroup =
          delayedGroupMap.get(keyManipulator.createFullKey(keys.parentKey, keys.childKey));
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
        "The group for the reserved value slot doesn't exist. keys:" + keys);
  }

  private synchronized void unregisterNormalGroup(
      NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> group) {
    long stamp = lock.writeLock();
    try {
      normalGroupMap.remove(group.getParentKey());
    } finally {
      lock.unlockWrite(stamp);
    }
  }

  private synchronized void unregisterDelayedGroup(
      DelayedGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> group) {
    long stamp = lock.writeLock();
    try {
      delayedGroupMap.remove(group.getFullKey());
    } finally {
      lock.unlockWrite(stamp);
    }
  }

  // Moves delayed slots from the NormalGroup to a new DelayedGroup. The new one is also
  // registered to `delayedGroupMap`.
  private boolean moveDelayedSlotToDelayedGroup(
      NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> normalGroup) {
    // Already tried to move this code inside NormalGroup.removeNotReadySlots() to remove
    // the `synchronized` keyword on this method. But the performance was degraded.
    long stamp = lock.writeLock();
    try {
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
        FULL_KEY fullKey = notReadySlot.getFullKey();
        DelayedGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> delayedGroup =
            new DelayedGroup<>(
                fullKey, emitter, keyManipulator, notReadySlot, this::unregisterDelayedGroup);

        DelayedGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> old =
            delayedGroupMap.put(fullKey, delayedGroup);

        // Delegate the value to the client thread
        notReadySlot.delegateTaskToWaiter(
            () -> {
              try {
                emitter.execute(
                    keyManipulator.getEmitKeyFromFullKey(fullKey),
                    Collections.singletonList(notReadySlot.getValue()));
              } finally {
                unregisterDelayedGroup(delayedGroup);
              }
            });

        if (old != null) {
          logger.warn("The slow group value map already has the same key group. {}", old);
        }
      }
      if (normalGroup.slots.values().stream().noneMatch(v -> v.getValue() != null)) {
        normalGroupMap.remove(normalGroup.getParentKey());
        logger.info("Removed a group as it's empty. normalGroup:{}", normalGroup);
      }
    } finally {
      lock.unlockWrite(stamp);
    }

    return true;
  }

  // TODO: This should be replaced by other metrics mechanism.
  private void startMonitorExecutorService() {
    monitorExecutorService.execute(
        () -> {
          while (!monitorExecutorService.isShutdown()) {
            // TODO: Move this to other metrics mechanism
            logger.info(
                "[MONITOR] Timestamp={}, NormalGroupClose.queue.size={}, DelayedSlotMove.queue.size={}, NormalGroupMap.size={}, DelayedGroupMap.size={}",
                Instant.now(),
                queueForClosingNormalGroup.size(),
                queueForMovingDelayedSlot.size(),
                normalGroupMap.size(),
                delayedGroupMap.size());
            try {
              TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              logger.warn("Interrupted", e);
              break;
            }
          }
        });
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
  }

  void setEmitter(Emittable<EMIT_KEY, V> emitter) {
    this.emitter = emitter;
  }

  // A queue to contain NormalGroup instances. The following timeout occurs in this queue:
  // - `group-close-expiration` fixes the size of expired NormalGroup.
  private class QueueForClosingNormalGroup implements Closeable {
    // Append-to-Last operation is executed by another thread via `add()` API and the worker thread.
    // Remove-from-First is executed by the worker thread.
    private final BlockingQueue<NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>> queue =
        new LinkedBlockingQueue<>();
    private final QueueForMovingDelayedSlot queueForMovingDelayedSlot;
    private final ExecutorService executorService;

    QueueForClosingNormalGroup(String label, QueueForMovingDelayedSlot queueForMovingDelayedSlot) {
      this.queueForMovingDelayedSlot = queueForMovingDelayedSlot;
      this.executorService =
          Executors.newSingleThreadExecutor(
              new ThreadFactoryBuilder()
                  .setDaemon(true)
                  .setNameFormat(label + "-group-commit-normal-group-close-%d")
                  .build());
      startNormalGroupCloseExecutorService();
    }

    void add(NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> normalGroup) {
      queue.add(normalGroup);
    }

    int size() {
      return queue.size();
    }

    private void startNormalGroupCloseExecutorService() {
      executorService.execute(
          () -> {
            while (!executorService.isShutdown()) {
              if (!process()) {
                break;
              }
            }
          });
    }

    private boolean process() {
      NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> normalGroup = queue.peek();
      Long retryWaitInMillis = null;

      if (normalGroup == null) {
        retryWaitInMillis = queueCheckIntervalInMillis;
      } else if (normalGroup.isSizeFixed()) {
        // Already the size is fixed. Nothing to do. Handle a next element immediately
      } else {
        Instant now = Instant.now();
        if (now.isAfter(normalGroup.groupClosedAt())) {
          // Expired. Fix the size
          normalGroup.fixSize();
        } else {
          // Not expired. Retry
          retryWaitInMillis = queueCheckIntervalInMillis;
        }
      }

      if (retryWaitInMillis != null) {
        try {
          TimeUnit.MILLISECONDS.sleep(retryWaitInMillis);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          logger.warn("Interrupted", e);
          return false;
        }
      } else {
        // Move the size-fixed group but not ready to the timeout queue
        if (!normalGroup.isReady()) {
          queueForMovingDelayedSlot.add(normalGroup);
        }
        NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> removed = queue.poll();
        // Check if the removed group is expected just in case.
        if (removed == null || !removed.equals(normalGroup)) {
          logger.error(
              "The queue for normal-group-close returned an inconsistent value. Re-enqueuing it. expected:{}, actual:{}",
              normalGroup,
              removed);
          if (removed != null) {
            queue.add(removed);
          }
        }
      }
      return true;
    }

    @Override
    public void close() {
      MoreExecutors.shutdownAndAwaitTermination(executorService, 5, TimeUnit.SECONDS);
    }
  }

  // A queue to contain DelayedGroup instances. The following timeout occurs in this queue:
  // - `delayed-slot-move-expiration` moves expired slots in NormalGroup to DelayedGroup.
  private class QueueForMovingDelayedSlot implements Closeable {
    // Append-to-Last operation is executed by another thread via `add()` API and the worker thread.
    // Remove-from-First is executed by the worker thread.
    private final BlockingQueue<NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>> queue =
        new LinkedBlockingQueue<>();

    private final ExecutorService executorService;

    QueueForMovingDelayedSlot(String label) {
      this.executorService =
          Executors.newSingleThreadExecutor(
              new ThreadFactoryBuilder()
                  .setDaemon(true)
                  .setNameFormat(label + "-group-commit-delayed-slot-move-%d")
                  .build());
      startDelayedSlotMoveExecutorService();
    }

    void add(NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> normalGroup) {
      queue.add(normalGroup);
    }

    int size() {
      return queue.size();
    }

    private void startDelayedSlotMoveExecutorService() {
      executorService.execute(
          () -> {
            while (!executorService.isShutdown()) {
              if (!handleQueueForDelayedSlotMove()) {
                break;
              }
            }
          });
    }

    private boolean handleQueueForDelayedSlotMove() {
      NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> normalGroup = queue.peek();
      Long retryWaitInMillis = null;

      if (normalGroup == null) {
        retryWaitInMillis = queueCheckIntervalInMillis * 2;
      } else if (normalGroup.isReady()) {
        // Already ready. Nothing to do. Handle a next element immediately
      } else {
        Instant now = Instant.now();
        if (now.isAfter(normalGroup.delayedSlotMovedAt())) {
          if (!moveDelayedSlotToDelayedGroup(normalGroup)) {
            NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> removed = queue.poll();
            // Check if the removed slot is expected just in case.
            if (removed == null || !removed.equals(normalGroup)) {
              logger.error(
                  "The queue for move-delayed-slot returned an inconsistent value. expected:{}, actual:{}",
                  normalGroup,
                  removed);
            }
            if (removed != null) {
              queue.add(removed);
            }
            return true;
          }
        } else {
          // Not expired. Retry
          retryWaitInMillis = queueCheckIntervalInMillis * 2;
        }
      }

      if (retryWaitInMillis != null) {
        try {
          TimeUnit.MILLISECONDS.sleep(retryWaitInMillis);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          logger.warn("Interrupted", e);
          return false;
        }
      } else {
        NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> removed = queue.poll();
        // Check if the removed slot is expected just in case.
        if (removed == null || !removed.equals(normalGroup)) {
          logger.error(
              "The queue for move-delayed-slot returned an inconsistent value. expected:{}, actual:{}",
              normalGroup,
              removed);
          if (removed != null) {
            queue.add(removed);
          }
        }
      }
      return true;
    }

    @Override
    public void close() {
      MoreExecutors.shutdownAndAwaitTermination(executorService, 5, TimeUnit.SECONDS);
    }
  }
}
