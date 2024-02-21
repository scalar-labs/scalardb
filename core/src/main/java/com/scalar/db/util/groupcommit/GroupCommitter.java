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

// TODO: K should be separate into PARENT_KEY, CHILD_KEY and FULL_KEY
public class GroupCommitter<K, V> implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(GroupCommitter.class);
  // Queues
  private final BlockingQueue<NormalGroup<K, V>> queueForNormalGroupClose =
      new LinkedBlockingQueue<>();
  private final BlockingQueue<NormalGroup<K, V>> queueForDelayedSlotMove =
      new LinkedBlockingQueue<>();
  // Parameters
  private final long queueCheckIntervalInMillis;
  private final long normalGroupCloseExpirationInMillis;
  private final long delayedSlotMoveExpirationInMillis;
  private final int numberOfRetentionValues;
  // Executors
  private final ExecutorService normalGroupCloseExecutorService;
  private final ExecutorService delayedSlotMoveExecutorService;
  private final ExecutorService monitorExecutorService;
  // Custom operations injected by the client
  private final KeyManipulator<K> keyManipulator;
  @LazyInit private Emittable<K, V> emitter;
  private final GroupManager groupManager;

  // This class is just for encapsulation of accesses to Groups
  private class GroupManager {
    // Groups
    @Nullable private NormalGroup<K, V> currentGroup;
    // Using ConcurrentHashMap results in less performance.
    private final Map<K, NormalGroup<K, V>> normalGroupMap = new HashMap<>();
    private final Map<K, DelayedGroup<K, V>> delayedGroupMap = new HashMap<>();
    private final StampedLock lock = new StampedLock();

    // Returns full key
    private K reserveNewSlot(K childKey) throws GroupCommitAlreadyClosedException {
      boolean isNewGroupCreated = false;
      NormalGroup<K, V> oldGroup = null;
      NormalGroup<K, V> newGroup = null;
      long stamp = lock.writeLock();
      try {
        if (currentGroup == null || currentGroup.isClosed()) {
          isNewGroupCreated = true;
          oldGroup = currentGroup;
          currentGroup =
              new NormalGroup<>(
                  emitter,
                  keyManipulator,
                  normalGroupCloseExpirationInMillis,
                  delayedSlotMoveExpirationInMillis,
                  numberOfRetentionValues,
                  this::unregisterNormalGroup);
          newGroup = currentGroup;
          // TODO: This can be a faster queue?
          queueForNormalGroupClose.add(currentGroup);
          normalGroupMap.put(currentGroup.key, currentGroup);
        }
      } finally {
        lock.unlockWrite(stamp);
      }

      if (isNewGroupCreated) {
        ///////// FIXME: DEBUG
        logger.info("New group:{}, old group:{}, child key:{}", newGroup, oldGroup, childKey);
        ///////// FIXME: DEBUG
      }
      return currentGroup.reserveNewSlot(childKey);
    }

    private Group<K, V> getGroup(Keys<K> keys) throws GroupCommitTargetNotFoundException {
      long stamp = lock.writeLock();
      try {
        DelayedGroup<K, V> delayedGroup =
            delayedGroupMap.get(keyManipulator.createFullKey(keys.parentKey, keys.childKey));
        if (delayedGroup != null) {
          return delayedGroup;
        }

        NormalGroup<K, V> normalGroup = normalGroupMap.get(keys.parentKey);
        if (normalGroup != null) {
          return normalGroup;
        }
      } finally {
        lock.unlockWrite(stamp);
      }

      throw new GroupCommitTargetNotFoundException(
          "The group for the reserved value slot doesn't exist. keys:" + keys);
    }

    private synchronized void unregisterNormalGroup(Group<K, V> group) {
      long stamp = lock.writeLock();
      try {
        normalGroupMap.remove(group.key);
      } finally {
        lock.unlockWrite(stamp);
      }
    }

    private synchronized void unregisterDelayedGroup(Group<K, V> group) {
      long stamp = lock.writeLock();
      try {
        delayedGroupMap.remove(group.key);
      } finally {
        lock.unlockWrite(stamp);
      }
    }

    private boolean moveDelayedSlotToDelayedGroup(NormalGroup<K, V> normalGroup) {
      // Already tried to move this code inside NormalGroup.removeNotReadySlots() to remove
      // the `synchronized` keyword on this method. But the performance was degraded.
      logger.info("[DELAYED-SLOT-MOVE] moveDelayedSlotToDelayedGroup#1 BV:{}", normalGroup);
      long stamp = lock.writeLock();
      try {
        List<Slot<K, V>> notReadySlots = normalGroup.removeNotReadySlots();
        if (notReadySlots == null) {
          normalGroup.updateDelayedSlotMovedAt();
          logger.info(
              "This group isn't needed to remove slots. Updated the expiration timing. group:{}",
              normalGroup);
          return false;
        }
        for (Slot<K, V> notReadySlot : notReadySlots) {
          K fullKey = notReadySlot.getFullKey();
          DelayedGroup<K, V> delayedGroup =
              new DelayedGroup<>(
                  fullKey,
                  emitter,
                  keyManipulator,
                  normalGroupCloseExpirationInMillis,
                  delayedSlotMoveExpirationInMillis,
                  notReadySlot,
                  this::unregisterDelayedGroup);

          // Delegate the value to the client thread
          notReadySlot.delegateTask(
              () -> {
                try {
                  emitter.execute(fullKey, Collections.singletonList(notReadySlot.getValue()));
                } finally {
                  unregisterDelayedGroup(delayedGroup);
                }
              });

          DelayedGroup<K, V> old = delayedGroupMap.put(fullKey, delayedGroup);
          if (old != null) {
            logger.warn("The slow group value map already has the same key group. {}", old);
          }
        }
        logger.info("[DELAYED-SLOT-MOVE] moveDelayedSlotToDelayedGroup#2 BV:{}", normalGroup);
        if (normalGroup.slots.values().stream().noneMatch(v -> v.getValue() != null)) {
          normalGroupMap.remove(normalGroup.key);
          logger.info("Removed a group as it's empty. normalGroup:{}", normalGroup);
        }
      } finally {
        lock.unlockWrite(stamp);
      }
      logger.info("[DELAYED-SLOT-MOVE] moveDelayedSlotToDelayedGroup#3 BV:{}", normalGroup);

      return true;
    }
  }

  public GroupCommitter(
      String label,
      long sizeFixExpirationInMillis,
      long timeoutExpirationInMillis,
      int numberOfRetentionValues,
      long expirationCheckIntervalInMillis,
      KeyManipulator<K> keyManipulator) {
    this.normalGroupCloseExpirationInMillis = sizeFixExpirationInMillis;
    this.delayedSlotMoveExpirationInMillis = timeoutExpirationInMillis;
    this.numberOfRetentionValues = numberOfRetentionValues;
    this.queueCheckIntervalInMillis = expirationCheckIntervalInMillis;
    this.keyManipulator = keyManipulator;
    this.groupManager = new GroupManager();

    this.monitorExecutorService =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(label + "-group-commit-monitor-%d")
                .build());
    startMonitorExecutorService();

    this.normalGroupCloseExecutorService =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(label + "-group-commit-normal-group-close-%d")
                .build());
    startNormalGroupCloseExecutorService();

    this.delayedSlotMoveExecutorService =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(label + "-group-commit-delayed-slot-move-%d")
                .build());
    startDelayedSlotMoveExecutorService();
  }

  public void setEmitter(Emittable<K, V> emitter) {
    this.emitter = emitter;
  }

  private boolean handleQueueForNormalGroupClose() {
    NormalGroup<K, V> normalGroup = queueForNormalGroupClose.peek();
    Long retryWaitInMillis = null;

    if (normalGroup == null) {
      retryWaitInMillis = queueCheckIntervalInMillis;
    } else if (normalGroup.isSizeFixed()) {
      // Already the size is fixed. Nothing to do. Handle a next element immediately
      ////////// FIXME: DEBUG LOG
      if (normalGroup.groupClosedAt().isBefore(Instant.now().minusMillis(5000))) {
        logger.info(
            "[NORMAL-GROUP-CLOSE] Too old group: group.key={}, group.values={}",
            normalGroup.key,
            normalGroup.slots);
      }
      ////////// FIXME: DEBUG LOG
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
        // TODO: Unified the error message
        logger.warn("Interrupted", e);
        return false;
      }
    } else {
      ////////// FIXME: DEBUG LOG
      logger.info("[NORMAL-GROUP-CLOSE] Fetched group={}", normalGroup);
      ////////// FIXME: DEBUG LOG
      // Move the size-fixed group but not ready to the timeout queue
      if (!normalGroup.isReady()) {
        queueForDelayedSlotMove.add(normalGroup);
      }
      NormalGroup<K, V> removed = queueForNormalGroupClose.poll();
      // Check if the removed group is expected just in case.
      if (removed == null || !removed.equals(normalGroup)) {
        logger.error(
            "The queue for normal-group-close returned an inconsistent value. Re-enqueuing it. expected:{}, actual:{}",
            normalGroup,
            removed);
        if (removed != null) {
          queueForNormalGroupClose.add(removed);
        }
      }
    }
    return true;
  }

  private boolean handleQueueForDelayedSlotMove() {
    NormalGroup<K, V> normalGroup = queueForDelayedSlotMove.peek();
    Long retryWaitInMillis = null;

    ////////// FIXME: DEBUG LOG
    logger.info(
        "[DELAYED-SLOT-MOVE] New group:{}, size:{}", normalGroup, queueForDelayedSlotMove.size());

    if (normalGroup == null) {
      retryWaitInMillis = queueCheckIntervalInMillis * 2;
    } else if (normalGroup.isReady()) {
      // Already ready. Nothing to do. Handle a next element immediately
    } else {
      Instant now = Instant.now();
      if (now.isAfter(normalGroup.delayedSlotMovedAt())) {
        ////////// FIXME: DEBUG LOG
        long start = System.currentTimeMillis();
        if (!groupManager.moveDelayedSlotToDelayedGroup(normalGroup)) {
          logger.info(
              "[DELAYED-SLOT-MOVE] Re-enqueue a group since no need to remove any slots. group:{}",
              normalGroup);
          NormalGroup<K, V> removed = queueForDelayedSlotMove.poll();
          // Check if the removed slot is expected just in case.
          if (removed == null || !removed.equals(normalGroup)) {
            logger.error(
                "The queue for move-delayed-slot returned an inconsistent value. expected:{}, actual:{}",
                normalGroup,
                removed);
          }
          if (removed != null) {
            queueForDelayedSlotMove.add(removed);
          }
          return true;
        }
        logger.info(
            "[DELAYED-SLOT-MOVE] Moved group:{} to delayed group, duration:{}ms, size:{}",
            normalGroup,
            (System.currentTimeMillis() - start),
            queueForDelayedSlotMove.size());
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
        // TODO: Unified the error message
        logger.warn("Interrupted", e);
        return false;
      }
    } else {
      ////////// FIXME: DEBUG LOG
      logger.info("[DELAYED-SLOT-MOVE] Removing fetched group: group={}", normalGroup);
      ////////// FIXME: DEBUG LOG
      NormalGroup<K, V> removed = queueForDelayedSlotMove.poll();
      // Check if the removed slot is expected just in case.
      if (removed == null || !removed.equals(normalGroup)) {
        logger.error(
            "The queue for move-delayed-slot returned an inconsistent value. expected:{}, actual:{}",
            normalGroup,
            removed);
        if (removed != null) {
          queueForDelayedSlotMove.add(removed);
        }
      }
    }
    return true;
  }

  private void startNormalGroupCloseExecutorService() {
    normalGroupCloseExecutorService.execute(
        () -> {
          while (!normalGroupCloseExecutorService.isShutdown()) {
            if (!handleQueueForNormalGroupClose()) {
              break;
            }
          }
        });
  }

  private void startDelayedSlotMoveExecutorService() {
    delayedSlotMoveExecutorService.execute(
        () -> {
          while (!delayedSlotMoveExecutorService.isShutdown()) {
            if (!handleQueueForDelayedSlotMove()) {
              break;
            }
          }
        });
  }

  private void startMonitorExecutorService() {
    monitorExecutorService.execute(
        () -> {
          while (!monitorExecutorService.isShutdown()) {
            logger.info(
                "[MONITOR] Timestamp={}, NormalGroupClose.queue.size={}, DelayedSlotMove.queue.size={}, NormalGroupMap.size={}, DelayedGroupMap.size={}",
                Instant.now(),
                queueForNormalGroupClose.size(),
                queueForDelayedSlotMove.size(),
                groupManager.normalGroupMap.size(),
                groupManager.delayedGroupMap.size());
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

  // Returns the full key
  public K reserve(K childKey) {
    int counterForDebug = 0;
    while (true) {
      try {
        return groupManager.reserveNewSlot(childKey);
      } catch (GroupCommitAlreadyClosedException e) {
        logger.info("Failed to reserve a new value slot. Retrying. key:{}", childKey);
        ///////// FIXME: DEBUG
        counterForDebug++;
        if (counterForDebug > 1000) {
          throw new IllegalStateException("Too many retries. Something is wrong, key:" + childKey);
        }
        ///////// FIXME: DEBUG
      } catch (Throwable e) {
        ///////// FIXME: DEBUG
        logger.error("Failed to reserve slot #2: Unexpected key={}", childKey);
        ///////// FIXME: DEBUG
        throw e;
      }
    }
  }

  public boolean isGroupCommitFullKey(K fullKey) {
    return keyManipulator.isFullKey(fullKey);
  }

  public void ready(K fullKey, V value) throws GroupCommitException {
    Keys<K> keys = keyManipulator.fromFullKey(fullKey);
    int retry = 0;
    while (true) {
      Group<K, V> group = groupManager.getGroup(keys);
      try {
        group.putValueToSlotAndWait(keys.childKey, value);
        return;
      } catch (GroupCommitAlreadyCompletedException | GroupCommitTargetNotFoundException e) {
        // This can throw an exception in a race condition when the value slot is moved to
        // delayed group. So, retry should be needed.
        if (group instanceof NormalGroup) {
          if (++retry >= 4) {
            throw new GroupCommitException(
                String.format("Retry over for putting a value to the slot. fullKey=%s", fullKey),
                e);
          }
          try {
            TimeUnit.MILLISECONDS.sleep(10);
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            // TODO: Unified the error message
            throw new RuntimeException(ex);
          }
          continue;
        }
        throw e;
      }
    }
  }

  public void remove(K fullKey) {
    Keys<K> keys = keyManipulator.fromFullKey(fullKey);
    try {
      Group<K, V> group = groupManager.getGroup(keys);
      group.removeSlot(keys.childKey);
    } catch (GroupCommitTargetNotFoundException e) {
      logger.warn("Failed to remove the slot. fullKey:{}", fullKey, e);
    }
  }

  // The ExecutorServices are created as daemon, so calling this method isn't needed.
  // But for testing, this should be called for resources.
  @Override
  public void close() {
    if (delayedSlotMoveExecutorService != null) {
      MoreExecutors.shutdownAndAwaitTermination(
          delayedSlotMoveExecutorService, 5, TimeUnit.SECONDS);
    }
    if (normalGroupCloseExecutorService != null) {
      MoreExecutors.shutdownAndAwaitTermination(
          normalGroupCloseExecutorService, 5, TimeUnit.SECONDS);
    }
    if (monitorExecutorService != null) {
      MoreExecutors.shutdownAndAwaitTermination(monitorExecutorService, 5, TimeUnit.SECONDS);
    }
  }
}
