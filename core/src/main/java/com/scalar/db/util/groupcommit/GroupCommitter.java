package com.scalar.db.util.groupcommit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.util.groupcommit.KeyManipulator.Keys;
import java.io.Closeable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A group committer which is responsible for the group and slot managements and emits ready groups.
 * This class receives 4 generic types as keys. But it's likely only one or a few key types are
 * enough. The reason to use the 4 keys is to detect wrong key usages at compile time.
 *
 * @param <PARENT_KEY> A key type to NormalGroup which contains multiple slots and is
 *     group-committed.
 * @param <CHILD_KEY> A key type to slot in NormalGroup which can contain a value ready to commit.
 * @param <FULL_KEY> A key type to DelayedGroup which contains a single slot and is
 *     singly-committed.
 * @param <EMIT_KEY> A key type that Emitter can interpret.
 * @param <V> A value type to be set to a slot.
 */
@ThreadSafe
public class GroupCommitter<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(GroupCommitter.class);

  // Background workers
  private final GroupSizeFixWorker<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> groupSizeFixWorker;
  private final DelayedSlotMoveWorker<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>
      delayedSlotMoveWorker;
  private final GroupCleanupWorker<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> groupCleanupWorker;

  // Monitor
  private final GroupCommitMonitor groupCommitMonitor;

  // This contains logics of how to treat keys.
  private final KeyManipulator<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY> keyManipulator;

  private final GroupManager<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> groupManager;

  private final AtomicBoolean closing = new AtomicBoolean();

  /**
   * @param label A label used for thread name.
   * @param config A configuration.
   * @param keyManipulator A key manipulator that contains logics how to treat keys.
   */
  public GroupCommitter(
      String label,
      GroupCommitConfig config,
      KeyManipulator<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY> keyManipulator) {
    logger.info("Starting GroupCommitter. Label: {}, Config: {}", label, config);
    this.keyManipulator = keyManipulator;
    this.groupManager = createGroupManager(config, keyManipulator);
    this.groupCleanupWorker = createGroupCleanupWorker(label, config, groupManager);
    this.delayedSlotMoveWorker =
        createDelayedSlotMoveWorker(label, config, groupManager, groupCleanupWorker);
    this.groupSizeFixWorker =
        createGroupSizeFixWorker(
            label, config, groupManager, delayedSlotMoveWorker, groupCleanupWorker);

    // TODO: Make this configurable.
    this.groupCommitMonitor = createMonitor(label);
  }

  Metrics getMetrics() {
    return new Metrics(
        groupSizeFixWorker.size(),
        delayedSlotMoveWorker.size(),
        groupCleanupWorker.size(),
        groupManager.sizeOfNormalGroupMap(),
        groupManager.sizeOfDelayedGroupMap());
  }

  /**
   * Set an emitter which contains implementation to emit values. Ideally, this should be passed to
   * the constructor, but the instantiation timings of {@link GroupCommitter} and {@link Emittable}
   * can be different. That's why this API exists.
   *
   * @param emitter An emitter.
   */
  public void setEmitter(Emittable<EMIT_KEY, V> emitter) {
    groupManager.setEmitter(emitter);
  }

  /**
   * Reserves a new slot in the current {@link NormalGroup}. The slot may be moved to a {@link
   * DelayedGroup} later.
   *
   * @param childKey A child key.
   * @return The full key associated with the reserved slot.
   */
  public FULL_KEY reserve(CHILD_KEY childKey) {
    if (closing.get()) {
      throw new GroupCommitException(
          String.format(
              "Reserving a new slot isn't allowed since already closed. Child key: %s", childKey));
    }

    while (true) {
      FULL_KEY fullKey = groupManager.reserveNewSlot(childKey);
      if (fullKey != null) {
        return fullKey;
      }
      logger.debug(
          "Failed to reserve a new value slot since the group was already size-fixed. Retrying. Key: {}",
          childKey);
    }
  }

  /**
   * Marks the slot associated with the specified key READY and then, waits until the group which
   * contains the slot is emitted.
   *
   * @param fullKey A full key associated with the slot already reserved with {@link
   *     GroupCommitter#reserve}.
   * @param value A value to be set to the slot. It will be committed with other values contained in
   *     slots of the same group.
   * @throws GroupCommitException when group commit fails
   */
  public void ready(FULL_KEY fullKey, V value) throws GroupCommitException {
    Keys<PARENT_KEY, CHILD_KEY, FULL_KEY> keys = keyManipulator.keysFromFullKey(fullKey);
    boolean failed = false;
    while (true) {
      Group<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> group = groupManager.getGroup(keys);
      if (group.putValueToSlotAndWait(keys.childKey, value)) {
        return;
      }
      // Failing to put a value to the slot can happen only when the slot is moved from the original
      // NormalGroup to a new DelayedGroup. So, only a single retry must be enough.
      if (failed) {
        throw new GroupCommitException(
            String.format(
                "Failed to put a value to the slot. The slot might be already removed before this operation. Group: %s, Full key: %s, Value: %s",
                group, fullKey, value));
      }
      failed = true;
      logger.debug(
          "The state of the group has been changed. Retrying. Group: {}, Keys: {}", group, keys);
    }
  }

  /**
   * Removes the slot from the group. This method needs to be called if the group commit for the key
   * is canceled and {@link GroupCommitter#ready} won't be called for it.
   *
   * @param fullKey A full key to specify the slot.
   */
  public void remove(FULL_KEY fullKey) {
    Keys<PARENT_KEY, CHILD_KEY, FULL_KEY> keys = keyManipulator.keysFromFullKey(fullKey);
    if (!groupManager.removeSlotFromGroup(keys)) {
      logger.debug(
          "Failed to remove the slot. Slots in a group that is already done can be automatically removed. Full key: {}",
          fullKey);
    }
  }

  /**
   * Closes the resources. The ExecutorServices are created as daemon, so calling this method isn't
   * needed. But for testing, this should be called for resources.
   */
  @Override
  public void close() {
    logger.info("Closing GroupCommitter");
    closing.set(true);
    int count = 0;
    do {
      Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
      Metrics metrics = getMetrics();
      if (!metrics.hasRemaining()) {
        logger.info("No ongoing group remains. Closing all the resources");
        groupCommitMonitor.close();
        groupSizeFixWorker.close();
        delayedSlotMoveWorker.close();
        groupCleanupWorker.close();
        break;
      }
      if (++count % 20 == 0) {
        logger.info("Ongoing slot still remains. Metrics: {}", metrics);
      }
    } while (true);
  }

  @VisibleForTesting
  GroupManager<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> createGroupManager(
      GroupCommitConfig config,
      KeyManipulator<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY> keyManipulator) {
    return new GroupManager<>(config, keyManipulator);
  }

  @VisibleForTesting
  GroupCleanupWorker<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> createGroupCleanupWorker(
      String label,
      GroupCommitConfig config,
      GroupManager<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> groupManager) {
    GroupCleanupWorker<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> worker =
        new GroupCleanupWorker<>(label, config.timeoutCheckIntervalMillis(), groupManager);
    groupManager.setGroupCleanupWorker(worker);
    return worker;
  }

  @VisibleForTesting
  DelayedSlotMoveWorker<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> createDelayedSlotMoveWorker(
      String label,
      GroupCommitConfig config,
      GroupManager<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> groupManager,
      GroupCleanupWorker<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> groupCleanupWorker) {
    return new DelayedSlotMoveWorker<>(
        label, config.timeoutCheckIntervalMillis(), groupManager, groupCleanupWorker);
  }

  @VisibleForTesting
  GroupSizeFixWorker<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> createGroupSizeFixWorker(
      String label,
      GroupCommitConfig config,
      GroupManager<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> groupManager,
      DelayedSlotMoveWorker<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> delayedSlotMoveWorker,
      GroupCleanupWorker<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> groupCleanupWorker) {
    GroupSizeFixWorker<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> worker =
        new GroupSizeFixWorker<>(
            label, config.timeoutCheckIntervalMillis(), delayedSlotMoveWorker, groupCleanupWorker);
    groupManager.setGroupSizeFixWorker(worker);
    return worker;
  }

  @VisibleForTesting
  GroupCommitMonitor createMonitor(String label) {
    return new GroupCommitMonitor(label, this::getMetrics);
  }
}
