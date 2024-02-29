package com.scalar.db.util.groupcommit;

import com.scalar.db.util.groupcommit.KeyManipulator.Keys;
import java.io.Closeable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A group committer which is responsible for the group and slot managements and emits ready groups.
 *
 * @param <PARENT_KEY> A key type to NormalGroup which contains multiple slots and is
 *     group-committed.
 * @param <CHILD_KEY> A key type to slot in NormalGroup which can contain a value ready to commit.
 * @param <FULL_KEY> A key type to DelayedGroup which contains a single slot and is
 *     singly-committed.
 * @param <EMIT_KEY> A key type that Emitter can interpret.
 * @param <V> A value type to be set to a slot.
 */
public class GroupCommitter<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(GroupCommitter.class);

  // This contains logics of how to treat keys.
  private final KeyManipulator<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY> keyManipulator;

  private final GroupManager<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> groupManager;

  /**
   * @param label A label used for thread name.
   * @param config A configuration.
   * @param keyManipulator A key manipulator that contains logics how to treat keys.
   */
  public GroupCommitter(
      String label,
      GroupCommitConfig config,
      KeyManipulator<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY> keyManipulator) {
    this.keyManipulator = keyManipulator;
    this.groupManager = new GroupManager<>(label, config, keyManipulator);
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
    while (true) {
      FULL_KEY fullKey = groupManager.reserveNewSlot(childKey);
      if (fullKey != null) {
        return fullKey;
      }
      logger.debug("Failed to reserve a new value slot. Retrying. key:{}", childKey);
    }
  }

  /**
   * Marks the slot associated with the specified key READY and then, waits until the group which
   * contains the slot is emitted.
   *
   * @param fullKey A full key associated with the slot already reserved with {@link
   *     GroupCommitter#reserve(CHILD_KEY childKey)}.
   * @param value A value to be set to the slot.
   * @throws GroupCommitException
   */
  public void ready(FULL_KEY fullKey, V value) throws GroupCommitException {
    Keys<PARENT_KEY, CHILD_KEY, FULL_KEY> keys = keyManipulator.keysFromFullKey(fullKey);
    while (true) {
      Group<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> group = groupManager.getGroup(keys);
      try {
        group.putValueToSlotAndWait(keys.childKey, value);
        return;
      } catch (GroupCommitAlreadyCompletedException e) {
        logger.warn("The group is already closed. Retrying. Group:{}, Keys:{}", group, keys);
      }
    }
  }

  /**
   * Removes the slot from the group.
   *
   * @param fullKey A full key to specify the slot.
   */
  public void remove(FULL_KEY fullKey) {
    Keys<PARENT_KEY, CHILD_KEY, FULL_KEY> keys = keyManipulator.keysFromFullKey(fullKey);
    try {
      Group<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> group = groupManager.getGroup(keys);
      group.removeSlot(keys.childKey);
    } catch (GroupCommitTargetNotFoundException e) {
      logger.warn("Failed to remove the slot. fullKey:{}", fullKey, e);
    }
  }

  /**
   * Closes the resources. The ExecutorServices used in GroupManager are created as daemon, so
   * calling this method isn't needed. But for testing, this should be called for resources.
   */
  @Override
  public void close() {
    groupManager.close();
  }
}
