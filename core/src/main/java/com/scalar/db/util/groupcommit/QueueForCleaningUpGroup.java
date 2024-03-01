package com.scalar.db.util.groupcommit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// A queue to contain ready Group instances which removes completed groups as cleaning up.
class QueueForCleaningUpGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>
    extends Queue<Group<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>> {
  private static final Logger logger = LoggerFactory.getLogger(QueueForCleaningUpGroup.class);
  private final GroupManager<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> groupManager;

  QueueForCleaningUpGroup(
      String label,
      long queueCheckIntervalInMillis,
      GroupManager<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> groupManager) {
    super(
        label + "-group-commit-group-cleanup", queueCheckIntervalInMillis, RetryMode.MOVE_TO_TAIL);
    this.groupManager = groupManager;
  }

  @Override
  boolean processItem(Group<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> group) {
    if (group != null) {
      // FIXME: Should see if all the slots are checked by the client thread.
      if (group.isDone()) {
        if (!groupManager.removeGroupFromMap(group)) {
          logger.warn("Failed to remove the group from the group map. Group:{}", group);
        }
        // The group is removed from the group map. Should remove it.
        return true;
      }
    }
    // Should not remove the item.
    return false;
  }
}
