package com.scalar.db.util.groupcommit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// A worker manages Group instances to removes completed ones.
class GroupCleanupWorker<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>
    extends BackgroundWorker<Group<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>> {
  private static final Logger logger = LoggerFactory.getLogger(GroupCleanupWorker.class);
  private final GroupManager<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> groupManager;

  GroupCleanupWorker(
      String label,
      long queueCheckIntervalInMillis,
      GroupManager<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> groupManager,
      CurrentTime currentTime) {
    super(
        label + "-group-commit-group-cleanup",
        queueCheckIntervalInMillis,
        RetryMode.MOVE_TO_TAIL,
        currentTime);
    this.groupManager = groupManager;
  }

  @Override
  boolean processItem(Group<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> group) {
    // Groups don't have chance to update the status from READY to DONE since the condition is
    // satisfied after all the clients get the result lazily. Therefore, update the status here.
    group.updateStatus();
    if (group.isDone()) {
      if (!groupManager.removeGroupFromMap(group)) {
        logger.warn("Failed to remove the group from the group map. Group:{}", group);
      }
      // The group is removed from the group map. Should remove it.
      return true;
    }
    // Should not remove the item.
    return false;
  }
}
