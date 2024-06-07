package com.scalar.db.util.groupcommit;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// A worker manages Group instances to removes completed ones.
@ThreadSafe
class GroupCleanupWorker<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V>
    extends BackgroundWorker<
        Group<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V>> {
  private static final Logger logger = LoggerFactory.getLogger(GroupCleanupWorker.class);
  private final GroupManager<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V>
      groupManager;

  GroupCleanupWorker(
      String label,
      long queueCheckIntervalInMillis,
      GroupManager<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V>
          groupManager) {
    super(
        label + "-group-commit-group-cleanup",
        queueCheckIntervalInMillis,
        // It's likely an item at the head stays not-done for a long time.
        RetryMode.RE_ENQUEUE);
    this.groupManager = groupManager;
  }

  @Override
  BlockingQueue<Group<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V>>
      createQueue() {
    // Use a normal queue because:
    // - The timeout of the queued groups is large since it's for "just in case"
    // - In most cases a queued group gets DONE before it's timed-out
    // - Therefore, scanning all the queued groups repeatedly without considering the timeout order
    //   is necessary
    return new LinkedBlockingQueue<>();
  }

  @Override
  boolean processItem(
      Group<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V> group) {
    if (group.oldGroupAbortTimeoutAtMillis() < System.currentTimeMillis()) {
      groupManager.removeGroupFromMap(group);
      group.abort();
      // Should remove the item.
      return true;
    }

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
