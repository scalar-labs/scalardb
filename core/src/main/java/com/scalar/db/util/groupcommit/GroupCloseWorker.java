package com.scalar.db.util.groupcommit;

import java.time.Instant;

// A worker manages NormalGroup instances to close timed-out groups and pass them to
// DelayedSlotMoveWorker.
// Ready NormalGroup is passed to GroupCleanupWorker.
class GroupCloseWorker<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>
    extends BackgroundWorker<NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>> {
  private final DelayedSlotMoveWorker<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>
      delayedSlotMoveWorker;
  private final GroupCleanupWorker<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> groupCleanupWorker;

  GroupCloseWorker(
      String label,
      long queueCheckIntervalInMillis,
      DelayedSlotMoveWorker<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> delayedSlotMoveWorker,
      GroupCleanupWorker<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> groupCleanupWorker) {
    super(
        label + "-group-commit-normal-group-close",
        queueCheckIntervalInMillis,
        RetryMode.KEEP_AT_HEAD);
    this.delayedSlotMoveWorker = delayedSlotMoveWorker;
    this.groupCleanupWorker = groupCleanupWorker;
  }

  private void enqueueItemToNextQueue(
      NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> normalGroup) {
    if (normalGroup.isReady()) {
      groupCleanupWorker.add(normalGroup);
    } else {
      delayedSlotMoveWorker.add(normalGroup);
    }
  }

  @Override
  boolean processItem(NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> normalGroup) {
    // Close the group if needed.
    if (normalGroup.isClosed()) {
      enqueueItemToNextQueue(normalGroup);
      // It's already closed. Should remove the item.
      return true;
    }

    Instant now = Instant.now();
    if (now.isAfter(normalGroup.groupClosedAt())) {
      // Expired. Fix the size (== close).
      normalGroup.close();

      enqueueItemToNextQueue(normalGroup);

      // Should remove the item.
      return true;
    }

    // Should not remove the item.
    return false;
  }
}
