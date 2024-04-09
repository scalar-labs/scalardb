package com.scalar.db.util.groupcommit;

import javax.annotation.concurrent.ThreadSafe;

// A worker manages NormalGroup instances to size-fix timed-out groups and pass them to
// DelayedSlotMoveWorker.
// Ready NormalGroup is passed to GroupCleanupWorker.
@ThreadSafe
class GroupSizeFixWorker<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>
    extends BackgroundWorker<NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>> {
  private final DelayedSlotMoveWorker<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>
      delayedSlotMoveWorker;
  private final GroupCleanupWorker<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> groupCleanupWorker;

  GroupSizeFixWorker(
      String label,
      long queueCheckIntervalInMillis,
      DelayedSlotMoveWorker<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> delayedSlotMoveWorker,
      GroupCleanupWorker<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> groupCleanupWorker) {
    super(
        label + "-group-commit-normal-group-size-fix",
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
    // Size-fix the group if needed.
    if (normalGroup.isSizeFixed()) {
      enqueueItemToNextQueue(normalGroup);
      // It's already size-fixed. Should remove the item.
      return true;
    }

    long now = System.currentTimeMillis();
    if (normalGroup.groupSizeFixTimeoutAtMillis() < now) {
      // Expired. Fix the size.
      normalGroup.fixSize();

      enqueueItemToNextQueue(normalGroup);

      // Should remove the item.
      return true;
    }

    // Should not remove the item.
    return false;
  }
}
