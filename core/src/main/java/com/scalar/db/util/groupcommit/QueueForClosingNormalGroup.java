package com.scalar.db.util.groupcommit;

import java.time.Instant;

// A queue to contain NormalGroup instances. The following timeout occurs in this queue:
// - `group-close-expiration` fixes the size of expired NormalGroup.
class QueueForClosingNormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>
    extends Queue<NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>> {
  private final QueueForMovingDelayedSlot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>
      queueForMovingDelayedSlot;
  private final QueueForCleaningUpGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>
      queueForCleaningUpGroup;

  QueueForClosingNormalGroup(
      String label,
      long queueCheckIntervalInMillis,
      QueueForMovingDelayedSlot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>
          queueForMovingDelayedSlot,
      QueueForCleaningUpGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>
          queueForCleaningUpGroup) {
    super(
        label + "-group-commit-normal-group-close",
        queueCheckIntervalInMillis,
        RetryMode.KEEP_AT_HEAD);
    this.queueForMovingDelayedSlot = queueForMovingDelayedSlot;
    this.queueForCleaningUpGroup = queueForCleaningUpGroup;
  }

  private void enqueueItemToNextQueue(
      NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> normalGroup) {
    if (normalGroup.isReady()) {
      queueForCleaningUpGroup.add(normalGroup);
    } else {
      queueForMovingDelayedSlot.add(normalGroup);
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
