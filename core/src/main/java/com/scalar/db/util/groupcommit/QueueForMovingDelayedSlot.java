package com.scalar.db.util.groupcommit;

import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// A queue to contain DelayedGroup instances. The following timeout occurs in this queue:
// - `delayed-slot-move-expiration` moves expired slots in NormalGroup to DelayedGroup.
class QueueForMovingDelayedSlot<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>
    extends Queue<NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>> {
  private static final Logger logger = LoggerFactory.getLogger(QueueForCleaningUpGroup.class);
  private final GroupManager<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> groupManager;
  private final QueueForCleaningUpGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>
      queueForCleaningUpGroup;

  QueueForMovingDelayedSlot(
      String label,
      long queueCheckIntervalInMillis,
      GroupManager<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> groupManager,
      QueueForCleaningUpGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>
          queueForCleaningUpGroup) {
    super(
        label + "-group-commit-delayed-slot-move",
        queueCheckIntervalInMillis,
        RetryMode.MOVE_TO_TAIL);
    this.groupManager = groupManager;
    this.queueForCleaningUpGroup = queueForCleaningUpGroup;
  }

  @Override
  boolean processItem(NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> normalGroup) {
    if (normalGroup != null) {
      if (normalGroup.isReady()) {
        queueForCleaningUpGroup.add(normalGroup);
        // Already ready. Should remove the item.
        return true;
      } else {
        if (Instant.now().isAfter(normalGroup.delayedSlotMovedAt())) {
          // Should remove the item if it's handled well.
          return groupManager.moveDelayedSlotToDelayedGroup(normalGroup);
        }
      }
    }
    // Should not remove the item.
    return false;
  }
}
