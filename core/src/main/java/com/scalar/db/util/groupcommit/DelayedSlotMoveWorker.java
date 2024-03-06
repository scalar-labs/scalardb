package com.scalar.db.util.groupcommit;

// A worker manages NormalGroup instances to move delayed slots to a new DelayedGroup.
// Ready NormalGroup is passed to GroupCleanupWorker.
class DelayedSlotMoveWorker<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>
    extends BackgroundWorker<NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V>> {
  private final GroupManager<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> groupManager;
  private final GroupCleanupWorker<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> groupCleanupWorker;

  DelayedSlotMoveWorker(
      String label,
      long queueCheckIntervalInMillis,
      GroupManager<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> groupManager,
      GroupCleanupWorker<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> groupCleanupWorker) {
    super(
        label + "-group-commit-delayed-slot-move",
        queueCheckIntervalInMillis,
        RetryMode.MOVE_TO_TAIL);
    this.groupManager = groupManager;
    this.groupCleanupWorker = groupCleanupWorker;
  }

  @Override
  boolean processItem(NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_KEY, V> normalGroup) {
    if (normalGroup.isReady()) {
      groupCleanupWorker.add(normalGroup);
      // Already ready. Should remove the item.
      return true;
    }

    if (normalGroup.delayedSlotMovedMillisAt() < System.currentTimeMillis()) {
      // Move delayed slots to a DelayedGroup so that the NormalGroup can be ready.
      boolean movedDelayedSlots = groupManager.moveDelayedSlotToDelayedGroup(normalGroup);

      // The status of the group may have changed
      if (normalGroup.isReady()) {
        groupCleanupWorker.add(normalGroup);
        // Already ready. Should remove the item.
        return true;
      }

      // If this is true, it means all delayed slots are moved and the normal group must be ready.
      assert !movedDelayedSlots;
    }

    // Should not remove the item.
    return false;
  }
}
