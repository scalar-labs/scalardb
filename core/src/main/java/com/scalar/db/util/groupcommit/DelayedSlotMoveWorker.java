package com.scalar.db.util.groupcommit;

import java.util.Comparator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import javax.annotation.concurrent.ThreadSafe;

// A worker manages NormalGroup instances to move delayed slots to a new DelayedGroup.
// Ready NormalGroup is passed to GroupCleanupWorker.
@ThreadSafe
class DelayedSlotMoveWorker<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V>
    extends BackgroundWorker<
        NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V>> {
  private final GroupManager<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V>
      groupManager;
  private final GroupCleanupWorker<
          PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V>
      groupCleanupWorker;

  DelayedSlotMoveWorker(
      String label,
      long queueCheckIntervalInMillis,
      GroupManager<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V> groupManager,
      GroupCleanupWorker<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V>
          groupCleanupWorker) {
    super(
        label + "-group-commit-delayed-slot-move",
        queueCheckIntervalInMillis,
        // Enqueued items of this worker can be out of order since `delayedSlotMoveTimeoutAt` would
        // be updated.
        RetryMode.RE_ENQUEUE);
    this.groupManager = groupManager;
    this.groupCleanupWorker = groupCleanupWorker;
  }

  @Override
  BlockingQueue<NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V>>
      createQueue() {
    // Use a priority queue to prioritize groups based on their timeout values, processing groups
    // with smaller timeout values first.
    return new PriorityBlockingQueue<>(
        64, Comparator.comparingLong(NormalGroup::delayedSlotMoveTimeoutAtMillis));
  }

  @Override
  boolean processItem(
      NormalGroup<PARENT_KEY, CHILD_KEY, FULL_KEY, EMIT_PARENT_KEY, EMIT_FULL_KEY, V> normalGroup) {
    if (normalGroup.isReady()) {
      groupCleanupWorker.add(normalGroup);
      // Already ready. Should remove the item.
      return true;
    }

    long currentTimeMillis = System.currentTimeMillis();

    if (normalGroup.oldGroupAbortTimeoutAtMillis() < currentTimeMillis) {
      // This garbage collection is needed considering the following case:
      // - There are two slots S-1 and S-2 in Group-A
      // - The both clients of the slots failed to remove the slots after failures
      // - The garbage slots will remain forever
      //
      // The garbage collection is only needed in this worker since:
      // - GroupSizeFixWorker manages only OPEN groups which will be eventually passed to
      //   DelayedSlotMoveWorker
      // - GroupCleanupWorker manages only READY groups whose client threads are already waiting
      //   until the group is emitted
      groupManager.removeGroupFromMap(normalGroup);
      normalGroup.abort();
      // Should remove the item.
      return true;
    }

    if (normalGroup.delayedSlotMoveTimeoutAtMillis() < currentTimeMillis) {
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
