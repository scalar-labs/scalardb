package com.scalar.db.util.groupcommit;

import com.google.common.base.MoreObjects;
import javax.annotation.concurrent.Immutable;

@Immutable
class GroupCommitMetrics {
  private final int queueLengthOfGroupCloseWorker;
  private final int queueLengthOfDelayedSlotMoveWorker;
  private final int queueLengthOfGroupCleanupWorker;
  private final int sizeOfNormalGroupMap;
  private final int sizeOfDelayedGroupMap;

  GroupCommitMetrics(
      int queueLengthOfGroupCloseWorker,
      int queueLengthOfDelayedSlotMoveWorker,
      int queueLengthOfGroupCleanupWorker,
      int sizeOfNormalGroupMap,
      int sizeOfDelayedGroupMap) {
    this.queueLengthOfGroupCloseWorker = queueLengthOfGroupCloseWorker;
    this.queueLengthOfDelayedSlotMoveWorker = queueLengthOfDelayedSlotMoveWorker;
    this.queueLengthOfGroupCleanupWorker = queueLengthOfGroupCleanupWorker;
    this.sizeOfNormalGroupMap = sizeOfNormalGroupMap;
    this.sizeOfDelayedGroupMap = sizeOfDelayedGroupMap;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("queueLengthOfGroupCloseWorker", queueLengthOfGroupCloseWorker)
        .add("queueLengthOfDelayedSlotMoveWorker", queueLengthOfDelayedSlotMoveWorker)
        .add("queueLengthOfGroupCleanupWorker", queueLengthOfGroupCleanupWorker)
        .add("sizeOfNormalGroupMap", sizeOfNormalGroupMap)
        .add("sizeOfDelayedGroupMap", sizeOfDelayedGroupMap)
        .toString();
  }

  public boolean hasRemaining() {
    return queueLengthOfGroupCloseWorker > 0
        || queueLengthOfDelayedSlotMoveWorker > 0
        || queueLengthOfGroupCleanupWorker > 0
        || sizeOfNormalGroupMap > 0
        || sizeOfDelayedGroupMap > 0;
  }

  // Add getters if necessary
}
