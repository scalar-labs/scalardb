package com.scalar.db.util.groupcommit;

import com.google.common.base.MoreObjects;
import jdk.nashorn.internal.ir.annotations.Immutable;

// TODO: Remove after introducing a proper metrics.
@Immutable
class Metrics {
  public final int queueLengthOfGroupCloseWorker;
  public final int queueLengthOfDelayedSlotMoveWorker;
  public final int queueLengthOfGroupCleanupWorker;
  public final int sizeOfNormalGroupMap;
  public final int sizeOfDelayedGroupMap;

  Metrics(
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
}
