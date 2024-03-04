package com.scalar.db.util.groupcommit;

import com.google.common.base.MoreObjects;

// TODO: Remove after introducing a proper metrics.
class Metrics {
  public final int sizeOfQueueForClosingNormalGroup;
  public final int sizeOfQueueForMovingDelayedSlot;
  public final int sizeOfQueueForCleaningUpGroup;
  public final int sizeOfNormalGroupMap;
  public final int sizeOfDelayedGroupMap;

  Metrics(
      int sizeOfQueueForClosingNormalGroup,
      int sizeOfQueueForMovingDelayedSlot,
      int sizeOfQueueForCleaningUpGroup,
      int sizeOfNormalGroupMap,
      int sizeOfDelayedGroupMap) {
    this.sizeOfQueueForClosingNormalGroup = sizeOfQueueForClosingNormalGroup;
    this.sizeOfQueueForMovingDelayedSlot = sizeOfQueueForMovingDelayedSlot;
    this.sizeOfQueueForCleaningUpGroup = sizeOfQueueForCleaningUpGroup;
    this.sizeOfNormalGroupMap = sizeOfNormalGroupMap;
    this.sizeOfDelayedGroupMap = sizeOfDelayedGroupMap;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("sizeOfQueueForClosingNormalGroup", sizeOfQueueForClosingNormalGroup)
        .add("sizeOfQueueForMovingDelayedSlot", sizeOfQueueForMovingDelayedSlot)
        .add("sizeOfQueueForCleaningUpGroup", sizeOfQueueForCleaningUpGroup)
        .add("sizeOfNormalGroupMap", sizeOfNormalGroupMap)
        .add("sizeOfDelayedGroupMap", sizeOfDelayedGroupMap)
        .toString();
  }
}
