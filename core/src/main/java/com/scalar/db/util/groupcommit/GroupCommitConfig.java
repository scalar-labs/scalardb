package com.scalar.db.util.groupcommit;

import com.google.common.base.MoreObjects;
import javax.annotation.concurrent.Immutable;

/** A configuration for group commit */
@Immutable
public class GroupCommitConfig {
  private final int slotCapacity;
  private final int groupCloseTimeoutMillis;
  private final int delayedSlotMoveTimeoutMillis;
  private final int timeoutCheckIntervalMillis;

  /**
   * A configuration of group commit.
   *
   * @param slotCapacity How many slots can be stored in a {@link NormalGroup}.
   * @param groupCloseTimeoutMillis A timeout to close (or size-fix) a {@link NormalGroup}.
   * @param delayedSlotMoveTimeoutMillis A timeout to move a delayed slot from {@link NormalGroup}
   *     to {@link DelayedGroup}.
   * @param timeoutCheckIntervalMillis An interval to check the queues.
   */
  public GroupCommitConfig(
      int slotCapacity,
      int groupCloseTimeoutMillis,
      int delayedSlotMoveTimeoutMillis,
      int timeoutCheckIntervalMillis) {
    this.slotCapacity = slotCapacity;
    this.groupCloseTimeoutMillis = groupCloseTimeoutMillis;
    this.delayedSlotMoveTimeoutMillis = delayedSlotMoveTimeoutMillis;
    this.timeoutCheckIntervalMillis = timeoutCheckIntervalMillis;
  }

  // For the SpotBugs warning CT_CONSTRUCTOR_THROW
  @Override
  protected final void finalize() {}

  public int slotCapacity() {
    return slotCapacity;
  }

  public int groupSizeFixTimeoutMillis() {
    return groupCloseTimeoutMillis;
  }

  public int delayedSlotMoveTimeoutMillis() {
    return delayedSlotMoveTimeoutMillis;
  }

  public int timeoutCheckIntervalMillis() {
    return timeoutCheckIntervalMillis;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("slotCapacity", slotCapacity)
        .add("groupCloseTimeoutMillis", groupCloseTimeoutMillis)
        .add("delayedSlotMoveTimeoutMillis", delayedSlotMoveTimeoutMillis)
        .add("timeoutCheckIntervalMillis", timeoutCheckIntervalMillis)
        .toString();
  }
}
