package com.scalar.db.util.groupcommit;

import com.google.common.base.MoreObjects;
import javax.annotation.concurrent.Immutable;

/** A configuration for group commit */
@Immutable
public class GroupCommitConfig {
  private final int slotCapacity;
  private final int groupSizeFixTimeoutMillis;
  private final int delayedSlotMoveTimeoutMillis;
  private final int oldGroupAbortTimeoutSeconds;
  private final int timeoutCheckIntervalMillis;
  private final boolean metricsConsoleReporterEnabled;

  /**
   * A configuration of group commit.
   *
   * @param slotCapacity How many slots can be stored in a {@link NormalGroup}.
   * @param groupSizeFixTimeoutMillis A timeout to close (or size-fix) a {@link NormalGroup}.
   * @param delayedSlotMoveTimeoutMillis A timeout to move a delayed slot from {@link NormalGroup}
   *     to {@link DelayedGroup}.
   * @param oldGroupAbortTimeoutSeconds A timeout to abort too old {@link Group}.
   * @param timeoutCheckIntervalMillis An interval to check the queues.
   * @param metricsConsoleReporterEnabled Whether to enable the console reporter for metrics.
   */
  public GroupCommitConfig(
      int slotCapacity,
      int groupSizeFixTimeoutMillis,
      int delayedSlotMoveTimeoutMillis,
      int oldGroupAbortTimeoutSeconds,
      int timeoutCheckIntervalMillis,
      boolean metricsConsoleReporterEnabled) {
    this.slotCapacity = slotCapacity;
    this.groupSizeFixTimeoutMillis = groupSizeFixTimeoutMillis;
    this.delayedSlotMoveTimeoutMillis = delayedSlotMoveTimeoutMillis;
    this.oldGroupAbortTimeoutSeconds = oldGroupAbortTimeoutSeconds;
    this.timeoutCheckIntervalMillis = timeoutCheckIntervalMillis;
    this.metricsConsoleReporterEnabled = metricsConsoleReporterEnabled;
  }

  // Mainly only for testing.
  public GroupCommitConfig(
      int slotCapacity,
      int groupSizeFixTimeoutMillis,
      int delayedSlotMoveTimeoutMillis,
      int oldGroupAbortTimeoutSeconds,
      int timeoutCheckIntervalMillis) {
    this(
        slotCapacity,
        groupSizeFixTimeoutMillis,
        delayedSlotMoveTimeoutMillis,
        oldGroupAbortTimeoutSeconds,
        timeoutCheckIntervalMillis,
        false);
  }

  // For the SpotBugs warning CT_CONSTRUCTOR_THROW
  @Override
  protected final void finalize() {}

  public int slotCapacity() {
    return slotCapacity;
  }

  public int groupSizeFixTimeoutMillis() {
    return groupSizeFixTimeoutMillis;
  }

  public int delayedSlotMoveTimeoutMillis() {
    return delayedSlotMoveTimeoutMillis;
  }

  public int oldGroupAbortTimeoutSeconds() {
    return oldGroupAbortTimeoutSeconds;
  }

  public int timeoutCheckIntervalMillis() {
    return timeoutCheckIntervalMillis;
  }

  public boolean metricsConsoleReporterEnabled() {
    return metricsConsoleReporterEnabled;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("slotCapacity", slotCapacity)
        .add("groupSizeFixTimeoutMillis", groupSizeFixTimeoutMillis)
        .add("delayedSlotMoveTimeoutMillis", delayedSlotMoveTimeoutMillis)
        .add("oldGroupAbortTimeoutSeconds", oldGroupAbortTimeoutSeconds)
        .add("timeoutCheckIntervalMillis", timeoutCheckIntervalMillis)
        .add("metricsConsoleReporterEnabled", metricsConsoleReporterEnabled)
        .toString();
  }
}
