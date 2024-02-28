package com.scalar.db.util.groupcommit;

import javax.annotation.concurrent.Immutable;

/** A configuration for group commit */
@Immutable
public class GroupCommitConfig {
  private final int retentionSlotsCount;
  private final int groupCloseExpirationInMillis;
  private final int delayedSlotExpirationInMillis;
  private final int checkIntervalInMillis;

  /**
   * A configuration of group commit.
   *
   * @param retentionSlotsCount How many slots can be stored in a {@link NormalGroup}.
   * @param groupCloseExpirationInMillis An expiration timeout to close (or size-fix) a {@link
   *     NormalGroup}.
   * @param delayedSlotExpirationInMillis An expiration timeout to move a delayed slot from {@link
   *     NormalGroup} to {@link DelayedGroup}.
   * @param checkIntervalInMillis An interval to check the queues.
   */
  public GroupCommitConfig(
      int retentionSlotsCount,
      int groupCloseExpirationInMillis,
      int delayedSlotExpirationInMillis,
      int checkIntervalInMillis) {
    this.retentionSlotsCount = retentionSlotsCount;
    this.groupCloseExpirationInMillis = groupCloseExpirationInMillis;
    this.delayedSlotExpirationInMillis = delayedSlotExpirationInMillis;
    this.checkIntervalInMillis = checkIntervalInMillis;
  }

  // For the SpotBugs warning CT_CONSTRUCTOR_THROW
  @Override
  protected final void finalize() {}

  public int retentionSlotsCount() {
    return retentionSlotsCount;
  }

  public int groupCloseExpirationInMillis() {
    return groupCloseExpirationInMillis;
  }

  public int delayedSlotExpirationInMillis() {
    return delayedSlotExpirationInMillis;
  }

  public int checkIntervalInMillis() {
    return checkIntervalInMillis;
  }
}
