package com.scalar.db.util.groupcommit;

import javax.annotation.concurrent.Immutable;

@Immutable
public class GroupCommitConfig {
  private final int retentionSlotsCount;
  private final int groupCloseExpirationInMillis;
  private final int delayedSlotExpirationInMillis;
  private final int checkIntervalInMillis;

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
