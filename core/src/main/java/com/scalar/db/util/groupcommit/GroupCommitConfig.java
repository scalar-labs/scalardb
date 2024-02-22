package com.scalar.db.util.groupcommit;

import static com.scalar.db.config.ConfigUtils.getInt;

import com.scalar.db.config.DatabaseConfig;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
public class GroupCommitConfig {
  private static final Logger logger = LoggerFactory.getLogger(GroupCommitConfig.class);

  public static final String SUFFIX_OF_RETENTION_SLOTS_COUNT = "retention_slots_count";
  public static final String SUFFIX_OF_GROUP_CLOSE_EXPIRATION_MILLIS =
      "group_close_expiration_millis";
  public static final String SUFFIX_OF_DELAYED_SLOT_EXPIRATION_MILLIS =
      "delayed_slot_expiration_millis";
  public static final String SUFFIX_OF_CHECK_INTERVAL_MILLIS = "check_interval_millis";

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

  public static GroupCommitConfig fromDatabaseConfig(DatabaseConfig databaseConfig, String prefix) {
    if (!prefix.endsWith(".")) {
      prefix = prefix + ".";
    }

    return new GroupCommitConfig(
        getInt(databaseConfig.getProperties(), prefix + SUFFIX_OF_RETENTION_SLOTS_COUNT, 20),
        getInt(
            databaseConfig.getProperties(), prefix + SUFFIX_OF_GROUP_CLOSE_EXPIRATION_MILLIS, 40),
        getInt(
            databaseConfig.getProperties(),
            prefix + SUFFIX_OF_DELAYED_SLOT_EXPIRATION_MILLIS,
            1200),
        getInt(databaseConfig.getProperties(), prefix + SUFFIX_OF_CHECK_INTERVAL_MILLIS, 20));
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
