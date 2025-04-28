package com.scalar.db.transaction.consensuscommit;

import static com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig.COORDINATOR_GROUP_COMMIT_DELAYED_SLOT_MOVE_TIMEOUT_MILLIS;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig.COORDINATOR_GROUP_COMMIT_ENABLED;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig.COORDINATOR_GROUP_COMMIT_GROUP_SIZE_FIX_TIMEOUT_MILLIS;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig.COORDINATOR_GROUP_COMMIT_METRICS_MONITOR_LOG_ENABLED;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig.COORDINATOR_GROUP_COMMIT_OLD_GROUP_ABORT_TIMEOUT_MILLIS;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig.COORDINATOR_GROUP_COMMIT_SLOT_CAPACITY;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig.COORDINATOR_GROUP_COMMIT_TIMEOUT_CHECK_INTERVAL_MILLIS;

import java.util.Properties;

public final class ConsensusCommitTestUtils {
  private static final String PROP_CONSENSUS_COMMIT_COORDINATOR_GROUP_COMMIT_ENABLED =
      "scalardb.consensus_commit.coordinator.group_commit.enabled";
  private static final String PROP_CONSENSUS_COMMIT_COORDINATOR_GROUP_COMMIT_SLOT_CAPACITY =
      "scalardb.consensus_commit.coordinator.group_commit.slot_capacity";
  private static final String
      PROP_CONSENSUS_COMMIT_COORDINATOR_GROUP_COMMIT_GROUP_SIZE_FIX_TIMEOUT_MILLIS =
          "scalardb.consensus_commit.coordinator.group_commit.group_size_fix_timeout_millis";
  private static final String
      PROP_CONSENSUS_COMMIT_COORDINATOR_GROUP_COMMIT_DELAYED_SLOT_MOVE_TIMEOUT_MILLIS =
          "scalardb.consensus_commit.coordinator.group_commit.delayed_slot_move_timeout_millis";
  private static final String
      PROP_CONSENSUS_COMMIT_COORDINATOR_GROUP_COMMIT_OLD_GROUP_ABORT_TIMEOUT_MILLIS =
          "scalardb.consensus_commit.coordinator.group_commit.old_group_abort_timeout_millis";
  private static final String
      PROP_CONSENSUS_COMMIT_COORDINATOR_GROUP_COMMIT_TIMEOUT_CHECK_INTERVAL_MILLIS =
          "scalardb.consensus_commit.coordinator.group_commit.timeout_check_interval_millis";
  private static final String
      PROP_CONSENSUS_COMMIT_COORDINATOR_GROUP_COMMIT_METRICS_MONITOR_LOG_ENABLED =
          "scalardb.consensus_commit.coordinator.group_commit.metrics_monitor_log_enabled";

  private ConsensusCommitTestUtils() {}

  private static void addProperty(
      Properties properties, String systemPropName, String configPropName) {
    if (System.getProperty(systemPropName) != null) {
      properties.setProperty(configPropName, System.getProperty(systemPropName));
    }
  }

  public static Properties loadConsensusCommitProperties(Properties properties) {
    addProperty(
        properties,
        PROP_CONSENSUS_COMMIT_COORDINATOR_GROUP_COMMIT_ENABLED,
        COORDINATOR_GROUP_COMMIT_ENABLED);
    addProperty(
        properties,
        PROP_CONSENSUS_COMMIT_COORDINATOR_GROUP_COMMIT_SLOT_CAPACITY,
        COORDINATOR_GROUP_COMMIT_SLOT_CAPACITY);
    addProperty(
        properties,
        PROP_CONSENSUS_COMMIT_COORDINATOR_GROUP_COMMIT_GROUP_SIZE_FIX_TIMEOUT_MILLIS,
        COORDINATOR_GROUP_COMMIT_GROUP_SIZE_FIX_TIMEOUT_MILLIS);
    addProperty(
        properties,
        PROP_CONSENSUS_COMMIT_COORDINATOR_GROUP_COMMIT_DELAYED_SLOT_MOVE_TIMEOUT_MILLIS,
        COORDINATOR_GROUP_COMMIT_DELAYED_SLOT_MOVE_TIMEOUT_MILLIS);
    addProperty(
        properties,
        PROP_CONSENSUS_COMMIT_COORDINATOR_GROUP_COMMIT_OLD_GROUP_ABORT_TIMEOUT_MILLIS,
        COORDINATOR_GROUP_COMMIT_OLD_GROUP_ABORT_TIMEOUT_MILLIS);
    addProperty(
        properties,
        PROP_CONSENSUS_COMMIT_COORDINATOR_GROUP_COMMIT_TIMEOUT_CHECK_INTERVAL_MILLIS,
        COORDINATOR_GROUP_COMMIT_TIMEOUT_CHECK_INTERVAL_MILLIS);
    addProperty(
        properties,
        PROP_CONSENSUS_COMMIT_COORDINATOR_GROUP_COMMIT_METRICS_MONITOR_LOG_ENABLED,
        COORDINATOR_GROUP_COMMIT_METRICS_MONITOR_LOG_ENABLED);
    return properties;
  }
}
