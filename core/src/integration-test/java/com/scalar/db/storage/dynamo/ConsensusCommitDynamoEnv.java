package com.scalar.db.storage.dynamo;

import static com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig.COORDINATOR_GROUP_COMMIT_DELAYED_SLOT_MOVE_TIMEOUT_MILLIS;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig.COORDINATOR_GROUP_COMMIT_ENABLED;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig.COORDINATOR_GROUP_COMMIT_GROUP_SIZE_FIX_TIMEOUT_MILLIS;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig.COORDINATOR_GROUP_COMMIT_METRICS_MONITOR_LOG_ENABLED;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig.COORDINATOR_GROUP_COMMIT_OLD_GROUP_ABORT_TIMEOUT_MILLIS;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig.COORDINATOR_GROUP_COMMIT_SLOT_CAPACITY;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig.COORDINATOR_GROUP_COMMIT_TIMEOUT_CHECK_INTERVAL_MILLIS;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig.DEFAULT_COORDINATOR_GROUP_COMMIT_DELAYED_SLOT_MOVE_TIMEOUT_MILLIS;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig.DEFAULT_COORDINATOR_GROUP_COMMIT_GROUP_SIZE_FIX_TIMEOUT_MILLIS;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig.DEFAULT_COORDINATOR_GROUP_COMMIT_OLD_GROUP_ABORT_TIMEOUT_MILLIS;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig.DEFAULT_COORDINATOR_GROUP_COMMIT_SLOT_CAPACITY;
import static com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig.DEFAULT_COORDINATOR_GROUP_COMMIT_TIMEOUT_CHECK_INTERVAL_MILLIS;

import java.util.Map;
import java.util.Properties;

public final class ConsensusCommitDynamoEnv {
  private ConsensusCommitDynamoEnv() {}

  public static Properties getProperties(String testName) {
    Properties properties = DynamoEnv.getProperties(testName);
    properties.setProperty(
        COORDINATOR_GROUP_COMMIT_ENABLED,
        System.getProperty(COORDINATOR_GROUP_COMMIT_ENABLED, "false"));
    properties.setProperty(
        COORDINATOR_GROUP_COMMIT_SLOT_CAPACITY,
        System.getProperty(
            COORDINATOR_GROUP_COMMIT_SLOT_CAPACITY,
            String.valueOf(DEFAULT_COORDINATOR_GROUP_COMMIT_SLOT_CAPACITY)));
    properties.setProperty(
        COORDINATOR_GROUP_COMMIT_GROUP_SIZE_FIX_TIMEOUT_MILLIS,
        System.getProperty(
            COORDINATOR_GROUP_COMMIT_GROUP_SIZE_FIX_TIMEOUT_MILLIS,
            String.valueOf(DEFAULT_COORDINATOR_GROUP_COMMIT_GROUP_SIZE_FIX_TIMEOUT_MILLIS)));
    properties.setProperty(
        COORDINATOR_GROUP_COMMIT_DELAYED_SLOT_MOVE_TIMEOUT_MILLIS,
        System.getProperty(
            COORDINATOR_GROUP_COMMIT_DELAYED_SLOT_MOVE_TIMEOUT_MILLIS,
            String.valueOf(DEFAULT_COORDINATOR_GROUP_COMMIT_DELAYED_SLOT_MOVE_TIMEOUT_MILLIS)));
    properties.setProperty(
        COORDINATOR_GROUP_COMMIT_OLD_GROUP_ABORT_TIMEOUT_MILLIS,
        System.getProperty(
            COORDINATOR_GROUP_COMMIT_OLD_GROUP_ABORT_TIMEOUT_MILLIS,
            String.valueOf(DEFAULT_COORDINATOR_GROUP_COMMIT_OLD_GROUP_ABORT_TIMEOUT_MILLIS)));
    properties.setProperty(
        COORDINATOR_GROUP_COMMIT_TIMEOUT_CHECK_INTERVAL_MILLIS,
        System.getProperty(
            COORDINATOR_GROUP_COMMIT_TIMEOUT_CHECK_INTERVAL_MILLIS,
            String.valueOf(DEFAULT_COORDINATOR_GROUP_COMMIT_TIMEOUT_CHECK_INTERVAL_MILLIS)));
    properties.setProperty(
        COORDINATOR_GROUP_COMMIT_METRICS_MONITOR_LOG_ENABLED,
        System.getProperty(COORDINATOR_GROUP_COMMIT_METRICS_MONITOR_LOG_ENABLED, "false"));
    return properties;
  }

  public static Map<String, String> getCreationOptions() {
    return DynamoEnv.getCreationOptions();
  }
}
