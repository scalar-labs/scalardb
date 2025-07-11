package com.scalar.db.transaction.consensuscommit;

import static com.scalar.db.config.ConfigUtils.getBoolean;
import static com.scalar.db.config.ConfigUtils.getInt;
import static com.scalar.db.config.ConfigUtils.getString;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.jdbc.JdbcConfig;
import com.scalar.db.storage.multistorage.MultiStorageConfig;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
public class ConsensusCommitConfig {
  private static final Logger logger = LoggerFactory.getLogger(ConsensusCommitConfig.class);

  public static final String TRANSACTION_MANAGER_NAME = "consensus-commit";
  public static final String PREFIX = DatabaseConfig.PREFIX + "consensus_commit.";
  public static final String ISOLATION_LEVEL = PREFIX + "isolation_level";
  public static final String COORDINATOR_NAMESPACE = PREFIX + "coordinator.namespace";

  public static final String PARALLEL_EXECUTOR_COUNT = PREFIX + "parallel_executor_count";
  public static final String PARALLEL_PREPARATION_ENABLED = PREFIX + "parallel_preparation.enabled";
  public static final String PARALLEL_VALIDATION_ENABLED = PREFIX + "parallel_validation.enabled";
  public static final String PARALLEL_COMMIT_ENABLED = PREFIX + "parallel_commit.enabled";
  public static final String PARALLEL_ROLLBACK_ENABLED = PREFIX + "parallel_rollback.enabled";

  public static final String ASYNC_COMMIT_ENABLED = PREFIX + "async_commit.enabled";
  public static final String ASYNC_ROLLBACK_ENABLED = PREFIX + "async_rollback.enabled";

  public static final String COORDINATOR_WRITE_OMISSION_ON_READ_ONLY_ENABLED =
      PREFIX + "coordinator.write_omission_on_read_only.enabled";
  public static final String ONE_PHASE_COMMIT_ENABLED = PREFIX + "one_phase_commit.enabled";
  public static final String PARALLEL_IMPLICIT_PRE_READ =
      PREFIX + "parallel_implicit_pre_read.enabled";
  public static final String INCLUDE_METADATA_ENABLED = PREFIX + "include_metadata.enabled";

  public static final String COORDINATOR_GROUP_COMMIT_PREFIX = PREFIX + "coordinator.group_commit.";
  public static final String COORDINATOR_GROUP_COMMIT_ENABLED =
      COORDINATOR_GROUP_COMMIT_PREFIX + "enabled";
  public static final String COORDINATOR_GROUP_COMMIT_SLOT_CAPACITY =
      COORDINATOR_GROUP_COMMIT_PREFIX + "slot_capacity";
  public static final String COORDINATOR_GROUP_COMMIT_GROUP_SIZE_FIX_TIMEOUT_MILLIS =
      COORDINATOR_GROUP_COMMIT_PREFIX + "group_size_fix_timeout_millis";
  public static final String COORDINATOR_GROUP_COMMIT_DELAYED_SLOT_MOVE_TIMEOUT_MILLIS =
      COORDINATOR_GROUP_COMMIT_PREFIX + "delayed_slot_move_timeout_millis";
  public static final String COORDINATOR_GROUP_COMMIT_OLD_GROUP_ABORT_TIMEOUT_MILLIS =
      COORDINATOR_GROUP_COMMIT_PREFIX + "old_group_abort_timeout_millis";
  public static final String COORDINATOR_GROUP_COMMIT_TIMEOUT_CHECK_INTERVAL_MILLIS =
      COORDINATOR_GROUP_COMMIT_PREFIX + "timeout_check_interval_millis";
  public static final String COORDINATOR_GROUP_COMMIT_METRICS_MONITOR_LOG_ENABLED =
      COORDINATOR_GROUP_COMMIT_PREFIX + "metrics_monitor_log_enabled";

  public static final int DEFAULT_PARALLEL_EXECUTOR_COUNT = 128;

  public static final int DEFAULT_COORDINATOR_GROUP_COMMIT_SLOT_CAPACITY = 20;
  public static final int DEFAULT_COORDINATOR_GROUP_COMMIT_GROUP_SIZE_FIX_TIMEOUT_MILLIS = 40;
  public static final int DEFAULT_COORDINATOR_GROUP_COMMIT_DELAYED_SLOT_MOVE_TIMEOUT_MILLIS = 1200;
  public static final int DEFAULT_COORDINATOR_GROUP_COMMIT_OLD_GROUP_ABORT_TIMEOUT_MILLIS = 60000;
  public static final int DEFAULT_COORDINATOR_GROUP_COMMIT_TIMEOUT_CHECK_INTERVAL_MILLIS = 20;

  private final Isolation isolation;
  @Nullable private final String coordinatorNamespace;

  private final int parallelExecutorCount;
  private final boolean parallelPreparationEnabled;
  private final boolean parallelValidationEnabled;
  private final boolean parallelCommitEnabled;
  private final boolean parallelRollbackEnabled;
  private final boolean asyncCommitEnabled;
  private final boolean asyncRollbackEnabled;

  private final boolean coordinatorWriteOmissionOnReadOnlyEnabled;
  private final boolean onePhaseCommitEnabled;
  private final boolean parallelImplicitPreReadEnabled;
  private final boolean includeMetadataEnabled;

  private final boolean coordinatorGroupCommitEnabled;
  private final int coordinatorGroupCommitSlotCapacity;
  private final int coordinatorGroupCommitGroupSizeFixTimeoutMillis;
  private final int coordinatorGroupCommitDelayedSlotMoveTimeoutMillis;
  private final int coordinatorGroupCommitOldGroupAbortTimeoutMillis;
  private final int coordinatorGroupCommitTimeoutCheckIntervalMillis;
  private final boolean coordinatorGroupCommitMetricsMonitorLogEnabled;

  public ConsensusCommitConfig(DatabaseConfig databaseConfig) {
    String transactionManager = databaseConfig.getTransactionManager();
    if (!transactionManager.equals(TRANSACTION_MANAGER_NAME)) {
      throw new IllegalArgumentException(
          DatabaseConfig.TRANSACTION_MANAGER + " should be '" + TRANSACTION_MANAGER_NAME + "'");
    }

    Properties properties = databaseConfig.getProperties();

    if (properties.containsKey("scalar.db.isolation_level")) {
      logger.warn(
          "The property \"scalar.db.isolation_level\" is deprecated and will be removed in 5.0.0. "
              + "Please use \""
              + ISOLATION_LEVEL
              + "\" instead");
    }
    isolation =
        Isolation.valueOf(
            getString(
                    properties,
                    ISOLATION_LEVEL,
                    getString(
                        properties,
                        "scalar.db.isolation_level", // for backward compatibility
                        Isolation.SNAPSHOT.toString()))
                .toUpperCase(Locale.ROOT));
    if (isolation.equals(Isolation.SERIALIZABLE)) {
      validateCrossPartitionScanConfig(databaseConfig);

      if (properties.containsKey("scalar.db.consensus_commit.serializable_strategy")) {
        logger.warn(
            "The property \"scalar.db.consensus_commit.serializable_strategy\" is deprecated and will "
                + "be removed in 5.0.0. The EXTRA_READ strategy is always used for the SERIALIZABLE "
                + "isolation level.");
      }
    }

    coordinatorNamespace = getString(properties, COORDINATOR_NAMESPACE, null);

    parallelExecutorCount =
        getInt(properties, PARALLEL_EXECUTOR_COUNT, DEFAULT_PARALLEL_EXECUTOR_COUNT);
    parallelPreparationEnabled = getBoolean(properties, PARALLEL_PREPARATION_ENABLED, true);
    parallelCommitEnabled = getBoolean(properties, PARALLEL_COMMIT_ENABLED, true);

    // Use the value of parallel commit for parallel validation and parallel rollback as default
    // value
    parallelValidationEnabled =
        getBoolean(properties, PARALLEL_VALIDATION_ENABLED, parallelCommitEnabled);
    parallelRollbackEnabled =
        getBoolean(properties, PARALLEL_ROLLBACK_ENABLED, parallelCommitEnabled);

    asyncCommitEnabled = getBoolean(properties, ASYNC_COMMIT_ENABLED, false);

    // Use the value of async commit for async rollback as default value
    asyncRollbackEnabled = getBoolean(properties, ASYNC_ROLLBACK_ENABLED, asyncCommitEnabled);

    coordinatorWriteOmissionOnReadOnlyEnabled =
        getBoolean(properties, COORDINATOR_WRITE_OMISSION_ON_READ_ONLY_ENABLED, true);

    onePhaseCommitEnabled = getBoolean(properties, ONE_PHASE_COMMIT_ENABLED, false);

    parallelImplicitPreReadEnabled = getBoolean(properties, PARALLEL_IMPLICIT_PRE_READ, true);

    includeMetadataEnabled = getBoolean(properties, INCLUDE_METADATA_ENABLED, false);

    coordinatorGroupCommitEnabled = getBoolean(properties, COORDINATOR_GROUP_COMMIT_ENABLED, false);
    coordinatorGroupCommitSlotCapacity =
        getInt(
            properties,
            COORDINATOR_GROUP_COMMIT_SLOT_CAPACITY,
            DEFAULT_COORDINATOR_GROUP_COMMIT_SLOT_CAPACITY);
    coordinatorGroupCommitGroupSizeFixTimeoutMillis =
        getInt(
            properties,
            COORDINATOR_GROUP_COMMIT_GROUP_SIZE_FIX_TIMEOUT_MILLIS,
            DEFAULT_COORDINATOR_GROUP_COMMIT_GROUP_SIZE_FIX_TIMEOUT_MILLIS);
    coordinatorGroupCommitDelayedSlotMoveTimeoutMillis =
        getInt(
            properties,
            COORDINATOR_GROUP_COMMIT_DELAYED_SLOT_MOVE_TIMEOUT_MILLIS,
            DEFAULT_COORDINATOR_GROUP_COMMIT_DELAYED_SLOT_MOVE_TIMEOUT_MILLIS);
    coordinatorGroupCommitOldGroupAbortTimeoutMillis =
        getInt(
            properties,
            COORDINATOR_GROUP_COMMIT_OLD_GROUP_ABORT_TIMEOUT_MILLIS,
            DEFAULT_COORDINATOR_GROUP_COMMIT_OLD_GROUP_ABORT_TIMEOUT_MILLIS);
    coordinatorGroupCommitTimeoutCheckIntervalMillis =
        getInt(
            properties,
            COORDINATOR_GROUP_COMMIT_TIMEOUT_CHECK_INTERVAL_MILLIS,
            DEFAULT_COORDINATOR_GROUP_COMMIT_TIMEOUT_CHECK_INTERVAL_MILLIS);
    coordinatorGroupCommitMetricsMonitorLogEnabled =
        getBoolean(properties, COORDINATOR_GROUP_COMMIT_METRICS_MONITOR_LOG_ENABLED, false);
  }

  public Isolation getIsolation() {
    return isolation;
  }

  public Optional<String> getCoordinatorNamespace() {
    return Optional.ofNullable(coordinatorNamespace);
  }

  public int getParallelExecutorCount() {
    return parallelExecutorCount;
  }

  public boolean isParallelPreparationEnabled() {
    return parallelPreparationEnabled;
  }

  public boolean isParallelValidationEnabled() {
    return parallelValidationEnabled;
  }

  public boolean isParallelCommitEnabled() {
    return parallelCommitEnabled;
  }

  public boolean isParallelRollbackEnabled() {
    return parallelRollbackEnabled;
  }

  public boolean isAsyncCommitEnabled() {
    return asyncCommitEnabled;
  }

  public boolean isAsyncRollbackEnabled() {
    return asyncRollbackEnabled;
  }

  public boolean isCoordinatorWriteOmissionOnReadOnlyEnabled() {
    return coordinatorWriteOmissionOnReadOnlyEnabled;
  }

  public boolean isOnePhaseCommitEnabled() {
    return onePhaseCommitEnabled;
  }

  public boolean isParallelImplicitPreReadEnabled() {
    return parallelImplicitPreReadEnabled;
  }

  public boolean isIncludeMetadataEnabled() {
    return includeMetadataEnabled;
  }

  public boolean isCoordinatorGroupCommitEnabled() {
    return coordinatorGroupCommitEnabled;
  }

  public int getCoordinatorGroupCommitSlotCapacity() {
    return coordinatorGroupCommitSlotCapacity;
  }

  public int getCoordinatorGroupCommitGroupSizeFixTimeoutMillis() {
    return coordinatorGroupCommitGroupSizeFixTimeoutMillis;
  }

  public int getCoordinatorGroupCommitDelayedSlotMoveTimeoutMillis() {
    return coordinatorGroupCommitDelayedSlotMoveTimeoutMillis;
  }

  public int getCoordinatorGroupCommitOldGroupAbortTimeoutMillis() {
    return coordinatorGroupCommitOldGroupAbortTimeoutMillis;
  }

  public int getCoordinatorGroupCommitTimeoutCheckIntervalMillis() {
    return coordinatorGroupCommitTimeoutCheckIntervalMillis;
  }

  public boolean isCoordinatorGroupCommitMetricsMonitorLogEnabled() {
    return coordinatorGroupCommitMetricsMonitorLogEnabled;
  }

  private void validateCrossPartitionScanConfig(DatabaseConfig databaseConfig) {
    // It might be better to let each storage have metadata (e.g., linearizable cross-partition scan
    // is supported or not) and check it rather than checking specific storage types. We will
    // revisit here when supporting metadata management in DistributedStorage.
    if (databaseConfig.getStorage().equals(MultiStorageConfig.STORAGE_NAME)) {
      MultiStorageConfig multiStorageConfig = new MultiStorageConfig(databaseConfig);
      for (Properties props : multiStorageConfig.getDatabasePropertiesMap().values()) {
        DatabaseConfig c = new DatabaseConfig(props);
        if (!c.getStorage().equals(JdbcConfig.STORAGE_NAME) && c.isCrossPartitionScanEnabled()) {
          warnCrossPartitionScan(c.getStorage());
        }
      }
    } else if (!databaseConfig.getStorage().equals(JdbcConfig.STORAGE_NAME)
        && databaseConfig.isCrossPartitionScanEnabled()) {
      warnCrossPartitionScan(databaseConfig.getStorage());
    }
  }

  private void warnCrossPartitionScan(String storageName) {
    logger.warn(
        "Enabling the cross-partition scan for '{}' with the 'SERIALIZABLE' isolation level is not recommended "
            + "because transactions could be executed at a lower isolation level (that is, 'SNAPSHOT'). "
            + "When using non-JDBC databases, use cross-partition scan at your own risk only if consistency does not matter for your transactions.",
        storageName);
  }
}
