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
  public static final String SERIALIZABLE_STRATEGY = PREFIX + "serializable_strategy";
  public static final String COORDINATOR_NAMESPACE = PREFIX + "coordinator.namespace";

  public static final String PARALLEL_EXECUTOR_COUNT = PREFIX + "parallel_executor_count";
  public static final String PARALLEL_PREPARATION_ENABLED = PREFIX + "parallel_preparation.enabled";
  public static final String PARALLEL_VALIDATION_ENABLED = PREFIX + "parallel_validation.enabled";
  public static final String PARALLEL_COMMIT_ENABLED = PREFIX + "parallel_commit.enabled";
  public static final String PARALLEL_ROLLBACK_ENABLED = PREFIX + "parallel_rollback.enabled";

  public static final String ASYNC_COMMIT_ENABLED = PREFIX + "async_commit.enabled";
  public static final String ASYNC_ROLLBACK_ENABLED = PREFIX + "async_rollback.enabled";

  public static final String PARALLEL_IMPLICIT_PRE_READ =
      PREFIX + "parallel_implicit_pre_read.enabled";

  public static final int DEFAULT_PARALLEL_EXECUTOR_COUNT = 128;

  public static final String INCLUDE_METADATA_ENABLED = PREFIX + "include_metadata.enabled";

  private final Isolation isolation;
  private final SerializableStrategy strategy;
  @Nullable private final String coordinatorNamespace;

  private final int parallelExecutorCount;
  private final boolean parallelPreparationEnabled;
  private final boolean parallelValidationEnabled;
  private final boolean parallelCommitEnabled;
  private final boolean parallelRollbackEnabled;
  private final boolean asyncCommitEnabled;
  private final boolean asyncRollbackEnabled;

  private final boolean isIncludeMetadataEnabled;

  private final boolean parallelImplicitPreReadEnabled;

  public ConsensusCommitConfig(DatabaseConfig databaseConfig) {
    String transactionManager = databaseConfig.getTransactionManager();
    if (!transactionManager.equals(TRANSACTION_MANAGER_NAME)) {
      throw new IllegalArgumentException(
          DatabaseConfig.TRANSACTION_MANAGER + " should be '" + TRANSACTION_MANAGER_NAME + "'");
    }

    if (databaseConfig.getProperties().containsKey("scalar.db.isolation_level")) {
      logger.warn(
          "The property \"scalar.db.isolation_level\" is deprecated and will be removed in 5.0.0. "
              + "Please use \""
              + ISOLATION_LEVEL
              + "\" instead");
    }
    isolation =
        Isolation.valueOf(
            getString(
                    databaseConfig.getProperties(),
                    ISOLATION_LEVEL,
                    getString(
                        databaseConfig.getProperties(),
                        "scalar.db.isolation_level", // for backward compatibility
                        Isolation.SNAPSHOT.toString()))
                .toUpperCase(Locale.ROOT));
    if (isolation.equals(Isolation.SERIALIZABLE)) {
      validateCrossPartitionScanConfig(databaseConfig);
    }
    strategy =
        SerializableStrategy.valueOf(
            getString(
                    databaseConfig.getProperties(),
                    SERIALIZABLE_STRATEGY,
                    SerializableStrategy.EXTRA_READ.toString())
                .toUpperCase(Locale.ROOT));

    coordinatorNamespace = getString(databaseConfig.getProperties(), COORDINATOR_NAMESPACE, null);

    parallelExecutorCount =
        getInt(
            databaseConfig.getProperties(),
            PARALLEL_EXECUTOR_COUNT,
            DEFAULT_PARALLEL_EXECUTOR_COUNT);
    parallelPreparationEnabled =
        getBoolean(databaseConfig.getProperties(), PARALLEL_PREPARATION_ENABLED, true);
    parallelCommitEnabled =
        getBoolean(databaseConfig.getProperties(), PARALLEL_COMMIT_ENABLED, true);

    // Use the value of parallel commit for parallel validation and parallel rollback as default
    // value
    parallelValidationEnabled =
        getBoolean(
            databaseConfig.getProperties(), PARALLEL_VALIDATION_ENABLED, parallelCommitEnabled);
    parallelRollbackEnabled =
        getBoolean(
            databaseConfig.getProperties(), PARALLEL_ROLLBACK_ENABLED, parallelCommitEnabled);

    asyncCommitEnabled = getBoolean(databaseConfig.getProperties(), ASYNC_COMMIT_ENABLED, false);

    // Use the value of async commit for async rollback as default value
    asyncRollbackEnabled =
        getBoolean(databaseConfig.getProperties(), ASYNC_ROLLBACK_ENABLED, asyncCommitEnabled);

    isIncludeMetadataEnabled =
        getBoolean(databaseConfig.getProperties(), INCLUDE_METADATA_ENABLED, false);

    parallelImplicitPreReadEnabled =
        getBoolean(databaseConfig.getProperties(), PARALLEL_IMPLICIT_PRE_READ, true);
  }

  // For the SpotBugs warning CT_CONSTRUCTOR_THROW
  @Override
  protected final void finalize() {}

  public Isolation getIsolation() {
    return isolation;
  }

  public SerializableStrategy getSerializableStrategy() {
    return strategy;
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

  public boolean isIncludeMetadataEnabled() {
    return isIncludeMetadataEnabled;
  }

  public boolean isParallelImplicitPreReadEnabled() {
    return parallelImplicitPreReadEnabled;
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
