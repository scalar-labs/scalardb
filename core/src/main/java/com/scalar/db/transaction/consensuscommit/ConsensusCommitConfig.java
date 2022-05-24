package com.scalar.db.transaction.consensuscommit;

import static com.scalar.db.config.ConfigUtils.getBoolean;
import static com.scalar.db.config.ConfigUtils.getInt;
import static com.scalar.db.config.ConfigUtils.getString;

import com.scalar.db.config.DatabaseConfig;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
public class ConsensusCommitConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsensusCommitConfig.class);

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

  public static final int DEFAULT_PARALLEL_EXECUTOR_COUNT = 30;

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

  public ConsensusCommitConfig(DatabaseConfig databaseConfig) {
    if (databaseConfig.getProperties().containsValue("scalar.db.isolation_level")) {
      LOGGER.warn(
          "The property \"scalar.db.isolation_level\" is deprecated and will be removed. "
              + "Please use \""
              + ISOLATION_LEVEL
              + "\" instead.");
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
                .toUpperCase());
    strategy =
        SerializableStrategy.valueOf(
            getString(
                    databaseConfig.getProperties(),
                    SERIALIZABLE_STRATEGY,
                    SerializableStrategy.EXTRA_READ.toString())
                .toUpperCase());

    coordinatorNamespace = getString(databaseConfig.getProperties(), COORDINATOR_NAMESPACE, null);

    parallelExecutorCount =
        getInt(
            databaseConfig.getProperties(),
            PARALLEL_EXECUTOR_COUNT,
            DEFAULT_PARALLEL_EXECUTOR_COUNT);
    parallelPreparationEnabled =
        getBoolean(databaseConfig.getProperties(), PARALLEL_PREPARATION_ENABLED, false);
    parallelCommitEnabled =
        getBoolean(databaseConfig.getProperties(), PARALLEL_COMMIT_ENABLED, false);

    // Use the value of parallel commit for parallel validation and parallel rollback as default
    // value
    parallelValidationEnabled =
        getBoolean(
            databaseConfig.getProperties(), PARALLEL_VALIDATION_ENABLED, parallelCommitEnabled);
    parallelRollbackEnabled =
        getBoolean(
            databaseConfig.getProperties(), PARALLEL_ROLLBACK_ENABLED, parallelCommitEnabled);

    asyncCommitEnabled = getBoolean(databaseConfig.getProperties(), ASYNC_COMMIT_ENABLED, false);
    asyncRollbackEnabled =
        getBoolean(databaseConfig.getProperties(), ASYNC_ROLLBACK_ENABLED, asyncCommitEnabled);
  }

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
}
