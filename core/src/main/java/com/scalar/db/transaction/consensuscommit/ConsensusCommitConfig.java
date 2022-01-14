package com.scalar.db.transaction.consensuscommit;

import static com.scalar.db.config.ConfigUtils.getBoolean;
import static com.scalar.db.config.ConfigUtils.getInt;
import static com.scalar.db.config.ConfigUtils.getLong;
import static com.scalar.db.config.ConfigUtils.getString;

import com.scalar.db.config.DatabaseConfig;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
@SuppressFBWarnings("JCIP_FIELD_ISNT_FINAL_IN_IMMUTABLE_CLASS")
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

  private final Properties props;

  private Isolation isolation;
  private SerializableStrategy strategy;
  @Nullable private String coordinatorNamespace;

  private int parallelExecutorCount;
  private boolean parallelPreparationEnabled;
  private boolean parallelValidationEnabled;
  private boolean parallelCommitEnabled;
  private boolean parallelRollbackEnabled;
  private boolean asyncCommitEnabled;
  private boolean asyncRollbackEnabled;

  private long tableMetadataCacheExpirationTimeSecs;

  // for two-phase consensus commit
  public static final String TWO_PHASE_CONSENSUS_COMMIT_PREFIX = PREFIX + "2pcc.";
  public static final String ACTIVE_TRANSACTIONS_MANAGEMENT_ENABLED =
      TWO_PHASE_CONSENSUS_COMMIT_PREFIX + "active_transactions_management.enabled";

  private boolean activeTransactionsManagementEnabled;

  public ConsensusCommitConfig(File propertiesFile) throws IOException {
    try (FileInputStream stream = new FileInputStream(propertiesFile)) {
      props = new Properties();
      props.load(stream);
    }
    load();
  }

  public ConsensusCommitConfig(InputStream stream) throws IOException {
    props = new Properties();
    props.load(stream);
    load();
  }

  public ConsensusCommitConfig(Properties properties) {
    props = new Properties();
    props.putAll(properties);
    load();
  }

  public Properties getProperties() {
    return props;
  }

  protected void load() {
    if (getProperties().containsValue("scalar.db.isolation_level")) {
      LOGGER.warn(
          "The property \"scalar.db.isolation_level\" is deprecated and will be removed. "
              + "Please use \""
              + ISOLATION_LEVEL
              + "\" instead.");
    }
    isolation =
        Isolation.valueOf(
            getString(
                    getProperties(),
                    ISOLATION_LEVEL,
                    getString(
                        getProperties(),
                        "scalar.db.isolation_level", // for backward compatibility
                        Isolation.SNAPSHOT.toString()))
                .toUpperCase());
    strategy =
        SerializableStrategy.valueOf(
            getString(
                    getProperties(),
                    SERIALIZABLE_STRATEGY,
                    SerializableStrategy.EXTRA_READ.toString())
                .toUpperCase());

    activeTransactionsManagementEnabled =
        getBoolean(getProperties(), ACTIVE_TRANSACTIONS_MANAGEMENT_ENABLED, true);

    coordinatorNamespace = getString(getProperties(), COORDINATOR_NAMESPACE, null);

    parallelExecutorCount =
        getInt(getProperties(), PARALLEL_EXECUTOR_COUNT, DEFAULT_PARALLEL_EXECUTOR_COUNT);
    parallelPreparationEnabled = getBoolean(getProperties(), PARALLEL_PREPARATION_ENABLED, false);
    parallelCommitEnabled = getBoolean(getProperties(), PARALLEL_COMMIT_ENABLED, false);

    // Use the value of parallel commit for parallel validation and parallel rollback as default
    // value
    parallelValidationEnabled =
        getBoolean(getProperties(), PARALLEL_VALIDATION_ENABLED, parallelCommitEnabled);
    parallelRollbackEnabled =
        getBoolean(getProperties(), PARALLEL_ROLLBACK_ENABLED, parallelCommitEnabled);

    asyncCommitEnabled = getBoolean(getProperties(), ASYNC_COMMIT_ENABLED, false);
    asyncRollbackEnabled = getBoolean(getProperties(), ASYNC_ROLLBACK_ENABLED, asyncCommitEnabled);

    // Use the same property as the table metadata cache expiration time for the transactional table
    // metadata expiration time
    tableMetadataCacheExpirationTimeSecs =
        getLong(getProperties(), DatabaseConfig.TABLE_METADATA_CACHE_EXPIRATION_TIME_SECS, -1);
  }

  public Isolation getIsolation() {
    return isolation;
  }

  public SerializableStrategy getSerializableStrategy() {
    return strategy;
  }

  public boolean isActiveTransactionsManagementEnabled() {
    return activeTransactionsManagementEnabled;
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

  public long getTableMetadataCacheExpirationTimeSecs() {
    return tableMetadataCacheExpirationTimeSecs;
  }
}
