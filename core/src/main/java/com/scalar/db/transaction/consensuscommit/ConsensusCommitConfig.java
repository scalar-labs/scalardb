package com.scalar.db.transaction.consensuscommit;

import static com.scalar.db.config.ConfigUtils.getBoolean;
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

  private final Properties props;

  private Isolation isolation;
  private SerializableStrategy strategy;
  @Nullable private String coordinatorNamespace;

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
}
