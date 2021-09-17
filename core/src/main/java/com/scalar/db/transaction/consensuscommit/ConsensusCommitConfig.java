package com.scalar.db.transaction.consensuscommit;

import com.google.common.base.Strings;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.util.Utility;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import javax.annotation.concurrent.Immutable;

@Immutable
@SuppressFBWarnings("JCIP_FIELD_ISNT_FINAL_IN_IMMUTABLE_CLASS")
public class ConsensusCommitConfig extends DatabaseConfig {
  public static final String PREFIX = DatabaseConfig.PREFIX + "consensus_commit.";
  public static final String SERIALIZABLE_STRATEGY = PREFIX + "serializable_strategy";

  private SerializableStrategy strategy;

  // for two-phase consensus commit
  public static final String TWO_PHASE_CONSENSUS_COMMIT_PREFIX = PREFIX + "2pcc.";
  public static final String ACTIVE_TRANSACTIONS_MANAGEMENT_ENABLED =
      TWO_PHASE_CONSENSUS_COMMIT_PREFIX + "active_transactions_management.enabled";

  private boolean activeTransactionsManagementEnabled;

  public ConsensusCommitConfig(File propertiesFile) throws IOException {
    super(propertiesFile);
  }

  public ConsensusCommitConfig(InputStream stream) throws IOException {
    super(stream);
  }

  public ConsensusCommitConfig(Properties properties) {
    super(properties);
  }

  @Override
  protected void load() {
    String transactionManager = getProperties().getProperty(DatabaseConfig.TRANSACTION_MANAGER);
    if (transactionManager != null && !transactionManager.equals("consensus-commit")) {
      throw new IllegalArgumentException(
          DatabaseConfig.TRANSACTION_MANAGER + " should be 'consensus-commit'");
    }

    super.load();

    if (!Strings.isNullOrEmpty(getProperties().getProperty(SERIALIZABLE_STRATEGY))) {
      strategy =
          SerializableStrategy.valueOf(
              getProperties().getProperty(SERIALIZABLE_STRATEGY).toUpperCase());
    } else {
      strategy = SerializableStrategy.EXTRA_READ;
    }

    String activeTransactionsManagementEnabledValue =
        getProperties().getProperty(ACTIVE_TRANSACTIONS_MANAGEMENT_ENABLED);
    if (Utility.isBooleanString(activeTransactionsManagementEnabledValue)) {
      activeTransactionsManagementEnabled =
          Boolean.parseBoolean(activeTransactionsManagementEnabledValue);
    } else {
      activeTransactionsManagementEnabled = true;
    }
  }

  public SerializableStrategy getSerializableStrategy() {
    return strategy;
  }

  public boolean isActiveTransactionsManagementEnabled() {
    return activeTransactionsManagementEnabled;
  }
}
