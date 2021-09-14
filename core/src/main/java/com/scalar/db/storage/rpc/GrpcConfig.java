package com.scalar.db.storage.rpc;

import com.google.common.base.Strings;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.util.Utility;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
@SuppressFBWarnings("JCIP_FIELD_ISNT_FINAL_IN_IMMUTABLE_CLASS")
public class GrpcConfig extends DatabaseConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcConfig.class);

  public static final String PREFIX = DatabaseConfig.PREFIX + "grpc.";
  public static final String DEADLINE_DURATION_MILLIS = PREFIX + "deadline_duration_millis";

  public static final long DEFAULT_DEADLINE_DURATION_MILLIS = 60000; // 60 seconds

  private long deadlineDurationMillis;

  // for two-phase commit transactions
  public static final String TWO_PHASE_COMMIT_TRANSACTION_PREFIX = PREFIX + "2pc.";
  public static final String ACTIVE_TRANSACTIONS_MANAGEMENT_ENABLED =
      TWO_PHASE_COMMIT_TRANSACTION_PREFIX + "active_transactions_management.enabled";

  private boolean activeTransactionsManagementEnabled;

  public GrpcConfig(File propertiesFile) throws IOException {
    super(propertiesFile);
  }

  public GrpcConfig(InputStream stream) throws IOException {
    super(stream);
  }

  public GrpcConfig(Properties properties) {
    super(properties);
  }

  @Override
  protected void load() {
    String storage = getProperties().getProperty(DatabaseConfig.STORAGE);
    if (storage == null || !storage.equals("grpc")) {
      throw new IllegalArgumentException(DatabaseConfig.STORAGE + " should be 'grpc'");
    }

    super.load();

    deadlineDurationMillis = getLong(DEADLINE_DURATION_MILLIS, DEFAULT_DEADLINE_DURATION_MILLIS);

    String activeTransactionsManagementEnabledValue =
        getProperties().getProperty(ACTIVE_TRANSACTIONS_MANAGEMENT_ENABLED);
    if (Utility.isBooleanString(activeTransactionsManagementEnabledValue)) {
      activeTransactionsManagementEnabled =
          Boolean.parseBoolean(activeTransactionsManagementEnabledValue);
    } else {
      activeTransactionsManagementEnabled = true;
    }
  }

  private long getLong(String name, long defaultValue) {
    String value = getProperties().getProperty(name);
    if (Strings.isNullOrEmpty(value)) {
      return defaultValue;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException ignored) {
      LOGGER.warn(
          "the specified value of '{}' is not a number. using the default value: {}",
          name,
          defaultValue);
      return defaultValue;
    }
  }

  public long getDeadlineDurationMillis() {
    return deadlineDurationMillis;
  }

  public boolean isActiveTransactionsManagementEnabled() {
    return activeTransactionsManagementEnabled;
  }
}
