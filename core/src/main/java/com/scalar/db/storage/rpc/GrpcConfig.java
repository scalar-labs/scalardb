package com.scalar.db.storage.rpc;

import static com.scalar.db.config.ConfigUtils.getBoolean;
import static com.scalar.db.config.ConfigUtils.getLong;

import com.scalar.db.config.DatabaseConfig;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import javax.annotation.concurrent.Immutable;

@SuppressFBWarnings("JCIP_FIELD_ISNT_FINAL_IN_IMMUTABLE_CLASS")
@Immutable
public class GrpcConfig extends DatabaseConfig {

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

    deadlineDurationMillis =
        getLong(getProperties(), DEADLINE_DURATION_MILLIS, DEFAULT_DEADLINE_DURATION_MILLIS);
    activeTransactionsManagementEnabled =
        getBoolean(getProperties(), ACTIVE_TRANSACTIONS_MANAGEMENT_ENABLED, true);
  }

  public long getDeadlineDurationMillis() {
    return deadlineDurationMillis;
  }

  public boolean isActiveTransactionsManagementEnabled() {
    return activeTransactionsManagementEnabled;
  }
}
