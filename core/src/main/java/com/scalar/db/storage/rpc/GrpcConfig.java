package com.scalar.db.storage.rpc;

import static com.scalar.db.config.ConfigUtils.getBoolean;
import static com.scalar.db.config.ConfigUtils.getLong;

import com.scalar.db.config.DatabaseConfig;
import javax.annotation.concurrent.Immutable;

@Immutable
public class GrpcConfig {

  public static final String PREFIX = DatabaseConfig.PREFIX + "grpc.";
  public static final String DEADLINE_DURATION_MILLIS = PREFIX + "deadline_duration_millis";

  public static final int DEFAULT_SCALAR_DB_SERVER_PORT = 60051;
  public static final long DEFAULT_DEADLINE_DURATION_MILLIS = 60000; // 60 seconds

  private final String host;
  private final int port;

  private final long deadlineDurationMillis;

  // for two-phase commit transactions
  public static final String TWO_PHASE_COMMIT_TRANSACTION_PREFIX = PREFIX + "2pc.";
  public static final String ACTIVE_TRANSACTIONS_MANAGEMENT_ENABLED =
      TWO_PHASE_COMMIT_TRANSACTION_PREFIX + "active_transactions_management.enabled";

  private final boolean activeTransactionsManagementEnabled;

  public GrpcConfig(DatabaseConfig databaseConfig) {
    String storage = databaseConfig.getProperties().getProperty(DatabaseConfig.STORAGE);
    if (storage == null || !storage.equals("grpc")) {
      throw new IllegalArgumentException(DatabaseConfig.STORAGE + " should be 'grpc'");
    }

    host = databaseConfig.getContactPoints().get(0);
    port =
        databaseConfig.getContactPort() == 0
            ? DEFAULT_SCALAR_DB_SERVER_PORT
            : databaseConfig.getContactPort();
    deadlineDurationMillis =
        getLong(
            databaseConfig.getProperties(),
            DEADLINE_DURATION_MILLIS,
            DEFAULT_DEADLINE_DURATION_MILLIS);
    activeTransactionsManagementEnabled =
        getBoolean(databaseConfig.getProperties(), ACTIVE_TRANSACTIONS_MANAGEMENT_ENABLED, true);
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public long getDeadlineDurationMillis() {
    return deadlineDurationMillis;
  }

  public boolean isActiveTransactionsManagementEnabled() {
    return activeTransactionsManagementEnabled;
  }
}
