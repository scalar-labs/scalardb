package com.scalar.db.storage.rpc;

import static com.scalar.db.config.ConfigUtils.getInt;
import static com.scalar.db.config.ConfigUtils.getLong;

import com.scalar.db.config.DatabaseConfig;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@Immutable
public class GrpcConfig {

  public static final String PREFIX = DatabaseConfig.PREFIX + "grpc.";
  public static final String DEADLINE_DURATION_MILLIS = PREFIX + "deadline_duration_millis";
  public static final String MAX_INBOUND_MESSAGE_SIZE = PREFIX + "max_inbound_message_size";
  public static final String MAX_INBOUND_METADATA_SIZE = PREFIX + "max_inbound_metadata_size";

  public static final int DEFAULT_SERVER_PORT = 60051;
  public static final long DEFAULT_DEADLINE_DURATION_MILLIS = 60000; // 60 seconds

  private final String host;
  private final int port;

  private final long deadlineDurationMillis;
  @Nullable private final Integer maxInboundMessageSize;
  @Nullable private final Integer maxInboundMetadataSize;

  public GrpcConfig(DatabaseConfig databaseConfig) {
    String storage = databaseConfig.getStorage();
    String transactionManager = databaseConfig.getTransactionManager();
    if (!"grpc".equals(storage) && !"grpc".equals(transactionManager)) {
      throw new IllegalArgumentException(
          DatabaseConfig.STORAGE
              + " or "
              + DatabaseConfig.TRANSACTION_MANAGER
              + " should be 'grpc'");
    }

    if (databaseConfig.getContactPoints().isEmpty()) {
      throw new IllegalArgumentException(DatabaseConfig.CONTACT_POINTS + " is empty");
    }
    host = databaseConfig.getContactPoints().get(0);
    port =
        databaseConfig.getContactPort() == 0
            ? DEFAULT_SERVER_PORT
            : databaseConfig.getContactPort();

    deadlineDurationMillis =
        getLong(
            databaseConfig.getProperties(),
            DEADLINE_DURATION_MILLIS,
            DEFAULT_DEADLINE_DURATION_MILLIS);
    maxInboundMessageSize = getInt(databaseConfig.getProperties(), MAX_INBOUND_MESSAGE_SIZE, null);
    maxInboundMetadataSize =
        getInt(databaseConfig.getProperties(), MAX_INBOUND_METADATA_SIZE, null);
  }

  // For the SpotBugs warning CT_CONSTRUCTOR_THROW
  @Override
  protected final void finalize() {}

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public long getDeadlineDurationMillis() {
    return deadlineDurationMillis;
  }

  public Optional<Integer> getMaxInboundMessageSize() {
    return Optional.ofNullable(maxInboundMessageSize);
  }

  public Optional<Integer> getMaxInboundMetadataSize() {
    return Optional.ofNullable(maxInboundMetadataSize);
  }
}
