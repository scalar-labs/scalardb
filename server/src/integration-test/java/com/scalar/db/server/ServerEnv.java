package com.scalar.db.server;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.rpc.GrpcConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.Isolation;
import com.scalar.db.transaction.consensuscommit.SerializableStrategy;
import java.util.Properties;
import javax.annotation.Nullable;

public final class ServerEnv {

  private static final String PROP_SERVER_EXTERNAL_SERVER_USED =
      "scalardb.server.external_server_used";
  private static final String DEFAULT_SERVER_EXTERNAL_SERVER_USED = "false";

  private static final String PROP_SERVER_CONTACT_POINTS = "scalardb.server.contact_points";
  private static final String PROP_SERVER_CONTACT_PORT = "scalardb.server.contact_port";
  private static final String PROP_SERVER_USERNAME = "scalardb.server.username";
  private static final String PROP_SERVER_PASSWORD = "scalardb.server.password";
  private static final String PROP_SERVER_STORAGE = "scalardb.server.storage";

  private static final String DEFAULT_SERVER_CONTACT_POINTS = "jdbc:mysql://localhost:3306/";
  private static final String DEFAULT_SERVER_USERNAME = "root";
  private static final String DEFAULT_SERVER_PASSWORD = "mysql";
  private static final String DEFAULT_SERVER_STORAGE = "jdbc";

  private static final String PROP_SERVER_JDBC_URL = "scalardb.server.jdbc.url";
  private static final String PROP_SERVER_JDBC_USERNAME = "scalardb.server.jdbc.username";
  private static final String PROP_SERVER_JDBC_PASSWORD = "scalardb.server.jdbc.password";

  private static final String DEFAULT_SERVER_JDBC_URL = "jdbc:mysql://localhost:3306/";
  private static final String DEFAULT_SERVER_JDBC_USERNAME = "root";
  private static final String DEFAULT_SERVER_JDBC_PASSWORD = "mysql";

  private static final String PROP_GRPC_CONTACT_POINTS = "scalardb.grpc.contact_points";
  private static final String PROP_GRPC_CONTACT_PORT = "scalardb.grpc.contact_port";

  private static final String DEFAULT_GRPC_CONTACT_POINTS = "localhost";
  private static final String DEFAULT_GRPC_CONTACT_PORT = "60051";

  public ServerEnv() {}

  public static ServerConfig getServerConfig() {
    return getServerConfig(null, null);
  }

  public static ServerConfig getServerConfig(
      @Nullable Isolation isolation, @Nullable SerializableStrategy serializableStrategy) {
    boolean externalServerUsed =
        Boolean.parseBoolean(
            System.getProperty(
                PROP_SERVER_EXTERNAL_SERVER_USED, DEFAULT_SERVER_EXTERNAL_SERVER_USED));
    if (externalServerUsed) {
      return null;
    }

    String contactPoints =
        System.getProperty(PROP_SERVER_CONTACT_POINTS, DEFAULT_SERVER_CONTACT_POINTS);
    String contactPort = System.getProperty(PROP_SERVER_CONTACT_PORT);
    String username = System.getProperty(PROP_SERVER_USERNAME, DEFAULT_SERVER_USERNAME);
    String password = System.getProperty(PROP_SERVER_PASSWORD, DEFAULT_SERVER_PASSWORD);
    String storage = System.getProperty(PROP_SERVER_STORAGE, DEFAULT_SERVER_STORAGE);

    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, contactPoints);
    if (contactPort != null) {
      properties.setProperty(DatabaseConfig.CONTACT_PORT, contactPort);
    }
    properties.setProperty(DatabaseConfig.USERNAME, username);
    properties.setProperty(DatabaseConfig.PASSWORD, password);
    properties.setProperty(DatabaseConfig.STORAGE, storage);
    if (isolation != null) {
      properties.setProperty(ConsensusCommitConfig.ISOLATION_LEVEL, isolation.name());
    }
    if (serializableStrategy != null) {
      properties.setProperty(
          ConsensusCommitConfig.SERIALIZABLE_STRATEGY, serializableStrategy.name());
    }
    properties.setProperty(ServerConfig.PROMETHEUS_EXPORTER_PORT, "-1");
    return new ServerConfig(properties);
  }

  public static ServerConfig getServerConfigWithJdbc() {
    boolean externalServerUsed =
        Boolean.parseBoolean(
            System.getProperty(
                PROP_SERVER_EXTERNAL_SERVER_USED, DEFAULT_SERVER_EXTERNAL_SERVER_USED));
    if (externalServerUsed) {
      return null;
    }

    String jdbcUrl = System.getProperty(PROP_SERVER_JDBC_URL, DEFAULT_SERVER_JDBC_URL);
    String username = System.getProperty(PROP_SERVER_JDBC_USERNAME, DEFAULT_SERVER_JDBC_USERNAME);
    String password = System.getProperty(PROP_SERVER_JDBC_PASSWORD, DEFAULT_SERVER_JDBC_PASSWORD);

    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, jdbcUrl);
    props.setProperty(DatabaseConfig.USERNAME, username);
    props.setProperty(DatabaseConfig.PASSWORD, password);
    props.setProperty(DatabaseConfig.STORAGE, "jdbc");
    props.setProperty(DatabaseConfig.TRANSACTION_MANAGER, "jdbc");
    props.setProperty(ServerConfig.PROMETHEUS_EXPORTER_PORT, "-1");
    return new ServerConfig(props);
  }

  public static GrpcConfig getGrpcConfig() {
    String contactPoints =
        System.getProperty(PROP_GRPC_CONTACT_POINTS, DEFAULT_GRPC_CONTACT_POINTS);
    String contactPort = System.getProperty(PROP_GRPC_CONTACT_PORT, DEFAULT_GRPC_CONTACT_PORT);

    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, contactPoints);
    properties.setProperty(DatabaseConfig.CONTACT_PORT, contactPort);
    properties.setProperty(DatabaseConfig.STORAGE, "grpc");
    return new GrpcConfig(properties);
  }
}
