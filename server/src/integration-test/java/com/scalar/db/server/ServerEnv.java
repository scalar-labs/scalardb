package com.scalar.db.server;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.jdbc.JdbcAdmin;
import com.scalar.db.storage.jdbc.JdbcConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.Isolation;
import com.scalar.db.transaction.consensuscommit.SerializableStrategy;
import java.util.Properties;
import javax.annotation.Nullable;

public final class ServerEnv {

  private static final String PROP_SERVER_EXTERNAL_SERVER_USED =
      "scalardb.server.external_server_used";
  private static final String DEFAULT_SERVER_EXTERNAL_SERVER_USED = "false";

  private static final String PROP_SERVER_JDBC_CONTACT_POINTS =
      "scalardb.server.jdbc.contact_points";
  private static final String PROP_SERVER_JDBC_USERNAME = "scalardb.server.jdbc.username";
  private static final String PROP_SERVER_JDBC_PASSWORD = "scalardb.server.jdbc.password";

  private static final String DEFAULT_SERVER_JDBC_CONTACT_POINTS = "jdbc:mysql://localhost:3306/";
  private static final String DEFAULT_SERVER_JDBC_USERNAME = "root";
  private static final String DEFAULT_SERVER_JDBC_PASSWORD = "mysql";

  private static final String PROP_GRPC_CONTACT_POINTS = "scalardb.grpc.contact_points";
  private static final String PROP_GRPC_CONTACT_PORT = "scalardb.grpc.contact_port";

  private static final String DEFAULT_GRPC_CONTACT_POINTS = "localhost";
  private static final String DEFAULT_GRPC_CONTACT_PORT = "60051";

  public ServerEnv() {}

  public static Properties getServerProperties(String testName) {
    return getServerProperties(testName, null, null);
  }

  public static Properties getServerProperties(
      String testName,
      @Nullable Isolation isolation,
      @Nullable SerializableStrategy serializableStrategy) {
    boolean externalServerUsed =
        Boolean.parseBoolean(
            System.getProperty(
                PROP_SERVER_EXTERNAL_SERVER_USED, DEFAULT_SERVER_EXTERNAL_SERVER_USED));
    if (externalServerUsed) {
      return null;
    }

    String jdbcContactPoints =
        System.getProperty(PROP_SERVER_JDBC_CONTACT_POINTS, DEFAULT_SERVER_JDBC_CONTACT_POINTS);
    String jdbcUsername =
        System.getProperty(PROP_SERVER_JDBC_USERNAME, DEFAULT_SERVER_JDBC_USERNAME);
    String jdbcPassword =
        System.getProperty(PROP_SERVER_JDBC_PASSWORD, DEFAULT_SERVER_JDBC_PASSWORD);

    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, jdbcContactPoints);
    properties.setProperty(DatabaseConfig.USERNAME, jdbcUsername);
    properties.setProperty(DatabaseConfig.PASSWORD, jdbcPassword);
    properties.setProperty(DatabaseConfig.STORAGE, "jdbc"); // use JDBC storage
    if (isolation != null) {
      properties.setProperty(ConsensusCommitConfig.ISOLATION_LEVEL, isolation.name());
    }
    if (serializableStrategy != null) {
      properties.setProperty(
          ConsensusCommitConfig.SERIALIZABLE_STRATEGY, serializableStrategy.name());
    }
    properties.setProperty(ServerConfig.PROMETHEUS_EXPORTER_PORT, "-1");

    // Add testName as a metadata schema suffix
    properties.setProperty(
        JdbcConfig.TABLE_METADATA_SCHEMA, JdbcAdmin.METADATA_SCHEMA + "_" + testName);

    return properties;
  }

  public static Properties getProperties(@SuppressWarnings("unused") String testName) {
    String contactPoints =
        System.getProperty(PROP_GRPC_CONTACT_POINTS, DEFAULT_GRPC_CONTACT_POINTS);
    String contactPort = System.getProperty(PROP_GRPC_CONTACT_PORT, DEFAULT_GRPC_CONTACT_PORT);

    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, contactPoints);
    properties.setProperty(DatabaseConfig.CONTACT_PORT, contactPort);
    properties.setProperty(DatabaseConfig.STORAGE, "grpc");
    properties.setProperty(DatabaseConfig.TRANSACTION_MANAGER, "grpc");
    return properties;
  }
}
