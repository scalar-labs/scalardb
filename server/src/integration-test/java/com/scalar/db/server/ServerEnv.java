package com.scalar.db.server;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.jdbc.JdbcAdmin;
import com.scalar.db.storage.jdbc.JdbcConfig;
import java.util.Properties;

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

  private static final String PROP_GRPC_CONTACT_POINTS_FOR_SERVER1 =
      "scalardb.server.server1.contact_points";
  private static final String PROP_GRPC_CONTACT_PORT_FOR_SERVER1 =
      "scalardb.server.server1.contact_port";
  private static final String PROP_GRPC_CONTACT_POINTS_FOR_SERVER2 =
      "scalardb.server.server2.contact_points";
  private static final String PROP_GRPC_CONTACT_PORT_FOR_SERVER2 =
      "scalardb.server.server2.contact_port";

  private static final String DEFAULT_GRPC_CONTACT_POINTS = "localhost";
  private static final String DEFAULT_GRPC_CONTACT_PORT_SERVER1 = "60051";
  private static final String DEFAULT_GRPC_CONTACT_PORT_SERVER2 = "60052";

  public ServerEnv() {}

  public static Properties getServer1Properties(String testName) {
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
    properties.setProperty(DatabaseConfig.CROSS_PARTITION_SCAN, "true");
    properties.setProperty(DatabaseConfig.CROSS_PARTITION_SCAN_FILTERING, "true");
    properties.setProperty(DatabaseConfig.CROSS_PARTITION_SCAN_ORDERING, "true");

    properties.setProperty(ServerConfig.PORT, DEFAULT_GRPC_CONTACT_PORT_SERVER1);
    properties.setProperty(ServerConfig.PROMETHEUS_EXPORTER_PORT, "-1");

    // Add testName as a metadata schema suffix
    properties.setProperty(JdbcConfig.METADATA_SCHEMA, JdbcAdmin.METADATA_SCHEMA + "_" + testName);

    return properties;
  }

  public static Properties getServer2Properties(String testName) {
    Properties properties = getServer1Properties(testName);
    if (properties == null) {
      return null;
    }

    properties.setProperty(ServerConfig.PORT, DEFAULT_GRPC_CONTACT_PORT_SERVER2);
    return properties;
  }

  public static Properties getClient1Properties(@SuppressWarnings("unused") String testName) {
    String contactPoints =
        System.getProperty(PROP_GRPC_CONTACT_POINTS_FOR_SERVER1, DEFAULT_GRPC_CONTACT_POINTS);
    String contactPort =
        System.getProperty(PROP_GRPC_CONTACT_PORT_FOR_SERVER1, DEFAULT_GRPC_CONTACT_PORT_SERVER1);

    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, contactPoints);
    properties.setProperty(DatabaseConfig.CONTACT_PORT, contactPort);
    properties.setProperty(DatabaseConfig.STORAGE, "grpc");
    properties.setProperty(DatabaseConfig.TRANSACTION_MANAGER, "grpc");
    return properties;
  }

  public static Properties getClient2Properties(@SuppressWarnings("unused") String testName) {
    String contactPoints =
        System.getProperty(PROP_GRPC_CONTACT_POINTS_FOR_SERVER2, DEFAULT_GRPC_CONTACT_POINTS);
    String contactPort =
        System.getProperty(PROP_GRPC_CONTACT_PORT_FOR_SERVER2, DEFAULT_GRPC_CONTACT_PORT_SERVER2);

    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, contactPoints);
    properties.setProperty(DatabaseConfig.CONTACT_PORT, contactPort);
    properties.setProperty(DatabaseConfig.STORAGE, "grpc");
    properties.setProperty(DatabaseConfig.TRANSACTION_MANAGER, "grpc");
    return properties;
  }
}
