package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import java.util.Properties;

public final class JdbcEnv {
  private static final String PROP_JDBC_URL = "scalardb.jdbc.url";
  private static final String PROP_JDBC_USERNAME = "scalardb.jdbc.username";
  private static final String PROP_JDBC_PASSWORD = "scalardb.jdbc.password";
  private static final String PROP_JDBC_NORMAL_USERNAME = "scalardb.jdbc.normal_username";
  private static final String PROP_JDBC_NORMAL_PASSWORD = "scalardb.jdbc.normal_password";

  private static final String DEFAULT_JDBC_URL = "jdbc:postgresql://localhost:5432/";
  private static final String DEFAULT_JDBC_USERNAME = "postgres";
  private static final String DEFAULT_JDBC_PASSWORD = "postgres";
  private static final String DEFAULT_JDBC_NORMAL_USERNAME = "test";
  private static final String DEFAULT_JDBC_NORMAL_PASSWORD = "test";

  private JdbcEnv() {}

  public static Properties getProperties(String testName) {
    String jdbcUrl = System.getProperty(PROP_JDBC_URL, DEFAULT_JDBC_URL);
    String username = System.getProperty(PROP_JDBC_USERNAME, DEFAULT_JDBC_USERNAME);
    String password = System.getProperty(PROP_JDBC_PASSWORD, DEFAULT_JDBC_PASSWORD);

    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, jdbcUrl);
    properties.setProperty(DatabaseConfig.USERNAME, username);
    properties.setProperty(DatabaseConfig.PASSWORD, password);
    properties.setProperty(DatabaseConfig.STORAGE, "jdbc");
    properties.setProperty(DatabaseConfig.CROSS_PARTITION_SCAN, "true");
    properties.setProperty(DatabaseConfig.CROSS_PARTITION_SCAN_FILTERING, "true");
    properties.setProperty(DatabaseConfig.CROSS_PARTITION_SCAN_ORDERING, "true");

    // Add testName as a metadata schema suffix
    properties.setProperty(
        JdbcConfig.TABLE_METADATA_SCHEMA,
        DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME + "_" + testName);

    // Metadata cache expiration time
    properties.setProperty(DatabaseConfig.METADATA_CACHE_EXPIRATION_TIME_SECS, "1");

    // Set connection pool minIdle to 0 because HikariCP creates minIdle connections at startup,
    // which may waste resources in the CI environment
    properties.setProperty(JdbcConfig.CONNECTION_POOL_MIN_IDLE, "0");
    properties.setProperty(JdbcConfig.TABLE_METADATA_CONNECTION_POOL_MIN_IDLE, "0");
    properties.setProperty(JdbcConfig.ADMIN_CONNECTION_POOL_MIN_IDLE, "0");

    return properties;
  }

  public static Properties getPropertiesForNormalUser(String testName) {
    String username = System.getProperty(PROP_JDBC_NORMAL_USERNAME, DEFAULT_JDBC_NORMAL_USERNAME);
    String password = System.getProperty(PROP_JDBC_NORMAL_PASSWORD, DEFAULT_JDBC_NORMAL_PASSWORD);

    Properties properties = getProperties(testName);
    properties.setProperty(DatabaseConfig.USERNAME, username);
    properties.setProperty(DatabaseConfig.PASSWORD, password);

    return properties;
  }

  public static boolean isOracle() {
    return System.getProperty(PROP_JDBC_URL, DEFAULT_JDBC_URL).startsWith("jdbc:oracle:");
  }

  public static boolean isSqlServer() {
    return System.getProperty(PROP_JDBC_URL, DEFAULT_JDBC_URL).startsWith("jdbc:sqlserver:");
  }

  public static boolean isSqlite() {
    Properties properties = new Properties();
    properties.setProperty(
        DatabaseConfig.CONTACT_POINTS, System.getProperty(PROP_JDBC_URL, DEFAULT_JDBC_URL));
    properties.setProperty(DatabaseConfig.STORAGE, "jdbc");
    return JdbcUtils.isSqlite(new JdbcConfig(new DatabaseConfig(properties)));
  }

  public static boolean isDb2() {
    return System.getProperty(PROP_JDBC_URL, DEFAULT_JDBC_URL).startsWith("jdbc:db2:");
  }
}
