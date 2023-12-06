package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import java.util.Properties;

public final class JdbcEnv {
  private static final String PROP_JDBC_URL = "scalardb.jdbc.url";
  private static final String PROP_JDBC_USERNAME = "scalardb.jdbc.username";
  private static final String PROP_JDBC_PASSWORD = "scalardb.jdbc.password";

  private static final String DEFAULT_JDBC_URL = "jdbc:mysql://localhost:3306/";
  private static final String DEFAULT_JDBC_USERNAME = "root";
  private static final String DEFAULT_JDBC_PASSWORD = "mysql";

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
        DatabaseConfig.SYSTEM_NAMESPACE_NAME,
        DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME + "_" + testName);

    return properties;
  }

  public static boolean isSqlite() {
    Properties properties = new Properties();
    properties.setProperty(
        DatabaseConfig.CONTACT_POINTS, System.getProperty(PROP_JDBC_URL, DEFAULT_JDBC_URL));
    properties.setProperty(DatabaseConfig.STORAGE, "jdbc");
    return JdbcUtils.isSqlite(new JdbcConfig(new DatabaseConfig(properties)));
  }
}
