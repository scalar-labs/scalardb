package com.scalar.db.storage.cassandra;

import com.scalar.db.config.DatabaseConfig;
import java.util.Properties;

public final class CassandraEnv {
  private static final String PROP_CASSANDRA_CONTACT_POINTS = "scalardb.cassandra.contact_points";
  private static final String PROP_CASSANDRA_USERNAME = "scalardb.cassandra.username";
  private static final String PROP_CASSANDRA_PASSWORD = "scalardb.cassandra.password";

  private static final String DEFAULT_CASSANDRA_CONTACT_POINTS = "localhost";
  private static final String DEFAULT_CASSANDRA_USERNAME = "cassandra";
  private static final String DEFAULT_CASSANDRA_PASSWORD = "cassandra";

  private CassandraEnv() {}

  public static Properties getProperties(String testName) {
    String contactPoints =
        System.getProperty(PROP_CASSANDRA_CONTACT_POINTS, DEFAULT_CASSANDRA_CONTACT_POINTS);
    String username = System.getProperty(PROP_CASSANDRA_USERNAME, DEFAULT_CASSANDRA_USERNAME);
    String password = System.getProperty(PROP_CASSANDRA_PASSWORD, DEFAULT_CASSANDRA_PASSWORD);

    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, contactPoints);
    properties.setProperty(DatabaseConfig.USERNAME, username);
    properties.setProperty(DatabaseConfig.PASSWORD, password);
    properties.setProperty(DatabaseConfig.CROSS_PARTITION_SCAN, "true");
    properties.setProperty(DatabaseConfig.CROSS_PARTITION_SCAN_FILTERING, "true");
    properties.setProperty(DatabaseConfig.CROSS_PARTITION_SCAN_ORDERING, "false");

    // Add testName as a metadata schema suffix
    properties.setProperty(
        DatabaseConfig.SYSTEM_NAMESPACE_NAME,
        DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME + "_" + testName);

    return properties;
  }
}
