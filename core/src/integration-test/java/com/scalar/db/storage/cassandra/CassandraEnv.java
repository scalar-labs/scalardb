package com.scalar.db.storage.cassandra;

import com.scalar.db.config.DatabaseConfig;
import java.util.Properties;

public final class CassandraEnv {
  private static final String PROP_CASSANDRA_CONTACT_POINTS = "scalardb.cassandra.contact_points";
  private static final String PROP_CASSANDRA_USERNAME = "scalardb.cassandra.username";
  private static final String PROP_CASSANDRA_PASSWORD = "scalardb.cassandra.password";
  private static final String PROP_CASSANDRA_NORMAL_USERNAME = "scalardb.cassandra.normal_username";
  private static final String PROP_CASSANDRA_NORMAL_PASSWORD = "scalardb.cassandra.normal_password";

  private static final String DEFAULT_CASSANDRA_CONTACT_POINTS = "localhost";
  private static final String DEFAULT_CASSANDRA_USERNAME = "cassandra";
  private static final String DEFAULT_CASSANDRA_PASSWORD = "cassandra";
  private static final String DEFAULT_CASSANDRA_NORMAL_USERNAME = "test";
  private static final String DEFAULT_CASSANDRA_NORMAL_PASSWORD = "test";

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

  /*
   * This method is used for executing commands other than CREATE commands. In Cassandra, the
   * ROLE executing the commands has multiple permissions for the object created implicitly.
   * To know the exact permissions that should be granted to the ROLE, we avoid this behavior by
   * using a different ROLE.
   */
  public static Properties getPropertiesForNormalUser(String testName) {
    String contactPoints =
        System.getProperty(PROP_CASSANDRA_CONTACT_POINTS, DEFAULT_CASSANDRA_CONTACT_POINTS);
    String username =
        System.getProperty(PROP_CASSANDRA_NORMAL_USERNAME, DEFAULT_CASSANDRA_NORMAL_USERNAME);
    String password =
        System.getProperty(PROP_CASSANDRA_NORMAL_PASSWORD, DEFAULT_CASSANDRA_NORMAL_PASSWORD);

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
