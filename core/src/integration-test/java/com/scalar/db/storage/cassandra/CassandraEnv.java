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

  public static Properties getProperties(@SuppressWarnings("unused") String testName) {
    String contactPoints =
        System.getProperty(PROP_CASSANDRA_CONTACT_POINTS, DEFAULT_CASSANDRA_CONTACT_POINTS);
    String username = System.getProperty(PROP_CASSANDRA_USERNAME, DEFAULT_CASSANDRA_USERNAME);
    String password = System.getProperty(PROP_CASSANDRA_PASSWORD, DEFAULT_CASSANDRA_PASSWORD);

    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, contactPoints);
    props.setProperty(DatabaseConfig.USERNAME, username);
    props.setProperty(DatabaseConfig.PASSWORD, password);
    props.setProperty(DatabaseConfig.CROSS_PARTITION_SCAN, "true");
    props.setProperty(DatabaseConfig.CROSS_PARTITION_SCAN_FILTERING, "false");
    props.setProperty(DatabaseConfig.CROSS_PARTITION_SCAN_ORDERING, "false");
    return props;
  }
}
