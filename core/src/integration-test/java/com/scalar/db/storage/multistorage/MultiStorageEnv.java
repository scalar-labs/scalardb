package com.scalar.db.storage.multistorage;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.jdbc.JdbcAdmin;
import com.scalar.db.storage.jdbc.JdbcConfig;
import java.util.Properties;

public final class MultiStorageEnv {

  private static final String PROP_CASSANDRA_CONTACT_POINTS = "scalardb.cassandra.contact_points";
  private static final String PROP_CASSANDRA_CONTACT_PORT = "scalardb.cassandra.contact_port";
  private static final String PROP_CASSANDRA_USERNAME = "scalardb.cassandra.username";
  private static final String PROP_CASSANDRA_PASSWORD = "scalardb.cassandra.password";

  private static final String PROP_JDBC_CONTACT_POINTS = "scalardb.jdbc.contact_points";
  private static final String PROP_JDBC_USERNAME = "scalardb.jdbc.username";
  private static final String PROP_JDBC_PASSWORD = "scalardb.jdbc.password";

  private static final String DEFAULT_CASSANDRA_CONTACT_POINT = "localhost";
  private static final String DEFAULT_CASSANDRA_USERNAME = "cassandra";
  private static final String DEFAULT_CASSANDRA_PASSWORD = "cassandra";

  private static final String DEFAULT_JDBC_CONTACT_POINT = "jdbc:mysql://localhost:3306/";
  private static final String DEFAULT_JDBC_USERNAME = "root";
  private static final String DEFAULT_JDBC_PASSWORD = "mysql";

  private MultiStorageEnv() {}

  public static Properties getPropertiesForCassandra(@SuppressWarnings("unused") String testName) {
    String contactPoints =
        System.getProperty(PROP_CASSANDRA_CONTACT_POINTS, DEFAULT_CASSANDRA_CONTACT_POINT);
    String contactPort = System.getProperty(PROP_CASSANDRA_CONTACT_PORT);
    String username = System.getProperty(PROP_CASSANDRA_USERNAME, DEFAULT_CASSANDRA_USERNAME);
    String password = System.getProperty(PROP_CASSANDRA_PASSWORD, DEFAULT_CASSANDRA_PASSWORD);

    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, contactPoints);
    if (contactPort != null) {
      properties.setProperty(DatabaseConfig.CONTACT_PORT, contactPort);
    }
    properties.setProperty(DatabaseConfig.USERNAME, username);
    properties.setProperty(DatabaseConfig.PASSWORD, password);
    properties.setProperty(DatabaseConfig.STORAGE, "cassandra");
    return properties;
  }

  public static Properties getPropertiesForJdbc(String testName) {
    String contactPoints = System.getProperty(PROP_JDBC_CONTACT_POINTS, DEFAULT_JDBC_CONTACT_POINT);
    String username = System.getProperty(PROP_JDBC_USERNAME, DEFAULT_JDBC_USERNAME);
    String password = System.getProperty(PROP_JDBC_PASSWORD, DEFAULT_JDBC_PASSWORD);

    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, contactPoints);
    properties.setProperty(DatabaseConfig.USERNAME, username);
    properties.setProperty(DatabaseConfig.PASSWORD, password);
    properties.setProperty(DatabaseConfig.STORAGE, "jdbc");

    // Add testName as a metadata schema suffix
    properties.setProperty(JdbcConfig.METADATA_SCHEMA, JdbcAdmin.METADATA_SCHEMA + "_" + testName);

    return properties;
  }
}
