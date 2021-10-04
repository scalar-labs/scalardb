package com.scalar.db.storage.multistorage;

import com.scalar.db.config.DatabaseConfig;
import java.util.Properties;

public final class MultiStorageEnv {

  private static final String PROP_STORAGE1_CONTACT_POINTS = "scalardb.storage1.contact_points";
  private static final String PROP_STORAGE1_CONTACT_PORT = "scalardb.storage1.contact_port";
  private static final String PROP_STORAGE1_USERNAME = "scalardb.storage1.username";
  private static final String PROP_STORAGE1_PASSWORD = "scalardb.storage1.password";
  private static final String PROP_STORAGE1_STORAGE = "scalardb.storage1.storage";

  private static final String PROP_STORAGE2_CONTACT_POINTS = "scalardb.storage2.contact_points";
  private static final String PROP_STORAGE2_CONTACT_PORT = "scalardb.storage2.contact_port";
  private static final String PROP_STORAGE2_USERNAME = "scalardb.storage2.username";
  private static final String PROP_STORAGE2_PASSWORD = "scalardb.storage2.password";
  private static final String PROP_STORAGE2_STORAGE = "scalardb.storage2.storage";

  private static final String DEFAULT_STORAGE1_CONTACT_POINT = "localhost";
  private static final String DEFAULT_STORAGE1_USERNAME = "cassandra";
  private static final String DEFAULT_STORAGE1_PASSWORD = "cassandra";
  private static final String DEFAULT_STORAGE1_STORAGE = "cassandra";

  private static final String DEFAULT_STORAGE2_CONTACT_POINT = "jdbc:mysql://localhost:3306/";
  private static final String DEFAULT_STORAGE2_USERNAME = "root";
  private static final String DEFAULT_STORAGE2_PASSWORD = "mysql";
  private static final String DEFAULT_STORAGE2_STORAGE = "jdbc";

  private MultiStorageEnv() {}

  public static DatabaseConfig getDatabaseConfigForStorage1() {
    String contactPoints =
        System.getProperty(PROP_STORAGE1_CONTACT_POINTS, DEFAULT_STORAGE1_CONTACT_POINT);
    String contactPort = System.getProperty(PROP_STORAGE1_CONTACT_PORT);
    String username = System.getProperty(PROP_STORAGE1_USERNAME, DEFAULT_STORAGE1_USERNAME);
    String password = System.getProperty(PROP_STORAGE1_PASSWORD, DEFAULT_STORAGE1_PASSWORD);
    String storage = System.getProperty(PROP_STORAGE1_STORAGE, DEFAULT_STORAGE1_STORAGE);

    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, contactPoints);
    if (contactPort != null) {
      properties.setProperty(DatabaseConfig.CONTACT_PORT, contactPort);
    }
    properties.setProperty(DatabaseConfig.USERNAME, username);
    properties.setProperty(DatabaseConfig.PASSWORD, password);
    properties.setProperty(DatabaseConfig.STORAGE, storage);
    return new DatabaseConfig(properties);
  }

  public static DatabaseConfig getDatabaseConfigForStorage2() {
    String contactPoints =
        System.getProperty(PROP_STORAGE2_CONTACT_POINTS, DEFAULT_STORAGE2_CONTACT_POINT);
    String contactPort = System.getProperty(PROP_STORAGE2_CONTACT_PORT);
    String username = System.getProperty(PROP_STORAGE2_USERNAME, DEFAULT_STORAGE2_USERNAME);
    String password = System.getProperty(PROP_STORAGE2_PASSWORD, DEFAULT_STORAGE2_PASSWORD);
    String storage = System.getProperty(PROP_STORAGE2_STORAGE, DEFAULT_STORAGE2_STORAGE);

    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, contactPoints);
    if (contactPort != null) {
      properties.setProperty(DatabaseConfig.CONTACT_PORT, contactPort);
    }
    properties.setProperty(DatabaseConfig.USERNAME, username);
    properties.setProperty(DatabaseConfig.PASSWORD, password);
    properties.setProperty(DatabaseConfig.STORAGE, storage);
    return new DatabaseConfig(properties);
  }
}
