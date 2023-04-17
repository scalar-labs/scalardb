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

    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, jdbcUrl);
    props.setProperty(DatabaseConfig.USERNAME, username);
    props.setProperty(DatabaseConfig.PASSWORD, password);
    props.setProperty(DatabaseConfig.STORAGE, "jdbc");

    // Add testName as a metadata schema suffix
    props.setProperty(JdbcConfig.TABLE_METADATA_SCHEMA, JdbcAdmin.METADATA_SCHEMA + "_" + testName);

    return props;
  }

  public static RdbEngine getRdbEngineFromProperty() {
    String jdbcUrl = System.getProperty(PROP_JDBC_URL, DEFAULT_JDBC_URL);
    if (jdbcUrl.startsWith("jdbc:mysql:")) {
      return RdbEngine.MYSQL;
    } else if (jdbcUrl.startsWith("jdbc:postgresql:")) {
      return RdbEngine.POSTGRESQL;
    } else if (jdbcUrl.startsWith("jdbc:oracle:")) {
      return RdbEngine.ORACLE;
    } else if (jdbcUrl.startsWith("jdbc:sqlserver:")) {
      return RdbEngine.SQL_SERVER;
    } else if (jdbcUrl.startsWith("jdbc:sqlite:")) {
      return RdbEngine.SQLITE;
    } else {
      throw new IllegalArgumentException("the rdb engine is not supported: " + jdbcUrl);
    }
  }
}
