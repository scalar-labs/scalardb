package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JdbcEnv {
  private static final Logger logger = LoggerFactory.getLogger(JdbcEnv.class);

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

  private static final int SPANNER_DATABASE_COUNT = 4;
  private static final Pattern SPANNER_DATABASE_PATTERN =
      Pattern.compile("(/databases/test-db-)\\d+");

  private JdbcEnv() {}

  public static Properties getProperties(String testName) {
    String jdbcUrl = resolveJdbcUrl(System.getProperty(PROP_JDBC_URL, DEFAULT_JDBC_URL));
    String username = System.getProperty(PROP_JDBC_USERNAME, DEFAULT_JDBC_USERNAME);
    String password = System.getProperty(PROP_JDBC_PASSWORD, DEFAULT_JDBC_PASSWORD);

    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, jdbcUrl);
    if (!username.isEmpty()) {
      properties.setProperty(DatabaseConfig.USERNAME, username);
    }
    if (!password.isEmpty()) {
      properties.setProperty(DatabaseConfig.PASSWORD, password);
    }
    properties.setProperty(DatabaseConfig.STORAGE, "jdbc");
    properties.setProperty(DatabaseConfig.CROSS_PARTITION_SCAN, "true");
    properties.setProperty(DatabaseConfig.CROSS_PARTITION_SCAN_FILTERING, "true");
    properties.setProperty(DatabaseConfig.CROSS_PARTITION_SCAN_ORDERING, "true");

    // Add testName as a metadata schema suffix
    properties.setProperty(
        DatabaseConfig.SYSTEM_NAMESPACE_NAME,
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
    return System.getProperty(PROP_JDBC_URL, DEFAULT_JDBC_URL).startsWith("jdbc:sqlite:");
  }

  public static boolean isDb2() {
    return System.getProperty(PROP_JDBC_URL, DEFAULT_JDBC_URL).startsWith("jdbc:db2:");
  }

  public static boolean isSpanner() {
    return System.getProperty(PROP_JDBC_URL, DEFAULT_JDBC_URL).startsWith("jdbc:cloudspanner:");
  }

  public static boolean isSpannerEmulator() {
    return isSpanner()
        && System.getProperty(PROP_JDBC_URL, DEFAULT_JDBC_URL).contains("autoConfigEmulator");
  }

  // Spreads parallel Gradle test workers across distinct Spanner databases (test-db-1..N) by
  // rewriting the database segment of the configured Cloud Spanner JDBC URL based on the
  // org.gradle.test.worker system property that Gradle assigns to each forked test JVM.
  static String resolveJdbcUrl(String jdbcUrl) {
    if (!jdbcUrl.startsWith("jdbc:cloudspanner:")) {
      return jdbcUrl;
    }
    String workerProp = System.getProperty("org.gradle.test.worker");
    if (workerProp == null) {
      return jdbcUrl;
    }
    int workerId;
    try {
      workerId = Integer.parseInt(workerProp);
    } catch (NumberFormatException e) {
      return jdbcUrl;
    }
    int dbIndex = ((workerId - 1) % SPANNER_DATABASE_COUNT) + 1;
    Matcher matcher = SPANNER_DATABASE_PATTERN.matcher(jdbcUrl);
    if (!matcher.find()) {
      return jdbcUrl;
    }
    String resolved = matcher.replaceFirst("$1" + dbIndex);
    logger.info("Gradle test worker {} is using Spanner database test-db-{}", workerId, dbIndex);
    return resolved;
  }
}
