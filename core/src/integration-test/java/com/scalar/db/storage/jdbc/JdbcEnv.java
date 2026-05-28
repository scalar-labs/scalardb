package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

  // Matches the database segment in a Cloud Spanner JDBC URL
  // (e.g. .../databases/<name>;autoConfigEmulator=true or .../databases/<name>?autoConfigEmulator=true)
  private static final Pattern SPANNER_DATABASE_PATTERN =
      Pattern.compile("(/databases/)[^/?;]+");

  private JdbcEnv() {}

  public static Properties getProperties(String testName) {
    String jdbcUrl = System.getProperty(PROP_JDBC_URL, DEFAULT_JDBC_URL);

    // For the Spanner emulator, rewrite the JDBC URL to use a per-worker database (test-db-N) so
    // that tests running in parallel across Gradle test workers do not share the same database.
    if (isSpannerEmulator()) {
      jdbcUrl = rewriteSpannerEmulatorJdbcUrl(jdbcUrl);
    }
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

  /**
   * Rewrites the database segment of a Spanner emulator JDBC URL to {@code test-db-N}, where {@code
   * N} is the Gradle test worker ID. This gives each parallel test worker its own database.
   *
   * @param jdbcUrl the original Spanner JDBC URL
   * @return the JDBC URL with its database segment replaced by {@code test-db-N}
   */
  private static String rewriteSpannerEmulatorJdbcUrl(String jdbcUrl) {
    String workerId = System.getProperty("org.gradle.test.worker", "1");
    Matcher matcher = SPANNER_DATABASE_PATTERN.matcher(jdbcUrl);
    return matcher.replaceFirst("$1test-db-" + workerId);
  }
}
