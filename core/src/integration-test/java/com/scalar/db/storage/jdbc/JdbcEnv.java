package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStorageColumnValueIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JdbcEnv {
  private static final Logger logger =
      LoggerFactory.getLogger(JdbcEnv.class);
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

  // Number of Spanner databases provisioned as `test-db-1` .. `test-db-N`.
  // Each Gradle test worker is pinned to one of these databases so parallel forks
  // don't clobber each other's schema.
  private static final int SPANNER_DATABASE_COUNT = 5;

  // Worker id -> assigned Spanner database name. Populated lazily on first lookup
  // per worker. `ConcurrentHashMap` because test workers in a fork can be multi-threaded.
  private static final Map<String, String> WORKER_TO_SPANNER_DATABASE = new ConcurrentHashMap<>();

  private JdbcEnv() {}

  public static Properties getProperties(String testName) {
    String jdbcUrl = System.getProperty(PROP_JDBC_URL, DEFAULT_JDBC_URL);
    String username = System.getProperty(PROP_JDBC_USERNAME, DEFAULT_JDBC_USERNAME);
    String password = System.getProperty(PROP_JDBC_PASSWORD, DEFAULT_JDBC_PASSWORD);
    if (isSpanner() && !isSpannerEmulator()) {
      jdbcUrl = assignSpannerDatabaseForWorker(jdbcUrl);
    }
    if (isSpanner() && !isSpannerEmulator()) {
      try {

        password =
            String.join(
                "",
                Files.readAllLines(
                    Paths.get(System.getProperty("user.home"), "spanner-credentials.json"),
                    StandardCharsets.UTF_8));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
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

  /**
   * Rewrites the database segment of the given Spanner JDBC URL to pin the current Gradle test
   * worker to a specific database (`test-db-1` .. `test-db-N`), so parallel forks each work
   * against a different database. The assignment is memoized per worker id in {@link
   * #WORKER_TO_SPANNER_DATABASE} so repeated calls within the same worker return the same database.
   *
   * <p>If the process is not running under Gradle test workers (i.e., the {@code
   * org.gradle.test.worker} system property is unset), the URL is returned unchanged.
   */
  private static String assignSpannerDatabaseForWorker(String jdbcUrl) {
    String workerId = System.getProperty("org.gradle.test.worker");
    if (workerId == null) {
      return jdbcUrl;
    }
    String databaseName =
        WORKER_TO_SPANNER_DATABASE.computeIfAbsent(
            workerId,
            wid -> {
              logger.info("Assigning Spanner database for worker id: " + wid);
              int index = Integer.parseInt(wid);
              if (index < 1 || index > SPANNER_DATABASE_COUNT) {
                throw new IllegalStateException(
                    "Gradle test worker id "
                        + wid
                        + " is out of range [1, "
                        + SPANNER_DATABASE_COUNT
                        + "]. Spanner has only "
                        + SPANNER_DATABASE_COUNT
                        + " pre-provisioned databases (test-db-1..test-db-"
                        + SPANNER_DATABASE_COUNT
                        + "); reduce maxParallelForks or provision more databases.");
              }
              return "test-db-" + index;
            });
    return jdbcUrl.replaceFirst("/databases/[^;/]+", "/databases/" + databaseName);
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
}
