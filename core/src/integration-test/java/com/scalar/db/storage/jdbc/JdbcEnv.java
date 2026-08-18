package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

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

  private JdbcEnv() {}

  public static Properties getProperties(String testName) {
    String jdbcUrl = System.getProperty(PROP_JDBC_URL, DEFAULT_JDBC_URL);
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
    return System.getProperty(PROP_JDBC_URL, DEFAULT_JDBC_URL).startsWith("jdbc:cloudspanner:")
        || System.getProperty(PROP_JDBC_URL, DEFAULT_JDBC_URL).startsWith("jdbc:spanner:");
  }

  public static boolean isYugabyte() {
    return System.getProperty(PROP_JDBC_URL, DEFAULT_JDBC_URL).startsWith("jdbc:yugabytedb:");
  }

  public static boolean isSpannerEmulator() {
    return isSpanner()
        && System.getProperty(PROP_JDBC_URL, DEFAULT_JDBC_URL).contains("autoConfigEmulator");
  }

  /** Caches the result of the MySQL version probe so at most one connection is opened per JVM. */
  private static final AtomicReference<Boolean> MYSQL_COLLATION_SUPPORT_PROBE =
      new AtomicReference<>();

  private static boolean isMysql() {
    return System.getProperty(PROP_JDBC_URL, DEFAULT_JDBC_URL).startsWith("jdbc:mysql:");
  }

  /**
   * Returns whether the collation integration tests are supported for the configured JDBC backend.
   * Designed for the JUnit 5 {@code EnabledIf} annotation with the condition {@code
   * "com.scalar.db.storage.jdbc.JdbcEnv#isCollationTestSupported"}.
   *
   * <p>SQL Server is supported. MySQL is supported only if the server is real MySQL 8.x: TiDB
   * (which shares the {@code jdbc:mysql:} URL) and MySQL 5.x (which has no {@code utf8mb4_0900_*}
   * collations) are excluded by probing the server version. If the probe connection fails, this
   * method returns false so that the suite is skipped instead of erroring.
   *
   * @return true if the collation integration tests are supported, false otherwise
   */
  public static boolean isCollationTestSupported() {
    if (isSqlServer()) {
      return true;
    }
    if (isMysql()) {
      Boolean cached = MYSQL_COLLATION_SUPPORT_PROBE.get();
      if (cached == null) {
        MYSQL_COLLATION_SUPPORT_PROBE.compareAndSet(null, probeMysqlCollationSupport());
        cached = MYSQL_COLLATION_SUPPORT_PROBE.get();
      }
      return cached;
    }
    return false;
  }

  private static boolean probeMysqlCollationSupport() {
    String jdbcUrl = System.getProperty(PROP_JDBC_URL, DEFAULT_JDBC_URL);
    String username = System.getProperty(PROP_JDBC_USERNAME, DEFAULT_JDBC_USERNAME);
    String password = System.getProperty(PROP_JDBC_PASSWORD, DEFAULT_JDBC_PASSWORD);
    try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT VERSION()")) {
      if (!resultSet.next()) {
        return false;
      }
      String version = resultSet.getString(1);
      if (version == null) {
        return false;
      }
      // TiDB shares the jdbc:mysql URL, and MySQL 5.x has no utf8mb4_0900_* collations
      return !version.contains("TiDB") && !version.startsWith("5.");
    } catch (SQLException e) {
      // Return false so that the suite is skipped instead of erroring at @EnabledIf evaluation
      return false;
    }
  }

  /**
   * Returns the per-engine target collation used by the collation integration tests, to be passed
   * to {@code AdminTestUtils#alterTableCollation(String, String, String)}.
   *
   * @return the target collation for the configured JDBC backend
   * @throws IllegalStateException if the configured JDBC backend does not support the collation
   *     integration tests
   */
  public static String getCollationTestTargetCollation() {
    if (isMysql()) {
      return "utf8mb4_0900_ai_ci";
    }
    if (isSqlServer()) {
      return "Latin1_General_100_CI_AI";
    }
    throw new IllegalStateException(
        "The collation integration tests are not supported for the JDBC URL: "
            + System.getProperty(PROP_JDBC_URL, DEFAULT_JDBC_URL));
  }
}
