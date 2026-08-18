package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JdbcEnv {
  private static final Logger logger = LoggerFactory.getLogger(JdbcEnv.class);

  private static final String PROP_JDBC_URL = "scalardb.jdbc.url";
  private static final String PROP_JDBC_COLLATION_TEST = "scalardb.jdbc.collation_test";
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

  /** Outcome of the MySQL collation-support probe. */
  private enum CollationProbeResult {
    /** The server supports the collations used by the collation integration tests. */
    SUPPORTED,
    /** The server definitively lacks the required collations (TiDB, MariaDB, MySQL 5.x). */
    UNSUPPORTED,
    /** The probe could not reach a verdict (e.g. connection failure); not cached, re-probed. */
    PROBE_FAILED
  }

  /**
   * Caches only definitive verdicts (SUPPORTED/UNSUPPORTED) of the MySQL version probe so that a
   * transient probe failure is not pinned for the whole JVM: on PROBE_FAILED the cache stays empty
   * and the next {@code EnabledIf} evaluation re-probes.
   */
  private static final AtomicReference<CollationProbeResult> MYSQL_COLLATION_SUPPORT_PROBE =
      new AtomicReference<>();

  private static boolean isMysql() {
    return System.getProperty(PROP_JDBC_URL, DEFAULT_JDBC_URL).startsWith("jdbc:mysql:");
  }

  /**
   * Returns whether the collation integration tests are supported for the configured JDBC backend.
   * Designed for the JUnit 5 {@code EnabledIf} annotation with the condition {@code
   * "com.scalar.db.storage.jdbc.JdbcEnv#isCollationTestSupported"}.
   *
   * <p>SQL Server is supported. MySQL is supported only if the server is real MySQL 8.x or later:
   * TiDB and MariaDB (which are reachable through the {@code jdbc:mysql:} URL) and MySQL 5.x (which
   * has no {@code utf8mb4_0900_*} collations) are excluded by probing the server version. If the
   * probe itself fails (e.g. connection failure), a warning is logged, the failure is not cached
   * (the next evaluation re-probes), and the suite is skipped — unless the system property {@code
   * scalardb.jdbc.collation_test} is set to {@code required}, in which case an {@link
   * IllegalStateException} is thrown so the skip cannot go unnoticed. A definitive UNSUPPORTED
   * verdict (TiDB/MariaDB/5.x) still skips even in required mode.
   *
   * @return true if the collation integration tests are supported, false otherwise
   */
  public static boolean isCollationTestSupported() {
    if (isSqlServer()) {
      return true;
    }
    if (isMysql()) {
      CollationProbeResult result = MYSQL_COLLATION_SUPPORT_PROBE.get();
      if (result == null) {
        result = probeMysqlCollationSupport();
        if (result != CollationProbeResult.PROBE_FAILED) {
          // Cache only definitive verdicts; a transient failure must not disable the suites for
          // the whole JVM
          MYSQL_COLLATION_SUPPORT_PROBE.compareAndSet(null, result);
          result = MYSQL_COLLATION_SUPPORT_PROBE.get();
        }
      }
      if (result == CollationProbeResult.PROBE_FAILED
          && "required".equals(System.getProperty(PROP_JDBC_COLLATION_TEST))) {
        throw new IllegalStateException(
            "The MySQL collation-support probe failed but the collation integration tests are "
                + "required ("
                + PROP_JDBC_COLLATION_TEST
                + "=required). See the preceding warning log for the probe failure cause");
      }
      return result == CollationProbeResult.SUPPORTED;
    }
    return false;
  }

  // The Statement and ResultSet are closed by try-with-resources; SpotBugs' obligation analysis
  // cannot see through the compiler-generated close paths and reports a false positive.
  @SuppressFBWarnings({"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"})
  private static CollationProbeResult probeMysqlCollationSupport() {
    String jdbcUrl = System.getProperty(PROP_JDBC_URL, DEFAULT_JDBC_URL);
    String username = System.getProperty(PROP_JDBC_USERNAME, DEFAULT_JDBC_USERNAME);
    String password = System.getProperty(PROP_JDBC_PASSWORD, DEFAULT_JDBC_PASSWORD);
    // ScalarDB serves jdbc:mysql URLs through the MariaDB driver, which is not visible to
    // DriverManager's service discovery in Gradle test workers and rejects the mysql scheme
    // unless permitMysqlScheme is set (see JdbcUtils and RdbEngineMysql for the same handling).
    try {
      Class.forName("org.mariadb.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      logger.warn(
          "Skipping the collation integration tests: the MariaDB driver class used for the MySQL "
              + "version probe was not found",
          e);
      return CollationProbeResult.PROBE_FAILED;
    }
    if (!jdbcUrl.contains("permitMysqlScheme")) {
      jdbcUrl = jdbcUrl + (jdbcUrl.contains("?") ? "&" : "?") + "permitMysqlScheme=true";
    }
    try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT VERSION()")) {
      if (!resultSet.next()) {
        logger.warn(
            "Skipping the collation integration tests: the MySQL version probe query "
                + "(SELECT VERSION()) returned no rows");
        return CollationProbeResult.PROBE_FAILED;
      }
      String version = resultSet.getString(1);
      if (version == null) {
        logger.warn(
            "Skipping the collation integration tests: the MySQL version probe query "
                + "(SELECT VERSION()) returned a null version");
        return CollationProbeResult.PROBE_FAILED;
      }
      if (!isCollationCapableMysqlVersion(version)) {
        logger.info(
            "Skipping the collation integration tests: the server version \"{}\" does not "
                + "support the utf8mb4_0900_* collations (TiDB, MariaDB, and MySQL 5.x are "
                + "excluded)",
            version);
        return CollationProbeResult.UNSUPPORTED;
      }
      return CollationProbeResult.SUPPORTED;
    } catch (SQLException e) {
      logger.warn(
          "Skipping the collation integration tests: the MySQL version probe connection failed", e);
      return CollationProbeResult.PROBE_FAILED;
    }
  }

  /**
   * Returns whether the given server version string (as reported by {@code SELECT VERSION()} over a
   * {@code jdbc:mysql:} URL) supports the {@code utf8mb4_0900_*} collations used by the collation
   * integration tests. TiDB and MariaDB (both reachable through the {@code jdbc:mysql:} URL) and
   * MySQL 5.x do not support them.
   *
   * <p>Package-private and pure so it can be unit-tested without a live database.
   *
   * @param version the server version string, may be null
   * @return true if the version denotes a collation-capable MySQL server, false otherwise
   */
  static boolean isCollationCapableMysqlVersion(String version) {
    if (version == null) {
      return false;
    }
    return !version.contains("TiDB") && !version.contains("MariaDB") && !version.startsWith("5.");
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
