package com.scalar.db.storage.jdbc;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Capability gate and per-engine target collations for the collation integration tests. Lives apart
 * from {@link JdbcEnv} so that class keeps its zero-I/O, pure-string-predicate shape: the probes
 * here open a short-lived JDBC connection to the configured backend.
 *
 * <p>Supported backends and targets:
 *
 * <ul>
 *   <li>SQL Server — {@code Latin1_General_100_CI_AI} (practically compatible with ICU {@code
 *       PRIMARY} for basic-Latin data; no probe needed, every supported SQL Server has it)
 *   <li>MySQL 8.x — {@code utf8mb4_0900_ai_ci} (UCA 9.0.0); a version probe excludes TiDB and
 *       MariaDB (both reachable through {@code jdbc:mysql:} URLs) and MySQL 5.x
 *   <li>MariaDB 10.10+ — {@code utf8mb4_uca1400_ai_ci} (UCA 14.0.0); a usage probe checks the
 *       collation exists (the {@code information_schema.COLLATIONS} view lists uca1400 collations
 *       under charset-generic names, so a catalog lookup is not reliable)
 *   <li>PostgreSQL (and AlloyDB) — a nondeterministic ICU collation at primary strength that the
 *       {@code alterTableCollation} hook creates in the test namespace; a probe checks the server
 *       was built with ICU ({@code pg_collation} has ICU-provider rows)
 * </ul>
 */
public final class JdbcCollationTestUtils {
  private static final Logger logger = LoggerFactory.getLogger(JdbcCollationTestUtils.class);

  private static final String PROP_JDBC_COLLATION_TEST = "scalardb.jdbc.collation_test";

  /**
   * Name of the nondeterministic ICU collation the collation tests create on PostgreSQL, at primary
   * strength (case- and accent-insensitive), matching the suites' {@code
   * scalar.db.collation.icu.strength=PRIMARY} configuration.
   */
  static final String POSTGRESQL_TEST_COLLATION = "scalardb_collation_test_ci";

  /** Outcome of a collation-support probe. */
  private enum CollationProbeResult {
    /** The server supports the collations used by the collation integration tests. */
    SUPPORTED,
    /** The server definitively lacks the required collations. */
    UNSUPPORTED,
    /** The probe could not reach a verdict (e.g. connection failure); not cached, re-probed. */
    PROBE_FAILED
  }

  /**
   * Caches only definitive verdicts (SUPPORTED/UNSUPPORTED) so that a transient probe failure is
   * not pinned for the whole JVM: on PROBE_FAILED the cache stays empty and the next {@code
   * EnabledIf} evaluation re-probes.
   */
  private static final AtomicReference<CollationProbeResult> COLLATION_SUPPORT_PROBE =
      new AtomicReference<>();

  private JdbcCollationTestUtils() {}

  private static boolean isMysqlUrl() {
    return JdbcEnv.getJdbcUrl().startsWith("jdbc:mysql:");
  }

  private static boolean isMariaDbUrl() {
    return JdbcEnv.getJdbcUrl().startsWith("jdbc:mariadb:");
  }

  private static boolean isPostgresqlUrl() {
    return JdbcEnv.getJdbcUrl().startsWith("jdbc:postgresql:");
  }

  /**
   * Returns whether the collation integration tests are supported for the configured JDBC backend.
   * Designed for the JUnit 5 {@code EnabledIf} annotation with the condition {@code
   * "com.scalar.db.storage.jdbc.JdbcCollationTestUtils#isCollationTestSupported"}.
   *
   * <p>If a probe itself fails (e.g. connection failure), a warning is logged, the failure is not
   * cached (the next evaluation re-probes), and the suite is skipped — unless the system property
   * {@code scalardb.jdbc.collation_test} is set to {@code required}, in which case an {@link
   * IllegalStateException} is thrown so the skip cannot go unnoticed. A definitive UNSUPPORTED
   * verdict (e.g. TiDB, MySQL 5.x, a PostgreSQL build without ICU) still skips even in required
   * mode.
   *
   * @return true if the collation integration tests are supported, false otherwise
   */
  public static boolean isCollationTestSupported() {
    if (JdbcEnv.isSqlServer()) {
      return true;
    }
    if (!isMysqlUrl() && !isMariaDbUrl() && !isPostgresqlUrl()) {
      return false;
    }
    CollationProbeResult result = COLLATION_SUPPORT_PROBE.get();
    if (result == null) {
      result = probeCollationSupport();
      if (result != CollationProbeResult.PROBE_FAILED) {
        // Cache only definitive verdicts; a transient failure must not disable the suites for
        // the whole JVM
        COLLATION_SUPPORT_PROBE.compareAndSet(null, result);
        result = COLLATION_SUPPORT_PROBE.get();
      }
    }
    if (result == CollationProbeResult.PROBE_FAILED
        && "required".equals(System.getProperty(PROP_JDBC_COLLATION_TEST))) {
      throw new IllegalStateException(
          "The collation-support probe failed but the collation integration tests are required ("
              + PROP_JDBC_COLLATION_TEST
              + "=required). See the preceding warning log for the probe failure cause");
    }
    return result == CollationProbeResult.SUPPORTED;
  }

  private static CollationProbeResult probeCollationSupport() {
    if (isMysqlUrl()) {
      return probeMysqlCollationSupport();
    }
    if (isMariaDbUrl()) {
      return probeMariaDbCollationSupport();
    }
    return probePostgresqlCollationSupport();
  }

  // The Statement and ResultSet are closed by try-with-resources; SpotBugs' obligation analysis
  // cannot see through the compiler-generated close paths and reports a false positive.
  @SuppressFBWarnings({"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"})
  private static CollationProbeResult probeMysqlCollationSupport() {
    String jdbcUrl = JdbcEnv.getJdbcUrl();
    // ScalarDB serves jdbc:mysql URLs through the MariaDB driver, which is not visible to
    // DriverManager's service discovery in Gradle test workers and rejects the mysql scheme
    // unless permitMysqlScheme is set (see JdbcUtils and RdbEngineMysql for the same handling).
    if (!registerDriver("org.mariadb.jdbc.Driver")) {
      return CollationProbeResult.PROBE_FAILED;
    }
    if (!jdbcUrl.contains("permitMysqlScheme")) {
      jdbcUrl = jdbcUrl + (jdbcUrl.contains("?") ? "&" : "?") + "permitMysqlScheme=true";
    }
    try (Connection connection =
            DriverManager.getConnection(jdbcUrl, JdbcEnv.getUsername(), JdbcEnv.getPassword());
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
        logger.warn(
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

  // A usage probe: evaluating an expression under the target collation proves the server supports
  // it (uca1400 collations exist since MariaDB 10.10). A statement error on a live connection is a
  // definitive UNSUPPORTED; only a connection failure is PROBE_FAILED.
  @SuppressFBWarnings({"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"})
  private static CollationProbeResult probeMariaDbCollationSupport() {
    if (!registerDriver("org.mariadb.jdbc.Driver")) {
      return CollationProbeResult.PROBE_FAILED;
    }
    try (Connection connection =
        DriverManager.getConnection(
            JdbcEnv.getJdbcUrl(), JdbcEnv.getUsername(), JdbcEnv.getPassword())) {
      try (Statement statement = connection.createStatement();
          ResultSet resultSet =
              statement.executeQuery(
                  "SELECT _utf8mb4'a' = _utf8mb4'A' COLLATE utf8mb4_uca1400_ai_ci")) {
        resultSet.next();
        return CollationProbeResult.SUPPORTED;
      } catch (SQLException e) {
        // Only an unknown-collation error is a definitive incapability; any other statement-stage
        // error (broken query, connection killed mid-statement, permissions) must stay
        // PROBE_FAILED so it is neither cached for the JVM nor exempt from required mode
        if (isUnknownCollationError(e)) {
          logger.warn(
              "Skipping the collation integration tests on {}: the MariaDB server does not "
                  + "support the utf8mb4_uca1400_ai_ci collation (available since MariaDB 10.10)",
              JdbcEnv.getJdbcUrl(),
              e);
          return CollationProbeResult.UNSUPPORTED;
        }
        logger.warn(
            "Skipping the collation integration tests: the MariaDB collation probe statement "
                + "failed",
            e);
        return CollationProbeResult.PROBE_FAILED;
      }
    } catch (SQLException e) {
      logger.warn(
          "Skipping the collation integration tests: the MariaDB collation probe connection "
              + "failed",
          e);
      return CollationProbeResult.PROBE_FAILED;
    }
  }

  // A PostgreSQL server built without ICU has no ICU-provider rows in pg_collation, and the
  // nondeterministic ICU collation the tests rely on cannot be created there.
  @SuppressFBWarnings({"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"})
  private static CollationProbeResult probePostgresqlCollationSupport() {
    if (!registerDriver("org.postgresql.Driver")) {
      return CollationProbeResult.PROBE_FAILED;
    }
    try (Connection connection =
            DriverManager.getConnection(
                JdbcEnv.getJdbcUrl(), JdbcEnv.getUsername(), JdbcEnv.getPassword());
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery("SELECT count(*) FROM pg_collation WHERE collprovider = 'i'")) {
      if (resultSet.next() && resultSet.getLong(1) > 0) {
        return CollationProbeResult.SUPPORTED;
      }
      logger.warn(
          "Skipping the collation integration tests on {}: the PostgreSQL server was built "
              + "without ICU (pg_collation has no ICU-provider collations)",
          JdbcEnv.getJdbcUrl());
      return CollationProbeResult.UNSUPPORTED;
    } catch (SQLException e) {
      logger.warn(
          "Skipping the collation integration tests: the PostgreSQL collation probe connection "
              + "failed",
          e);
      return CollationProbeResult.PROBE_FAILED;
    }
  }

  // DriverManager's ServiceLoader-based auto-discovery does not see the test runtime classpath in
  // Gradle test workers, so drivers are registered explicitly
  private static boolean registerDriver(String driverClassName) {
    try {
      Class.forName(driverClassName);
      return true;
    } catch (ClassNotFoundException e) {
      logger.warn(
          "Skipping the collation integration tests: the JDBC driver class {} used for the "
              + "collation probe was not found",
          driverClassName,
          e);
      return false;
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
  /**
   * Returns whether the given exception denotes MySQL/MariaDB error 1273 (ER_UNKNOWN_COLLATION) —
   * the one statement-stage error that proves a definitive collation incapability rather than a
   * transient or environmental failure.
   *
   * <p>Package-private and pure so it can be unit-tested without a live database.
   *
   * @param e the exception from the probe statement
   * @return true if the error is ER_UNKNOWN_COLLATION
   */
  static boolean isUnknownCollationError(SQLException e) {
    return e.getErrorCode() == 1273;
  }

  static boolean isCollationCapableMysqlVersion(String version) {
    if (version == null) {
      return false;
    }
    return !version.contains("TiDB") && !version.contains("MariaDB") && !version.startsWith("5.");
  }

  /**
   * Returns whether the configured JDBC backend applies the collation integration tests' target
   * collation at the namespace level (MySQL and MariaDB) and supports the collation tests. Designed
   * for the JUnit 5 {@code EnabledIf} annotation on tests that exercise the namespace-level
   * mechanism itself.
   *
   * @return true if the backend applies the target collation at the namespace level and supports
   *     the collation integration tests, false otherwise
   */
  public static boolean isNamespaceCollationTestSupported() {
    return (isMysqlUrl() || isMariaDbUrl()) && isCollationTestSupported();
  }

  /**
   * Returns the per-engine target collation used by the collation integration tests, to be passed
   * to the {@code AdminTestUtils} collation hooks ({@code alterNamespaceCollation} and {@code
   * alterTableCollation}). On PostgreSQL the returned name denotes the nondeterministic ICU
   * collation the hook creates in the test namespace rather than a built-in collation.
   *
   * @return the target collation for the configured JDBC backend
   * @throws IllegalStateException if the configured JDBC backend does not support the collation
   *     integration tests
   */
  public static String getCollationTestTargetCollation() {
    if (isMysqlUrl()) {
      return "utf8mb4_0900_ai_ci";
    }
    if (isMariaDbUrl()) {
      return "utf8mb4_uca1400_ai_ci";
    }
    if (isPostgresqlUrl()) {
      return POSTGRESQL_TEST_COLLATION;
    }
    if (JdbcEnv.isSqlServer()) {
      return "Latin1_General_100_CI_AI";
    }
    throw new IllegalStateException(
        "The collation integration tests are not supported for the JDBC URL: "
            + JdbcEnv.getJdbcUrl());
  }
}
