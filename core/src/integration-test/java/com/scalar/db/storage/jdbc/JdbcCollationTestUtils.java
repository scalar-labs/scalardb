package com.scalar.db.storage.jdbc;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
 *   <li>MySQL 8.x — {@code utf8mb4_0900_ai_ci} (UCA 9.0.0); a version probe excludes MariaDB
 *       (reachable through {@code jdbc:mysql:} URLs) and MySQL 5.x
 *   <li>TiDB 7.4+ — {@code utf8mb4_0900_ai_ci}, also reached through {@code jdbc:mysql:} URLs; a
 *       usage probe checks the collation exists and that comparing under it is case-insensitive. A
 *       case-sensitive result means the cluster was bootstrapped with {@code
 *       new_collations_enabled_on_first_bootstrap=false}, under which every collation compares
 *       binary while {@code information_schema} keeps reporting it correctly
 *   <li>MariaDB 10.10+ — {@code utf8mb4_uca1400_ai_ci} (UCA 14.0.0); a usage probe checks the
 *       collation exists (the {@code information_schema.COLLATIONS} view lists uca1400 collations
 *       under charset-generic names, so a catalog lookup is not reliable)
 *   <li>Oracle — {@code UCA0700_ROOT_AI} on 19c and {@code UCA1210_ROOT_AI} on 21c and later,
 *       derived from the version the JDBC driver reports because 19c offers no {@code UCA1210_*}
 *       family; a usage probe checks the derived collation exists and compares case-insensitively.
 *       Applying it needs {@code MAX_STRING_SIZE=EXTENDED} on the database, which no read-only
 *       probe can observe, so a database without it reaches the suites and fails there
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

  /** Matches the TiDB release in a version string shaped like {@code 8.0.11-TiDB-v8.5.0}. */
  private static final Pattern TIDB_RELEASE_PATTERN = Pattern.compile("TiDB-v(\\d+)\\.(\\d+)");

  /** First TiDB release offering {@code utf8mb4_0900_ai_ci}. */
  private static final int TIDB_COLLATION_MAJOR = 7;

  private static final int TIDB_COLLATION_MINOR = 4;

  /** First Oracle release offering the {@code UCA1210_*} family; 19c reaches only UCA 7.0. */
  private static final int ORACLE_UCA1210_MAJOR = 21;

  /**
   * Target collation derived by the Oracle probe. Oracle is the only backend whose target is not
   * knowable from the URL, and {@link #getCollationTestTargetCollation()} is reached from paths
   * that never probe, including teardown, so an absent value throws rather than falling back to a
   * name nothing verified.
   */
  private static final AtomicReference<String> ORACLE_TARGET_COLLATION = new AtomicReference<>();

  /** Outcome of a collation-support probe. */
  enum CollationProbeResult {
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
   * verdict (e.g. TiDB below 7.4, MySQL 5.x, a PostgreSQL build without ICU) still skips even in
   * required mode. A TiDB cluster that accepts {@code utf8mb4_0900_ai_ci} yet compares binary under
   * it throws an {@link IllegalStateException} whatever {@code scalardb.jdbc.collation_test} is set
   * to, because the cause is fixed at bootstrap and cannot be repaired on the running cluster.
   *
   * @return true if the collation integration tests are supported, false otherwise
   */
  public static boolean isCollationTestSupported() {
    if (JdbcEnv.isSqlServer()) {
      return true;
    }
    if (!isMysqlUrl() && !isMariaDbUrl() && !isPostgresqlUrl() && !JdbcEnv.isOracle()) {
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
    if (JdbcEnv.isOracle()) {
      return probeOracleCollationSupport();
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
        Statement statement = connection.createStatement()) {
      String version;
      try (ResultSet resultSet = statement.executeQuery("SELECT VERSION()")) {
        if (!resultSet.next()) {
          logger.warn(
              "Skipping the collation integration tests: the MySQL version probe query "
                  + "(SELECT VERSION()) returned no rows");
          return CollationProbeResult.PROBE_FAILED;
        }
        version = resultSet.getString(1);
      }
      if (version == null) {
        logger.warn(
            "Skipping the collation integration tests: the MySQL version probe query "
                + "(SELECT VERSION()) returned a null version");
        return CollationProbeResult.PROBE_FAILED;
      }
      if (isTidbVersion(version)) {
        return probeTidbCollationSupport(statement, version);
      }
      if (!isCollationCapableMysqlVersion(version)) {
        logger.warn(
            "Skipping the collation integration tests: the server version \"{}\" does not "
                + "support the utf8mb4_0900_* collations (MariaDB and MySQL 5.x are excluded)",
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
   * Probes whether the TiDB server behind the given statement supports the collation integration
   * tests. A usage probe: evaluating a comparison under the target collation proves the server both
   * knows {@code utf8mb4_0900_ai_ci} (offered since TiDB 7.4) and applies it.
   *
   * <p>Package-private, and taking the statement as a parameter, so its verdicts can be unit-tested
   * against a mocked {@link Statement} without a live database.
   *
   * @param statement an open statement on the server to probe
   * @param version the server version string, as reported by {@code SELECT VERSION()}
   * @return the probe verdict
   * @throws IllegalStateException if the server accepts the target collation but compares binary
   *     under it
   */
  static CollationProbeResult probeTidbCollationSupport(Statement statement, String version) {
    try (ResultSet resultSet =
        statement.executeQuery("SELECT _utf8mb4'a' = _utf8mb4'A' COLLATE utf8mb4_0900_ai_ci")) {
      if (!resultSet.next()) {
        logger.warn(
            "Skipping the collation integration tests: the TiDB collation probe query returned "
                + "no rows");
        return CollationProbeResult.PROBE_FAILED;
      }
      if (!resultSet.getBoolean(1)) {
        // A hard error rather than UNSUPPORTED: new_collations_enabled_on_first_bootstrap is
        // read only at bootstrap and silently ignored afterwards, so a skip would pin a condition
        // that cannot be repaired on the running cluster
        throw new IllegalStateException(
            "The TiDB server at "
                + JdbcEnv.getJdbcUrl()
                + " compares strings case-sensitively under utf8mb4_0900_ai_ci, which means the "
                + "cluster was bootstrapped with new_collations_enabled_on_first_bootstrap=false. "
                + "Recreate the cluster with that setting enabled before running the collation "
                + "integration tests");
      }
      return CollationProbeResult.SUPPORTED;
    } catch (SQLException e) {
      // A capable version reporting the collation as unknown is a regression, not an
      // incapability: keeping it PROBE_FAILED is what makes required mode fail rather than skip
      if (isUnknownCollationError(e) && isCollationIncapableTidbVersion(version)) {
        logger.warn(
            "Skipping the collation integration tests on {}: the TiDB server version \"{}\" does "
                + "not support the utf8mb4_0900_ai_ci collation (available since TiDB 7.4)",
            JdbcEnv.getJdbcUrl(),
            version,
            e);
        return CollationProbeResult.UNSUPPORTED;
      }
      logger.warn(
          "Skipping the collation integration tests: the TiDB collation probe statement failed "
              + "on server version \"{}\"",
          version,
          e);
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

  // Oracle has no error code that definitively proves an incapability. ORA-43929 reports the
  // missing MAX_STRING_SIZE=EXTENDED prerequisite, which is a fixable property of the database
  // rather than of the server version, so every failure stays PROBE_FAILED and required mode fails
  // instead of dropping coverage. The collation name comes from a fixed per-version mapping, not
  // from any external input.
  @SuppressFBWarnings({
    "OBL_UNSATISFIED_OBLIGATION",
    "ODR_OPEN_DATABASE_RESOURCE",
    "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE"
  })
  private static CollationProbeResult probeOracleCollationSupport() {
    if (!registerDriver("oracle.jdbc.OracleDriver")) {
      return CollationProbeResult.PROBE_FAILED;
    }
    try (Connection connection =
        DriverManager.getConnection(
            JdbcEnv.getJdbcUrl(), JdbcEnv.getUsername(), JdbcEnv.getPassword())) {
      // The driver reports the version, so the probe needs no grant on v$instance, which the CI
      // test user does not hold
      String collation = oracleTargetCollation(connection.getMetaData().getDatabaseMajorVersion());
      try (Statement statement = connection.createStatement();
          ResultSet resultSet =
              statement.executeQuery(
                  "SELECT CASE WHEN 'a' = 'A' COLLATE "
                      + collation
                      + " THEN 1 ELSE 0 END FROM dual")) {
        if (!resultSet.next()) {
          logger.warn(
              "Skipping the collation integration tests: the Oracle collation probe query "
                  + "returned no rows");
          return CollationProbeResult.PROBE_FAILED;
        }
        if (resultSet.getInt(1) != 1) {
          logger.warn(
              "Skipping the collation integration tests on {}: the Oracle server compares strings "
                  + "case-sensitively under {}",
              JdbcEnv.getJdbcUrl(),
              collation);
          return CollationProbeResult.PROBE_FAILED;
        }
      }
      ORACLE_TARGET_COLLATION.set(collation);
      return CollationProbeResult.SUPPORTED;
    } catch (SQLException e) {
      logger.warn("Skipping the collation integration tests: the Oracle collation probe failed", e);
      return CollationProbeResult.PROBE_FAILED;
    }
  }

  /**
   * Returns the target collation for the given Oracle major version. 19c offers no {@code
   * UCA1210_*} family, so it takes the UCA 7.0 name; 21c and later take the UCA 12.1 one. Both are
   * accent-insensitive, which on Oracle implies case-insensitive.
   *
   * <p>Package-private and pure so it can be unit-tested without a live database.
   *
   * @param majorVersion the Oracle major version, as reported by the JDBC driver
   * @return the target collation for that version
   */
  static String oracleTargetCollation(int majorVersion) {
    return majorVersion >= ORACLE_UCA1210_MAJOR ? "UCA1210_ROOT_AI" : "UCA0700_ROOT_AI";
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

  /**
   * Returns whether the given server version string (as reported by {@code SELECT VERSION()} over a
   * {@code jdbc:mysql:} URL) supports the {@code utf8mb4_0900_*} collations used by the collation
   * integration tests. MariaDB and TiDB (both also reachable through the {@code jdbc:mysql:} URL)
   * and MySQL 5.x do not support them. TiDB is excluded here as well as dispatched ahead of this
   * check, so no evaluation order lets a TiDB server reach the suites without the case-sensitivity
   * probe; its own version floor is {@link #isCollationIncapableTidbVersion(String)}.
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
   * Returns whether the given server version string denotes TiDB, which reports a version shaped
   * like {@code 8.0.11-TiDB-v8.5.0} — the MySQL version it is wire-compatible with, then its own
   * release.
   *
   * <p>Package-private and pure so it can be unit-tested without a live database.
   *
   * @param version the server version string, may be null
   * @return true if the version denotes a TiDB server, false otherwise
   */
  static boolean isTidbVersion(String version) {
    return version != null && version.contains("TiDB");
  }

  /**
   * Returns whether the given TiDB version string denotes a release older than 7.4, the first that
   * offers {@code utf8mb4_0900_ai_ci}. A version string whose TiDB release cannot be parsed yields
   * false, so an unknown-collation error reported by it is treated as a probe failure rather than a
   * definitive incapability.
   *
   * <p>Package-private and pure so it can be unit-tested without a live database.
   *
   * @param version the server version string, may be null
   * @return true if the version denotes a TiDB release known to predate 7.4, false otherwise
   */
  static boolean isCollationIncapableTidbVersion(String version) {
    if (version == null) {
      return false;
    }
    Matcher matcher = TIDB_RELEASE_PATTERN.matcher(version);
    if (!matcher.find()) {
      return false;
    }
    int major = Integer.parseInt(matcher.group(1));
    int minor = Integer.parseInt(matcher.group(2));
    return major < TIDB_COLLATION_MAJOR
        || (major == TIDB_COLLATION_MAJOR && minor < TIDB_COLLATION_MINOR);
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
    if (JdbcEnv.isOracle()) {
      String collation = ORACLE_TARGET_COLLATION.get();
      if (collation == null) {
        throw new IllegalStateException(
            "The Oracle target collation is unavailable because the collation-support probe has "
                + "not reached a supported verdict for "
                + JdbcEnv.getJdbcUrl());
      }
      return collation;
    }
    throw new IllegalStateException(
        "The collation integration tests are not supported for the JDBC URL: "
            + JdbcEnv.getJdbcUrl());
  }
}
