package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Gate and per-engine target collations for the collation integration tests. Lives apart from
 * {@link JdbcEnv} so that class keeps its zero-I/O, pure-string-predicate shape: gating a backend
 * on its version and resolving Oracle's target collation open a short-lived JDBC connection.
 *
 * <p>{@code docs/collation.md} records each backend's target collation and why it was chosen.
 */
public final class JdbcCollationTestUtils {
  /**
   * Name of the nondeterministic ICU collation the collation tests create on PostgreSQL, at primary
   * strength (case- and accent-insensitive), matching the suites' {@code
   * scalar.db.collation.icu.rules=[strength 1]} configuration.
   */
  static final String POSTGRESQL_TEST_COLLATION = "scalardb_collation_test_ci";

  /** First Oracle release offering the {@code UCA1210_*} family; 19c reaches only UCA 7.0. */
  private static final int ORACLE_UCA1210_MAJOR = 21;

  /**
   * Oracle's target collation, resolved once from the version the JDBC driver reports. Oracle is
   * the only backend whose target is not knowable from the URL.
   */
  private static String resolvedOracleTargetCollation;

  private JdbcCollationTestUtils() {}

  /**
   * Returns whether the collation integration tests are enabled for the configured JDBC backend.
   * Designed for the JUnit 5 {@code EnabledIf} annotation with the condition {@code
   * "com.scalar.db.storage.jdbc.JdbcCollationTestUtils#isCollationTestSupported"}.
   *
   * <p>The suites run on the backends that have a known target collation and a version that offers
   * it. Backends without one skip.
   *
   * @return true if the collation integration tests are enabled, false otherwise
   */
  public static boolean isCollationTestSupported() {
    if (JdbcEnv.isPostgresql() || JdbcEnv.isSqlServer() || JdbcEnv.isMariaDb()) {
      return true;
    }
    if (JdbcEnv.isOracle()) {
      // Resolve here so that every later read, teardown included, is a cache hit
      resolveOracleTargetCollation();
      return true;
    }
    if (JdbcEnv.isMysql()) {
      // TiDB speaks the MySQL protocol over a jdbc:mysql: URL and reports a product version such
      // as "5.7.25-TiDB-v6.5.0", so the engine and its version are only knowable from that string
      String version = getStorageProductVersion();
      if (version.contains("-TiDB-v")) {
        // TiDB offers utf8mb4_0900_ai_ci since 7.4
        return !version.contains("-TiDB-v6.5.");
      }
      // MySQL offers utf8mb4_0900_ai_ci since 8.0
      return !version.startsWith("5.7.");
    }
    // TODO support Db2. Db2 has ICU collation but the collation can only be created at the database
    //  level which takes time to setup
    // SQLite and Yugabyte only have binary collations
    if (JdbcEnv.isDb2() || JdbcEnv.isSqlite() || JdbcEnv.isYugabyte()) {
      return false;
    }
    throw new IllegalStateException("Unsupported JDBC URL: " + JdbcEnv.getJdbcUrl());
  }

  /** Returns the product version the JDBC driver reports for the configured backend. */
  private static String getStorageProductVersion() {
    try (HikariDataSource dataSource = initDataSource();
        Connection connection = dataSource.getConnection()) {
      return connection.getMetaData().getDatabaseProductVersion();
    } catch (SQLException e) {
      throw new IllegalStateException("Failed to read the database product version", e);
    }
  }

  /** Returns the major product version the JDBC driver reports for the configured backend. */
  private static int getStorageMajorVersion() {
    try (HikariDataSource dataSource = initDataSource();
        Connection connection = dataSource.getConnection()) {
      return connection.getMetaData().getDatabaseMajorVersion();
    } catch (SQLException e) {
      throw new IllegalStateException("Failed to read the major database product version", e);
    }
  }

  private static HikariDataSource initDataSource() {
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(JdbcEnv.getProperties("unused")));
    return JdbcUtils.initDataSourceForAdmin(config, RdbEngineFactory.create(config));
  }

  /**
   * Returns whether the configured JDBC backend applies the collation integration tests' target
   * collation at the namespace level (MySQL and MariaDB) and runs the collation tests. Designed for
   * the JUnit 5 {@code EnabledIf} annotation on tests that exercise the namespace-level mechanism
   * itself.
   *
   * @return true if the backend applies the target collation at the namespace level and the
   *     collation integration tests are enabled, false otherwise
   */
  public static boolean isNamespaceCollationTestSupported() {
    return (JdbcEnv.isMysql() || JdbcEnv.isMariaDb()) && isCollationTestSupported();
  }

  /**
   * Returns the per-engine target collation used by the collation integration tests, to be passed
   * to the {@code AdminTestUtils} collation hooks ({@code alterNamespaceCollation} and {@code
   * alterTableCollation}). On PostgreSQL the returned name denotes the nondeterministic ICU
   * collation the hook creates in the test namespace rather than a built-in collation.
   *
   * @return the target collation for the configured JDBC backend
   * @throws IllegalStateException if no target collation is known for the configured JDBC backend
   */
  public static String getCollationTestTargetCollation() {
    if (JdbcEnv.isMysql()) {
      return "utf8mb4_0900_ai_ci";
    }
    if (JdbcEnv.isMariaDb()) {
      return "utf8mb4_uca1400_ai_ci";
    }
    if (JdbcEnv.isPostgresql()) {
      return POSTGRESQL_TEST_COLLATION;
    }
    if (JdbcEnv.isSqlServer()) {
      return "Latin1_General_100_CI_AI";
    }
    if (JdbcEnv.isOracle()) {
      return resolveOracleTargetCollation();
    }
    throw new IllegalStateException(
        "No target collation is known for the JDBC URL: " + JdbcEnv.getJdbcUrl());
  }

  /**
   * Returns Oracle's target collation, reading the major version from the JDBC driver on the first
   * call and caching it. The driver reports the version, so this needs no grant on {@code
   * v$instance}, which the CI test user does not hold. A failure to read it throws rather than
   * falling back to a name that is absent on some supported version.
   */
  private static synchronized String resolveOracleTargetCollation() {
    if (resolvedOracleTargetCollation != null) {
      return resolvedOracleTargetCollation;
    }
    resolvedOracleTargetCollation = oracleTargetCollation(getStorageMajorVersion());
    return resolvedOracleTargetCollation;
  }

  /**
   * Returns the target collation for the given Oracle major version. 19c offers no {@code
   * UCA1210_*} family, so it takes the UCA 7.0 name; 21c and later take the UCA 12.1 one.
   *
   * @param majorVersion the Oracle major version, as reported by the JDBC driver
   * @return the target collation for that version
   */
  private static String oracleTargetCollation(int majorVersion) {
    return majorVersion >= ORACLE_UCA1210_MAJOR ? "UCA1210_ROOT_AI" : "UCA0700_ROOT_AI";
  }
}
