package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

/**
 * Exercises the fail-fast collation verification of {@link
 * JdbcAdminTestUtils#alterTableCollation(String, String, String)} on the namespace-level engines: a
 * table leaked by a prior broken run is silently reused by {@code CREATE TABLE ... IF NOT EXISTS}
 * and keeps its stale collation, so the verification must reject it instead of letting the
 * collation suites run green against the wrong collation.
 */
@EnabledIf("com.scalar.db.storage.jdbc.JdbcCollationTestUtils#isNamespaceCollationTestSupported")
public class JdbcCollationVerificationIntegrationTest {

  private static final String NAMESPACE = "int_test_collation_verification";
  private static final String TABLE = "tbl";
  // Differs from every per-engine target collation so the leaked table is detectably stale
  private static final String STALE_COLLATION = "utf8mb4_bin";

  @Test
  @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
  public void alterTableCollation_TableLeakedWithStaleCollation_ShouldThrowIllegalStateException()
      throws Exception {
    JdbcAdminTestUtils adminTestUtils =
        new JdbcAdminTestUtils(JdbcEnv.getProperties("collation_verification"));
    try (Connection connection = openBackendConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("DROP DATABASE IF EXISTS " + NAMESPACE);
      statement.execute(
          "CREATE DATABASE " + NAMESPACE + " CHARACTER SET utf8mb4 COLLATE " + STALE_COLLATION);
      statement.execute(
          "CREATE TABLE "
              + NAMESPACE
              + "."
              + TABLE
              + " (pk VARCHAR(128) NOT NULL, val LONGTEXT, PRIMARY KEY (pk))");
      try {
        assertThatThrownBy(
                () ->
                    adminTestUtils.alterTableCollation(
                        NAMESPACE, TABLE, JdbcCollationTestUtils.getCollationTestTargetCollation()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("stale collation");
      } finally {
        statement.execute("DROP DATABASE " + NAMESPACE);
        adminTestUtils.close();
      }
    }
  }

  // ScalarDB serves jdbc:mysql URLs through the MariaDB driver, which is not visible to
  // DriverManager's service discovery in Gradle test workers and rejects the mysql scheme unless
  // permitMysqlScheme is set (see JdbcCollationTestUtils for the same handling)
  private static Connection openBackendConnection() throws SQLException {
    try {
      Class.forName("org.mariadb.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("The MariaDB JDBC driver class was not found", e);
    }
    String jdbcUrl = JdbcEnv.getJdbcUrl();
    if (jdbcUrl.startsWith("jdbc:mysql:") && !jdbcUrl.contains("permitMysqlScheme")) {
      jdbcUrl += (jdbcUrl.contains("?") ? "&" : "?") + "permitMysqlScheme=true";
    }
    return DriverManager.getConnection(jdbcUrl, JdbcEnv.getUsername(), JdbcEnv.getPassword());
  }
}
