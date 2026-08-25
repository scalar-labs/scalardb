package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.jdbc.plugin.failover.FailoverFailedSQLException;
import software.amazon.jdbc.plugin.failover.FailoverSuccessSQLException;
import software.amazon.jdbc.plugin.failover.TransactionStateUnknownSQLException;

/**
 * Pins the connection-pool behavior that ScalarDB's JDBC failure handling depends on.
 *
 * <p>ScalarDB deliberately adds no classification for the AWS Advanced JDBC Wrapper's failover
 * SQLStates. That is correct only because HikariCP evicts a connection whose SQLState starts with
 * "08", which makes the subsequent {@code rollback()} fail. {@link
 * com.scalar.db.transaction.jdbc.JdbcTransaction#commit()} infers "the outcome is unknown" from
 * exactly that failure. If the eviction stops happening -- because HikariCP changes, or because
 * someone sets {@code exceptionOverrideClassName} -- the rollback would quietly succeed as a no-op
 * and an unknown outcome would be reported as a plain commit failure, which the caller is told is
 * safe to retry.
 *
 * <p>These tests exist so that regression is caught in CI rather than in production. The companion
 * defense for the storage layer lives in {@code RdbEnginePostgresqlTest} and {@code
 * RdbEngineMysqlTest}, which pin that {@code isConflict} does not match failover SQLStates.
 */
class JdbcFailoverExceptionBehaviorTest {

  private HikariDataSource dataSource;

  @BeforeEach
  void setUp() {
    FailoverSimulatingDriver.reset();
    dataSource = FailoverSimulatingDriver.createDataSource();
  }

  @AfterEach
  void tearDown() {
    if (dataSource != null) {
      dataSource.close();
    }
    FailoverSimulatingDriver.reset();
  }

  @ParameterizedTest
  @ValueSource(strings = {"08001", "08S02", "08007"})
  void commit_GivenFailoverSqlState_ShouldEvictConnectionFromPool(String sqlState)
      throws SQLException {
    // Arrange
    Connection connection = dataSource.getConnection();
    FailoverSimulatingDriver.failOnCommitWith(sqlState);

    // Act
    assertThatThrownBy(connection::commit).isInstanceOf(SQLException.class);

    // Assert
    // HikariCP marked the connection broken, so it is no longer usable. This is the behavior
    // JdbcTransaction#commit() relies on to distinguish "unknown outcome" from "definitely failed".
    assertThatThrownBy(connection::rollback)
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("Connection is closed");
  }

  @Test
  void commit_GivenNonConnectionSqlState_ShouldKeepConnectionUsable() throws SQLException {
    // Arrange
    Connection connection = dataSource.getConnection();
    // 40001 is a genuine serialization failure. The connection survives it.
    FailoverSimulatingDriver.failOnCommitWith("40001");

    // Act
    assertThatThrownBy(connection::commit).isInstanceOf(SQLException.class);

    // Assert
    // The connection was not evicted, so rollback still works. JdbcTransaction#commit() therefore
    // classifies this as a definite failure rather than an unknown outcome -- which is correct.
    assertThat(connection.isClosed()).isFalse();
    connection.rollback();
    connection.close();
  }

  /**
   * The whole design rests on the AWS Advanced JDBC Wrapper reporting failover with a SQLState that
   * HikariCP recognizes as a connection exception, carried on the exception itself rather than on a
   * cause -- HikariCP walks {@code getNextException()} but never {@code getCause()}.
   *
   * <p>Pin that contract against the bundled wrapper so a version bump that changes it fails here
   * instead of silently removing the eviction the rest of this class depends on.
   */
  @Test
  void wrapperFailoverExceptions_ShouldCarryConnectionSqlStateAtTopLevel() {
    // Act
    // Assert
    assertThat(new FailoverSuccessSQLException().getSQLState()).startsWith("08");
    assertThat(new TransactionStateUnknownSQLException().getSQLState()).startsWith("08");
    assertThat(new FailoverFailedSQLException("failover failed").getSQLState()).startsWith("08");
  }
}
