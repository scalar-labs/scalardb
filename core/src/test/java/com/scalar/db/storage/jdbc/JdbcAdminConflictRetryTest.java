package com.scalar.db.storage.jdbc;

import static com.scalar.db.storage.jdbc.JdbcAdmin.MAX_CONFLICT_RETRY_COUNT;
import static com.scalar.db.storage.jdbc.JdbcAdmin.execute;
import static com.scalar.db.storage.jdbc.JdbcAdmin.executeQuery;
import static com.scalar.db.storage.jdbc.JdbcAdmin.executeUpdate;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Tests for the conflict retry behavior of the {@link JdbcAdmin} statement helpers.
 *
 * <p>The retried region covers the statement execution and the commit, but not the SQL warning
 * handling that follows a successful commit. See {@code JdbcAdmin#executeWithConflictRetry}.
 */
public class JdbcAdminConflictRetryTest {

  private static final String SQL = "INSERT INTO \"scalardb\".\"metadata\" VALUES ('t','c')";
  private static final String OTHER_SQL = "CREATE TABLE \"ns\".\"tbl\" (pk INT)";

  @Mock private Connection connection;
  @Mock private Statement statement;
  @Mock private PreparedStatement preparedStatement;
  @Mock private ResultSet resultSet;
  @Mock private RdbEngineStrategy rdbEngine;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    when(connection.createStatement()).thenReturn(statement);
    when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
  }

  private static SQLException conflict(int errorCode) {
    return new SQLException("conflict", "40001", errorCode);
  }

  private void treatAsConflict() {
    when(rdbEngine.isConflict(any(SQLException.class))).thenReturn(true);
  }

  // --- execute -------------------------------------------------------------------------------

  @Test
  public void execute_WhenNoErrorOccurs_ShouldExecuteOnceAndCommitOnce() throws Exception {
    // Act
    execute(connection, rdbEngine, SQL, true);

    // Assert
    verify(statement).execute(SQL);
    verify(connection).commit();
    verify(connection, never()).rollback();
  }

  @Test
  public void execute_WhenConflictOccursOnceThenSucceeds_ShouldRetryAndCommitOnce()
      throws Exception {
    // Arrange
    treatAsConflict();
    doThrow(conflict(8177)).doReturn(false).when(statement).execute(SQL);

    // Act
    execute(connection, rdbEngine, SQL, true);

    // Assert
    verify(statement, times(2)).execute(SQL);
    verify(connection).rollback();
    verify(connection).commit();
  }

  @Test
  public void execute_WhenConflictOccursUpToTheLimitThenSucceeds_ShouldNotThrow() throws Exception {
    // Arrange
    treatAsConflict();
    SQLException[] failures = new SQLException[MAX_CONFLICT_RETRY_COUNT];
    for (int i = 0; i < MAX_CONFLICT_RETRY_COUNT; i++) {
      failures[i] = conflict(8177);
    }
    doThrow(failures).doReturn(false).when(statement).execute(SQL);

    // Act
    execute(connection, rdbEngine, SQL, true);

    // Assert
    verify(statement, times(MAX_CONFLICT_RETRY_COUNT + 1)).execute(SQL);
    verify(connection, times(MAX_CONFLICT_RETRY_COUNT)).rollback();
    verify(connection).commit();
  }

  @Test
  public void execute_WhenConflictExceedsTheLimit_ShouldThrowTheOriginalException()
      throws Exception {
    // Arrange
    treatAsConflict();
    SQLException original = conflict(8177);
    doThrow(original).when(statement).execute(SQL);

    // Act Assert
    assertThatThrownBy(() -> execute(connection, rdbEngine, SQL, true))
        .isSameAs(original)
        .hasMessage("conflict");
    assertThat(original.getErrorCode()).isEqualTo(8177);
    verify(statement, times(MAX_CONFLICT_RETRY_COUNT + 1)).execute(SQL);
    verify(connection, never()).commit();
  }

  @Test
  public void execute_WhenErrorIsNotConflict_ShouldThrowImmediatelyWithoutRollback()
      throws Exception {
    // Arrange
    when(rdbEngine.isConflict(any(SQLException.class))).thenReturn(false);
    SQLException original = new SQLException("table or view does not exist", "42000", 942);
    doThrow(original).when(statement).execute(SQL);

    // Act Assert
    assertThatThrownBy(() -> execute(connection, rdbEngine, SQL, true)).isSameAs(original);
    verify(statement).execute(SQL);
    verify(connection, never()).rollback();
    verify(connection, never()).commit();
  }

  @Test
  public void execute_WhenAutoCommit_ShouldRetryWithoutCallingRollbackOrCommit() throws Exception {
    // Arrange
    treatAsConflict();
    doThrow(conflict(8177)).doReturn(false).when(statement).execute(SQL);

    // Act
    execute(connection, rdbEngine, SQL, false);

    // Assert
    verify(statement, times(2)).execute(SQL);
    verify(connection, never()).rollback();
    verify(connection, never()).commit();
  }

  @Test
  public void execute_WhenRollbackFails_ShouldThrowOriginalExceptionWithSuppressed()
      throws Exception {
    // Arrange
    treatAsConflict();
    SQLException original = conflict(8177);
    SQLException rollbackFailure = new SQLException("rollback failed");
    doThrow(original).when(statement).execute(SQL);
    doThrow(rollbackFailure).when(connection).rollback();

    // Act Assert
    assertThatThrownBy(() -> execute(connection, rdbEngine, SQL, true)).isSameAs(original);
    assertThat(original.getSuppressed()).containsExactly(rollbackFailure);
    verify(statement).execute(SQL);
  }

  @Test
  public void execute_WhenCommitReportsConflict_ShouldRetryStatementAndCommit() throws Exception {
    // Arrange -- PostgreSQL surfaces serialization failures during the commit attempt
    treatAsConflict();
    doThrow(conflict(0)).doNothing().when(connection).commit();

    // Act
    execute(connection, rdbEngine, SQL, true);

    // Assert
    verify(statement, times(2)).execute(SQL);
    verify(connection).rollback();
    verify(connection, times(2)).commit();
  }

  @Test
  public void execute_WhenWarningHandlerThrowsAfterCommit_ShouldNotReExecuteTheStatement()
      throws Exception {
    // Arrange
    treatAsConflict();
    when(statement.getWarnings()).thenReturn(new SQLWarning("duplicated index"));
    SQLException fromHandler = conflict(8177);

    // Act Assert
    assertThatThrownBy(
            () ->
                execute(
                    connection,
                    rdbEngine,
                    SQL,
                    true,
                    warning -> {
                      throw fromHandler;
                    }))
        .isSameAs(fromHandler);
    verify(statement).execute(SQL);
    verify(connection).commit();
    verify(connection, never()).rollback();
  }

  @Test
  public void execute_WithSqls_WhenConflictOccursOnSecondSql_ShouldRetryOnlyThatSql()
      throws Exception {
    // Arrange
    treatAsConflict();
    doReturn(false).when(statement).execute(OTHER_SQL);
    doThrow(conflict(8177)).doReturn(false).when(statement).execute(SQL);

    // Act
    execute(connection, rdbEngine, new String[] {OTHER_SQL, SQL}, true);

    // Assert
    verify(statement, times(1)).execute(OTHER_SQL);
    verify(statement, times(2)).execute(SQL);
    // One statement is one transaction here: each SQL commits on its own, which is what makes
    // re-executing only the conflicting statement safe.
    verify(connection, times(2)).commit();
  }

  @Test
  public void execute_WhenDeadlockOrConsistentReadFailureOccurs_ShouldRetry() throws Exception {
    // Arrange -- isConflict() also covers ORA-00060 and ORA-08176
    treatAsConflict();
    doThrow(conflict(60)).doThrow(conflict(8176)).doReturn(false).when(statement).execute(SQL);

    // Act
    execute(connection, rdbEngine, SQL, true);

    // Assert
    verify(statement, times(3)).execute(SQL);
    verify(connection, times(2)).rollback();
  }

  // --- executeUpdate -------------------------------------------------------------------------

  @Test
  public void executeUpdate_WhenConflictOccursOnceThenSucceeds_ShouldReapplyParamsAndRetry()
      throws Exception {
    // Arrange
    treatAsConflict();
    doThrow(conflict(8177)).doReturn(1).when(preparedStatement).executeUpdate();
    List<String> boundValues = new ArrayList<>();

    // Act
    executeUpdate(connection, rdbEngine, SQL, true, ps -> boundValues.add("bound"));

    // Assert
    verify(preparedStatement, times(2)).executeUpdate();
    assertThat(boundValues).hasSize(2);
    verify(connection).rollback();
    verify(connection).commit();
  }

  // --- executeQuery --------------------------------------------------------------------------

  @Test
  public void
      executeQuery_WhenConflictOccursOnceThenSucceeds_ShouldReturnMappedResultWithoutDuping()
          throws Exception {
    // Arrange
    treatAsConflict();
    when(resultSet.next()).thenReturn(true, false, true, false);
    when(resultSet.getString(1)).thenReturn("a");
    doThrow(conflict(8177)).doReturn(resultSet).when(statement).executeQuery(SQL);

    // Act -- the mapper builds its collection inside the lambda, so a retry must not double-add
    List<String> result =
        executeQuery(
            connection,
            rdbEngine,
            SQL,
            true,
            rs -> {
              List<String> names = new ArrayList<>();
              while (rs.next()) {
                names.add(rs.getString(1));
              }
              return names;
            });

    // Assert
    assertThat(result).containsExactly("a");
    verify(statement, times(2)).executeQuery(SQL);
    verify(connection).rollback();
  }

  @Test
  public void executeQuery_WhenConflictOccursDuringResultIteration_ShouldNotDupeMappedResult()
      throws Exception {
    // Arrange -- ORA-08176 is a consistent read failure, so the conflict surfaces while the result
    // set is being read, after the mapper has already consumed part of it
    treatAsConflict();
    when(statement.executeQuery(SQL)).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true).thenThrow(conflict(8176)).thenReturn(true, false);
    when(resultSet.getString(1)).thenReturn("a");

    // Act -- the mapper is re-run from scratch against a new result set, so the partially built
    // collection from the first attempt must not survive into the returned one
    List<String> result =
        executeQuery(
            connection,
            rdbEngine,
            SQL,
            true,
            rs -> {
              List<String> names = new ArrayList<>();
              while (rs.next()) {
                names.add(rs.getString(1));
              }
              return names;
            });

    // Assert
    assertThat(result).containsExactly("a");
    verify(statement, times(2)).executeQuery(SQL);
    verify(connection).rollback();
    verify(connection).commit();
  }

  @Test
  public void executeQuery_WithParamSetter_WhenConflictOccurs_ShouldReapplyParamsAndRetry()
      throws Exception {
    // Arrange
    treatAsConflict();
    when(resultSet.next()).thenReturn(false);
    doThrow(conflict(8177)).doReturn(resultSet).when(preparedStatement).executeQuery();
    List<String> boundValues = new ArrayList<>();

    // Act
    Boolean result =
        executeQuery(
            connection, rdbEngine, SQL, true, ps -> boundValues.add("bound"), ResultSet::next);

    // Assert
    assertThat(result).isFalse();
    assertThat(boundValues).hasSize(2);
    verify(preparedStatement, times(2)).executeQuery();
  }
}
