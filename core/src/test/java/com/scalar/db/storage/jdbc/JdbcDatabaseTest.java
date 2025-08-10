package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.storage.RetriableExecutionException;
import com.scalar.db.io.Key;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class JdbcDatabaseTest {

  private static final String NAMESPACE = "ns";
  private static final String TABLE = "tbl";

  @Mock private DatabaseConfig databaseConfig;
  @Mock private BasicDataSource dataSource;
  @Mock private BasicDataSource tableMetadataDataSource;
  @Mock private JdbcService jdbcService;

  @Mock private ResultInterpreter resultInterpreter;
  @Mock private Connection connection;
  @Mock private PreparedStatement preparedStatement;
  @Mock private ResultSet resultSet;
  @Mock private SQLException sqlException;

  private JdbcDatabase jdbcDatabase;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    when(dataSource.getConnection()).thenReturn(connection);

    jdbcDatabase =
        new JdbcDatabase(
            databaseConfig,
            dataSource,
            tableMetadataDataSource,
            RdbEngine.createRdbEngineStrategy(RdbEngine.POSTGRESQL),
            jdbcService);
  }

  @Test
  public void whenGetOperationExecuted_shouldCallJdbcService() throws Exception {
    // Arrange

    // Act
    Get get = new Get(Key.ofText("p1", "val")).forNamespace(NAMESPACE).forTable(TABLE);
    jdbcDatabase.get(get);

    // Assert
    verify(connection).setReadOnly(true);
    verify(jdbcService).get(any(), any());
    verify(connection).close();
  }

  @Test
  public void
      whenGetOperationExecutedAndJdbcServiceThrowsSQLException_shouldThrowExecutionException()
          throws Exception {
    // Arrange
    when(jdbcService.get(any(), any())).thenThrow(sqlException);

    // Act Assert
    assertThatThrownBy(
            () -> {
              Get get = new Get(Key.ofText("p1", "val")).forNamespace(NAMESPACE).forTable(TABLE);
              jdbcDatabase.get(get);
            })
        .isInstanceOf(ExecutionException.class)
        .hasCause(sqlException);
    verify(connection).setReadOnly(true);
    verify(connection).close();
  }

  @Test
  public void whenScanOperationExecutedAndScannerClosed_shouldCallJdbcService() throws Exception {
    // Arrange
    when(jdbcService.getScanner(any(), any()))
        .thenReturn(
            new ScannerImpl(resultInterpreter, connection, preparedStatement, resultSet, true));

    // Act
    Scan scan = new Scan(Key.ofText("p1", "val")).forNamespace(NAMESPACE).forTable(TABLE);
    Scanner scanner = jdbcDatabase.scan(scan);
    scanner.close();

    // Assert
    verify(connection).setAutoCommit(false);
    verify(connection).setReadOnly(true);
    verify(jdbcService).getScanner(any(), any());
    verify(connection).commit();
    verify(connection).close();
  }

  @Test
  public void
      whenScanOperationExecutedAndJdbcServiceThrowsSQLException_shouldThrowExecutionException()
          throws Exception {
    // Arrange
    when(jdbcService.getScanner(any(), any())).thenThrow(sqlException);

    // Act Assert
    assertThatThrownBy(
            () -> {
              Scan scan = new Scan(Key.ofText("p1", "val")).forNamespace(NAMESPACE).forTable(TABLE);
              jdbcDatabase.scan(scan);
            })
        .isInstanceOf(ExecutionException.class)
        .hasCause(sqlException);
    verify(connection).setAutoCommit(false);
    verify(connection).setReadOnly(true);
    verify(connection).rollback();
    verify(connection).close();
  }

  @Test
  public void
      whenScanOperationExecutedAndJdbcServiceThrowsIllegalArgumentException_shouldCloseConnectionAndThrowIllegalArgumentException()
          throws Exception {
    // Arrange
    Exception cause = new IllegalArgumentException("Table not found");
    // Simulate the table not found scenario.
    when(jdbcService.getScanner(any(), any())).thenThrow(cause);

    // Act Assert
    assertThatThrownBy(
            () -> {
              Scan scan = new Scan(Key.ofText("p1", "val")).forNamespace(NAMESPACE).forTable(TABLE);
              jdbcDatabase.scan(scan);
            })
        .isInstanceOf(IllegalArgumentException.class);
    verify(connection).close();
  }

  @Test
  public void
      whenScanOperationExecutedAndScannerClosed_SQLExceptionThrownByConnectionCommit_shouldThrowIOException()
          throws Exception {
    // Arrange
    when(jdbcService.getScanner(any(), any()))
        .thenReturn(
            new ScannerImpl(resultInterpreter, connection, preparedStatement, resultSet, true));
    doThrow(sqlException).when(connection).commit();

    // Act
    Scan scan = new Scan(Key.ofText("p1", "val")).forNamespace(NAMESPACE).forTable(TABLE);
    Scanner scanner = jdbcDatabase.scan(scan);
    assertThatThrownBy(scanner::close).isInstanceOf(IOException.class).hasCause(sqlException);

    // Assert
    verify(connection).setAutoCommit(false);
    verify(connection).setReadOnly(true);
    verify(jdbcService).getScanner(any(), any());
    verify(connection).commit();
    verify(connection).rollback();
    verify(connection).close();
  }

  @Test
  public void whenPutOperationExecuted_shouldCallJdbcService() throws Exception {
    // Arrange
    when(jdbcService.put(any(), any())).thenReturn(true);

    // Act
    Put put =
        new Put(Key.ofText("p1", "val1"))
            .withValue("v1", "val2")
            .forNamespace(NAMESPACE)
            .forTable(TABLE);
    jdbcDatabase.put(put);

    // Assert
    verify(jdbcService).put(any(), any());
    verify(connection).close();
  }

  @Test
  public void
      whenPutOperationWithConditionExecutedAndJdbcServiceReturnsFalse_shouldThrowNoMutationException()
          throws Exception {
    // Arrange
    when(jdbcService.put(any(), any())).thenReturn(false);

    // Act Assert
    assertThatThrownBy(
            () -> {
              Put put =
                  new Put(Key.ofText("p1", "val1"))
                      .withValue("v1", "val2")
                      .withCondition(new PutIfNotExists())
                      .forNamespace(NAMESPACE)
                      .forTable(TABLE);
              jdbcDatabase.put(put);
            })
        .isInstanceOf(NoMutationException.class);
    verify(connection).close();
  }

  @Test
  public void
      whenPutOperationExecutedAndJdbcServiceThrowsSQLException_shouldThrowExecutionException()
          throws Exception {
    // Arrange
    when(jdbcService.put(any(), any())).thenThrow(sqlException);

    // Act Assert
    assertThatThrownBy(
            () -> {
              Put put =
                  new Put(Key.ofText("p1", "val1"))
                      .withValue("v1", "val2")
                      .forNamespace(NAMESPACE)
                      .forTable(TABLE);
              jdbcDatabase.put(put);
            })
        .isInstanceOf(ExecutionException.class)
        .hasCause(sqlException);
    verify(connection).close();
  }

  @Test
  public void whenDeleteOperationExecuted_shouldCallJdbcService() throws Exception {
    // Arrange
    when(jdbcService.delete(any(), any())).thenReturn(true);

    // Act
    Delete delete = new Delete(Key.ofText("p1", "val1")).forNamespace(NAMESPACE).forTable(TABLE);
    jdbcDatabase.delete(delete);

    // Assert
    verify(jdbcService).delete(any(), any());
    verify(connection).close();
  }

  @Test
  public void
      whenDeleteOperationWithConditionExecutedAndJdbcServiceReturnsFalse_shouldThrowNoMutationException()
          throws Exception {
    // Arrange
    when(jdbcService.delete(any(), any())).thenReturn(false);

    // Act Assert
    assertThatThrownBy(
            () -> {
              Delete delete =
                  new Delete(Key.ofText("p1", "val1"))
                      .withCondition(new DeleteIfExists())
                      .forNamespace(NAMESPACE)
                      .forTable(TABLE);
              jdbcDatabase.delete(delete);
            })
        .isInstanceOf(NoMutationException.class);
    verify(connection).close();
  }

  @Test
  public void
      whenDeleteOperationExecutedAndJdbcServiceThrowsSQLException_shouldThrowExecutionException()
          throws Exception {
    // Arrange
    when(jdbcService.delete(any(), any())).thenThrow(sqlException);

    // Act Assert
    assertThatThrownBy(
            () -> {
              Delete delete =
                  new Delete(Key.ofText("p1", "val1")).forNamespace(NAMESPACE).forTable(TABLE);
              jdbcDatabase.delete(delete);
            })
        .isInstanceOf(ExecutionException.class)
        .hasCause(sqlException);
    verify(connection).close();
  }

  @Test
  public void whenMutateOperationExecuted_shouldCallJdbcService() throws Exception {
    // Arrange
    when(jdbcService.mutate(any(), any())).thenReturn(true);

    // Act
    Put put =
        new Put(Key.ofText("p1", "val1"))
            .withValue("v1", "val2")
            .forNamespace(NAMESPACE)
            .forTable(TABLE);
    Delete delete = new Delete(Key.ofText("p1", "val1")).forNamespace(NAMESPACE).forTable(TABLE);
    jdbcDatabase.mutate(Arrays.asList(put, delete));

    // Assert
    verify(connection).setAutoCommit(false);
    verify(jdbcService).mutate(any(), any());
    verify(connection).commit();
    verify(connection).close();
  }

  @Test
  public void
      whenMutateOperationWithConditionExecutedAndJdbcServiceReturnsFalse_shouldThrowNoMutationException()
          throws Exception {
    // Arrange
    when(jdbcService.mutate(any(), any())).thenReturn(false);

    // Act Assert
    assertThatThrownBy(
            () -> {
              Put put =
                  new Put(Key.ofText("p1", "val1"))
                      .withValue("v1", "val2")
                      .withCondition(new PutIfNotExists())
                      .forNamespace(NAMESPACE)
                      .forTable(TABLE);
              Delete delete =
                  new Delete(Key.ofText("p1", "val1"))
                      .withCondition(new DeleteIfExists())
                      .forNamespace(NAMESPACE)
                      .forTable(TABLE);
              jdbcDatabase.mutate(Arrays.asList(put, delete));
            })
        .isInstanceOf(NoMutationException.class);
    verify(connection).setAutoCommit(false);
    verify(jdbcService).mutate(any(), any());
    verify(connection).rollback();
    verify(connection).close();
  }

  @Test
  public void
      whenMutateOperationExecutedAndJdbcServiceThrowsSQLException_shouldThrowExecutionException()
          throws Exception {
    // Arrange
    when(jdbcService.mutate(any(), any())).thenThrow(sqlException);

    // Act Assert
    assertThatThrownBy(
            () -> {
              Put put =
                  new Put(Key.ofText("p1", "val1"))
                      .withValue("v1", "val2")
                      .forNamespace(NAMESPACE)
                      .forTable(TABLE);
              Delete delete =
                  new Delete(Key.ofText("p1", "val1")).forNamespace(NAMESPACE).forTable(TABLE);
              jdbcDatabase.mutate(Arrays.asList(put, delete));
            })
        .isInstanceOf(ExecutionException.class)
        .hasCause(sqlException);
    verify(connection).setAutoCommit(false);
    verify(jdbcService).mutate(any(), any());
    verify(connection).rollback();
    verify(connection).close();
  }

  @Test
  public void mutate_withConflictError_shouldThrowRetriableExecutionException()
      throws SQLException, ExecutionException {
    // Arrange
    when(jdbcService.mutate(any(), any())).thenThrow(sqlException);
    when(sqlException.getSQLState()).thenReturn("40001");

    // Act Assert
    assertThatThrownBy(
            () -> {
              Put put =
                  new Put(Key.ofText("p1", "val1"))
                      .withValue("v1", "val2")
                      .forNamespace(NAMESPACE)
                      .forTable(TABLE);
              Delete delete =
                  new Delete(Key.ofText("p1", "val1")).forNamespace(NAMESPACE).forTable(TABLE);
              jdbcDatabase.mutate(Arrays.asList(put, delete));
            })
        .isInstanceOf(RetriableExecutionException.class)
        .hasCause(sqlException);
    verify(connection).setAutoCommit(false);
    verify(jdbcService).mutate(any(), any());
    verify(connection).rollback();
    verify(connection).close();
  }

  @Test
  public void mutate_WhenSettingAutoCommitFails_ShouldThrowExceptionAndCloseConnection()
      throws SQLException, ExecutionException {
    // Arrange
    Exception exception = new RuntimeException("Failed to set auto-commit");
    doThrow(exception).when(connection).setAutoCommit(anyBoolean());

    // Act Assert
    assertThatThrownBy(
            () -> {
              Put put =
                  new Put(Key.ofText("p1", "val1"))
                      .withValue("v1", "val2")
                      .forNamespace(NAMESPACE)
                      .forTable(TABLE);
              Delete delete =
                  new Delete(Key.ofText("p1", "val1")).forNamespace(NAMESPACE).forTable(TABLE);
              jdbcDatabase.mutate(Arrays.asList(put, delete));
            })
        .isEqualTo(exception);
    verify(connection).setAutoCommit(false);
    verify(jdbcService, never()).mutate(any(), any());
    verify(connection, never()).rollback();
    verify(connection).close();
  }
}
