package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class JdbcDatabaseTest {

  @Mock private BasicDataSource dataSource;
  @Mock private JdbcService jdbcService;

  @Mock private ResultInterpreter resultInterpreter;
  @Mock private Connection connection;
  @Mock private PreparedStatement preparedStatement;
  @Mock private ResultSet resultSet;
  @Mock private SQLException sqlException;

  private JdbcDatabase jdbcDatabase;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    jdbcDatabase = new JdbcDatabase(dataSource, jdbcService);

    when(dataSource.getConnection()).thenReturn(connection);
  }

  @Test
  public void whenGetOperationExecuted_shouldCallJdbcService() throws Exception {
    // Arrange

    // Act
    Get get = new Get(new Key(new TextValue("p1", "val")));
    jdbcDatabase.get(get);

    // Assert
    verify(jdbcService).get(any(), any(), any(), any());
    verify(connection).close();
  }

  @Test
  public void
      whenGetOperationExecutedAndJdbcServiceThrowsSQLException_shouldThrowExecutionException()
          throws Exception {
    // Arrange
    when(jdbcService.get(any(), any(), any(), any())).thenThrow(sqlException);

    // Act Assert
    assertThatThrownBy(
            () -> {
              Get get = new Get(new Key(new TextValue("p1", "val")));
              jdbcDatabase.get(get);
            })
        .isInstanceOf(ExecutionException.class);
    verify(connection).close();
  }

  @Test
  public void whenScanOperationExecutedAndScannerClosed_shouldCallJdbcService() throws Exception {
    // Arrange
    when(jdbcService.scan(any(), any(), any(), any()))
        .thenReturn(new ScannerImpl(resultInterpreter, connection, preparedStatement, resultSet));

    // Act
    Scan scan = new Scan(new Key(new TextValue("p1", "val")));
    Scanner scanner = jdbcDatabase.scan(scan);
    scanner.close();

    // Assert
    verify(jdbcService).scan(any(), any(), any(), any());
    verify(connection).close();
  }

  @Test
  public void
      whenScanOperationExecutedAndJdbcServiceThrowsSQLException_shouldThrowExecutionException()
          throws Exception {
    // Arrange
    when(jdbcService.scan(any(), any(), any(), any())).thenThrow(sqlException);

    // Act Assert
    assertThatThrownBy(
            () -> {
              Scan scan = new Scan(new Key(new TextValue("p1", "val")));
              jdbcDatabase.scan(scan);
            })
        .isInstanceOf(ExecutionException.class);
    verify(connection).close();
  }

  @Test
  public void whenPutOperationExecuted_shouldCallJdbcService() throws Exception {
    // Arrange
    when(jdbcService.put(any(), any(), any(), any())).thenReturn(true);

    // Act
    Put put = new Put(new Key(new TextValue("p1", "val1"))).withValue(new TextValue("v1", "val2"));
    jdbcDatabase.put(put);

    // Assert
    verify(jdbcService).put(any(), any(), any(), any());
    verify(connection).close();
  }

  @Test
  public void
      whenPutOperationWithConditionExecutedAndJdbcServiceReturnsFalse_shouldThrowNoMutationException()
          throws Exception {
    // Arrange
    when(jdbcService.put(any(), any(), any(), any())).thenReturn(false);

    // Act Assert
    assertThatThrownBy(
            () -> {
              Put put =
                  new Put(new Key(new TextValue("p1", "val1")))
                      .withValue(new TextValue("v1", "val2"))
                      .withCondition(new PutIfNotExists());
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
    when(jdbcService.put(any(), any(), any(), any())).thenThrow(sqlException);

    // Act Assert
    assertThatThrownBy(
            () -> {
              Put put =
                  new Put(new Key(new TextValue("p1", "val1")))
                      .withValue(new TextValue("v1", "val2"));
              jdbcDatabase.put(put);
            })
        .isInstanceOf(ExecutionException.class);
    verify(connection).close();
  }

  @Test
  public void whenDeleteOperationExecuted_shouldCallJdbcService() throws Exception {
    // Arrange
    when(jdbcService.delete(any(), any(), any(), any())).thenReturn(true);

    // Act
    Delete delete = new Delete(new Key(new TextValue("p1", "val1")));
    jdbcDatabase.delete(delete);

    // Assert
    verify(jdbcService).delete(any(), any(), any(), any());
    verify(connection).close();
  }

  @Test
  public void
      whenDeleteOperationWithConditionExecutedAndJdbcServiceReturnsFalse_shouldThrowNoMutationException()
          throws Exception {
    // Arrange
    when(jdbcService.delete(any(), any(), any(), any())).thenReturn(false);

    // Act Assert
    assertThatThrownBy(
            () -> {
              Delete delete =
                  new Delete(new Key(new TextValue("p1", "val1")))
                      .withCondition(new DeleteIfExists());
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
    when(jdbcService.delete(any(), any(), any(), any())).thenThrow(sqlException);

    // Act Assert
    assertThatThrownBy(
            () -> {
              Delete delete = new Delete(new Key(new TextValue("p1", "val1")));
              jdbcDatabase.delete(delete);
            })
        .isInstanceOf(ExecutionException.class);
    verify(connection).close();
  }

  @Test
  public void whenMutateOperationExecuted_shouldCallJdbcService() throws Exception {
    // Arrange
    when(jdbcService.mutate(any(), any(), any(), any())).thenReturn(true);

    // Act
    Put put = new Put(new Key(new TextValue("p1", "val1"))).withValue(new TextValue("v1", "val2"));
    Delete delete = new Delete(new Key(new TextValue("p1", "val1")));
    jdbcDatabase.mutate(Arrays.asList(put, delete));

    // Assert
    verify(jdbcService).mutate(any(), any(), any(), any());
    verify(connection).close();
  }

  @Test
  public void
      whenMutateOperationWithConditionExecutedAndJdbcServiceReturnsFalse_shouldThrowNoMutationException()
          throws Exception {
    // Arrange
    when(jdbcService.mutate(any(), any(), any(), any())).thenReturn(false);

    // Act Assert
    assertThatThrownBy(
            () -> {
              Put put =
                  new Put(new Key(new TextValue("p1", "val1")))
                      .withValue(new TextValue("v1", "val2"))
                      .withCondition(new PutIfNotExists());
              Delete delete =
                  new Delete(new Key(new TextValue("p1", "val1")))
                      .withCondition(new DeleteIfExists());
              jdbcDatabase.mutate(Arrays.asList(put, delete));
            })
        .isInstanceOf(NoMutationException.class);
    verify(connection).close();
  }

  @Test
  public void
      whenMutateOperationExecutedAndJdbcServiceThrowsSQLException_shouldThrowExecutionException()
          throws Exception {
    // Arrange
    when(jdbcService.mutate(any(), any(), any(), any())).thenThrow(sqlException);

    // Act Assert
    assertThatThrownBy(
            () -> {
              Put put =
                  new Put(new Key(new TextValue("p1", "val1")))
                      .withValue(new TextValue("v1", "val2"));
              Delete delete = new Delete(new Key(new TextValue("p1", "val1")));
              jdbcDatabase.mutate(Arrays.asList(put, delete));
            })
        .isInstanceOf(ExecutionException.class);
    verify(connection).close();
  }
}
