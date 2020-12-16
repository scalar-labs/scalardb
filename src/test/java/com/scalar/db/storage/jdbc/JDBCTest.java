package com.scalar.db.storage.jdbc;

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
import com.scalar.db.storage.jdbc.query.SelectQuery;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JDBCTest {

  @Mock private BasicDataSource dataSource;
  @Mock private JDBCService jdbcService;

  @Mock private SelectQuery selectQuery;
  @Mock private Connection connection;
  @Mock private ResultSet resultSet;
  @Mock private SQLException sqlException;

  private JDBC jdbc;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    jdbc = new JDBC(dataSource, jdbcService);

    when(dataSource.getConnection()).thenReturn(connection);
  }

  @Test
  public void whenGetOperationExecuted_shouldCallJDBCService() throws Exception {
    // Arrange

    // Act
    Get get = new Get(new Key(new TextValue("p1", "val")));
    jdbc.get(get);

    // Assert
    verify(jdbcService).get(any(), any(), any(), any());
    verify(connection).close();
  }

  @Test
  public void
      whenGetOperationExecutedAndJDBCServiceThrowsSQLException_shouldThrowExecutionException()
          throws Exception {
    // Arrange
    when(jdbcService.get(any(), any(), any(), any())).thenThrow(sqlException);

    // Act Assert
    assertThatThrownBy(
            () -> {
              Get get = new Get(new Key(new TextValue("p1", "val")));
              jdbc.get(get);
            })
        .isInstanceOf(ExecutionException.class);
    verify(connection).close();
  }

  @Test
  public void whenScanOperationExecutedAndScannerClosed_shouldCallJDBCService() throws Exception {
    // Arrange
    when(jdbcService.scan(any(), any(), any(), any()))
        .thenReturn(new ScannerImpl(selectQuery, connection, resultSet));

    // Act
    Scan scan = new Scan(new Key(new TextValue("p1", "val")));
    Scanner scanner = jdbc.scan(scan);
    scanner.close();

    // Assert
    verify(jdbcService).scan(any(), any(), any(), any());
    verify(connection).close();
  }

  @Test
  public void
      whenScanOperationExecutedAndJDBCServiceThrowsSQLException_shouldThrowExecutionException()
          throws Exception {
    // Arrange
    when(jdbcService.scan(any(), any(), any(), any())).thenThrow(sqlException);

    // Act Assert
    assertThatThrownBy(
            () -> {
              Scan scan = new Scan(new Key(new TextValue("p1", "val")));
              jdbc.scan(scan);
            })
        .isInstanceOf(ExecutionException.class);
    verify(connection).close();
  }

  @Test
  public void whenPutOperationExecuted_shouldCallJDBCService() throws Exception {
    // Arrange
    when(jdbcService.put(any(), any(), any(), any())).thenReturn(true);

    // Act
    Put put = new Put(new Key(new TextValue("p1", "val1"))).withValue(new TextValue("v1", "val2"));
    jdbc.put(put);

    // Assert
    verify(jdbcService).put(any(), any(), any(), any());
    verify(connection).close();
  }

  @Test
  public void
      whenPutOperationWithConditionExecutedAndJDBCServiceReturnsFalse_shouldThrowNoMutationException()
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
              jdbc.put(put);
            })
        .isInstanceOf(NoMutationException.class);
    verify(connection).close();
  }

  @Test
  public void
      whenPutOperationExecutedAndJDBCServiceThrowsSQLException_shouldThrowExecutionException()
          throws Exception {
    // Arrange
    when(jdbcService.put(any(), any(), any(), any())).thenThrow(sqlException);

    // Act Assert
    assertThatThrownBy(
            () -> {
              Put put =
                  new Put(new Key(new TextValue("p1", "val1")))
                      .withValue(new TextValue("v1", "val2"));
              jdbc.put(put);
            })
        .isInstanceOf(ExecutionException.class);
    verify(connection).close();
  }

  @Test
  public void whenDeleteOperationExecuted_shouldCallJDBCService() throws Exception {
    // Arrange
    when(jdbcService.delete(any(), any(), any(), any())).thenReturn(true);

    // Act
    Delete delete = new Delete(new Key(new TextValue("p1", "val1")));
    jdbc.delete(delete);

    // Assert
    verify(jdbcService).delete(any(), any(), any(), any());
    verify(connection).close();
  }

  @Test
  public void
      whenDeleteOperationWithConditionExecutedAndJDBCServiceReturnsFalse_shouldThrowNoMutationException()
          throws Exception {
    // Arrange
    when(jdbcService.delete(any(), any(), any(), any())).thenReturn(false);

    // Act Assert
    assertThatThrownBy(
            () -> {
              Delete delete =
                  new Delete(new Key(new TextValue("p1", "val1")))
                      .withCondition(new DeleteIfExists());
              jdbc.delete(delete);
            })
        .isInstanceOf(NoMutationException.class);
    verify(connection).close();
  }

  @Test
  public void
      whenDeleteOperationExecutedAndJDBCServiceThrowsSQLException_shouldThrowExecutionException()
          throws Exception {
    // Arrange
    when(jdbcService.delete(any(), any(), any(), any())).thenThrow(sqlException);

    // Act Assert
    assertThatThrownBy(
            () -> {
              Delete delete = new Delete(new Key(new TextValue("p1", "val1")));
              jdbc.delete(delete);
            })
        .isInstanceOf(ExecutionException.class);
    verify(connection).close();
  }

  @Test
  public void whenMutateOperationExecuted_shouldCallJDBCService() throws Exception {
    // Arrange
    when(jdbcService.mutate(any(), any(), any(), any())).thenReturn(true);

    // Act
    Put put = new Put(new Key(new TextValue("p1", "val1"))).withValue(new TextValue("v1", "val2"));
    Delete delete = new Delete(new Key(new TextValue("p1", "val1")));
    jdbc.mutate(Arrays.asList(put, delete));

    // Assert
    verify(jdbcService).mutate(any(), any(), any(), any());
    verify(connection).close();
  }

  @Test
  public void
      whenMutateOperationWithConditionExecutedAndJDBCServiceReturnsFalse_shouldThrowNoMutationException()
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
              jdbc.mutate(Arrays.asList(put, delete));
            })
        .isInstanceOf(NoMutationException.class);
    verify(connection).close();
  }

  @Test
  public void
      whenMutateOperationExecutedAndJDBCServiceThrowsSQLException_shouldThrowExecutionException()
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
              jdbc.mutate(Arrays.asList(put, delete));
            })
        .isInstanceOf(ExecutionException.class);
    verify(connection).close();
  }
}
