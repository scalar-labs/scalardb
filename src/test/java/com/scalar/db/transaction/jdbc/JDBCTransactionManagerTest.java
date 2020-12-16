package com.scalar.db.transaction.jdbc;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.storage.jdbc.JDBCService;
import com.scalar.db.storage.jdbc.ScannerImpl;
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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JDBCTransactionManagerTest {

  @Mock private BasicDataSource dataSource;
  @Mock private JDBCService jdbcService;

  @Mock private SelectQuery selectQuery;
  @Mock private Connection connection;
  @Mock private ResultSet resultSet;
  @Mock private SQLException sqlException;

  private JDBCTransactionManager manager;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    manager = new JDBCTransactionManager(dataSource, jdbcService);

    when(dataSource.getConnection()).thenReturn(connection);
  }

  @Test
  public void whenSomeOperationsExecutedAndCommit_shouldCallJDBCService() throws Exception {
    // Arrange
    when(jdbcService.scan(any(), any(), any(), any()))
        .thenReturn(new ScannerImpl(selectQuery, connection, resultSet));
    when(resultSet.next()).thenReturn(false);
    when(jdbcService.put(any(), any(), any(), any())).thenReturn(true);
    when(jdbcService.delete(any(), any(), any(), any())).thenReturn(true);

    // Act
    JDBCTransaction transaction = manager.start();
    Get get = new Get(new Key(new TextValue("p1", "val")));
    transaction.get(get);
    Scan scan = new Scan(new Key(new TextValue("p1", "val")));
    transaction.scan(scan);
    Put put = new Put(new Key(new TextValue("p1", "val1"))).withValue(new TextValue("v1", "val2"));
    transaction.put(put);
    Delete delete = new Delete(new Key(new TextValue("p1", "val1")));
    transaction.delete(delete);
    transaction.mutate(Arrays.asList(put, delete));
    transaction.commit();

    // Assert
    verify(jdbcService).get(any(), any(), any(), any());
    verify(jdbcService).scan(any(), any(), any(), any());
    verify(jdbcService).put(any(), any(), any(), any());
    verify(jdbcService).delete(any(), any(), any(), any());
    verify(jdbcService).mutate(any(), any(), any(), any());
    verify(connection).commit();
    verify(connection).close();
  }

  @Test
  public void whenGetOperationsExecutedAndJDBCServiceThrowsSQLException_shouldThrowCrudException()
      throws Exception {
    // Arrange
    when(jdbcService.get(any(), any(), any(), any())).thenThrow(sqlException);

    // Act Assert
    assertThatThrownBy(
            () -> {
              JDBCTransaction transaction = manager.start();
              Get get = new Get(new Key(new TextValue("p1", "val")));
              transaction.get(get);
            })
        .isInstanceOf(CrudException.class);
  }

  @Test
  public void whenScanOperationsExecutedAndJDBCServiceThrowsSQLException_shouldThrowCrudException()
      throws Exception {
    // Arrange
    when(jdbcService.scan(any(), any(), any(), any())).thenThrow(sqlException);

    // Act Assert
    assertThatThrownBy(
            () -> {
              JDBCTransaction transaction = manager.start();
              Scan scan = new Scan(new Key(new TextValue("p1", "val")));
              transaction.scan(scan);
            })
        .isInstanceOf(CrudException.class);
  }

  @Test
  public void whenPutOperationsExecutedAndJDBCServiceThrowsSQLException_shouldThrowCrudException()
      throws Exception {
    // Arrange
    when(jdbcService.put(any(), any(), any(), any())).thenThrow(sqlException);

    // Act Assert
    assertThatThrownBy(
            () -> {
              JDBCTransaction transaction = manager.start();
              Put put =
                  new Put(new Key(new TextValue("p1", "val1")))
                      .withValue(new TextValue("v1", "val2"));
              transaction.put(put);
            })
        .isInstanceOf(CrudException.class);
  }

  @Test
  public void
      whenDeleteOperationsExecutedAndJDBCServiceThrowsSQLException_shouldThrowCrudException()
          throws Exception {
    // Arrange
    when(jdbcService.delete(any(), any(), any(), any())).thenThrow(sqlException);

    // Act Assert
    assertThatThrownBy(
            () -> {
              JDBCTransaction transaction = manager.start();
              Delete delete = new Delete(new Key(new TextValue("p1", "val1")));
              transaction.delete(delete);
            })
        .isInstanceOf(CrudException.class);
  }

  @Test
  public void
      whenMutateOperationsExecutedAndJDBCServiceThrowsSQLException_shouldThrowCrudException()
          throws Exception {
    // Arrange
    when(jdbcService.mutate(any(), any(), any(), any())).thenThrow(sqlException);

    // Act Assert
    assertThatThrownBy(
            () -> {
              JDBCTransaction transaction = manager.start();
              Put put =
                  new Put(new Key(new TextValue("p1", "val1")))
                      .withValue(new TextValue("v1", "val2"));
              Delete delete = new Delete(new Key(new TextValue("p1", "val1")));
              transaction.mutate(Arrays.asList(put, delete));
            })
        .isInstanceOf(CrudException.class);
  }

  @Test
  public void whenCommitFails_shouldThrowCommitExceptionAndRollback() throws Exception {
    // Arrange
    doThrow(sqlException).when(connection).commit();

    // Act Assert
    assertThatThrownBy(
            () -> {
              JDBCTransaction transaction = manager.start();
              Get get = new Get(new Key(new TextValue("p1", "val")));
              transaction.get(get);
              Put put =
                  new Put(new Key(new TextValue("p1", "val1")))
                      .withValue(new TextValue("v1", "val2"));
              transaction.put(put);
              transaction.commit();
            })
        .isInstanceOf(CommitException.class);
    verify(connection).rollback();
    verify(connection).close();
  }

  @Test
  public void whenRollbackFails_shouldThrowAbortException() throws Exception {
    // Arrange
    doThrow(sqlException).when(connection).rollback();

    // Act Assert
    assertThatThrownBy(
            () -> {
              JDBCTransaction transaction = manager.start();
              Get get = new Get(new Key(new TextValue("p1", "val")));
              transaction.get(get);
              Put put =
                  new Put(new Key(new TextValue("p1", "val1")))
                      .withValue(new TextValue("v1", "val2"));
              transaction.put(put);
              transaction.abort();
            })
        .isInstanceOf(AbortException.class);
    verify(connection).close();
  }

  @Test
  public void whenCommitAndRollbackFails_shouldThrowUnknownTransactionStatusException()
      throws Exception {
    // Arrange
    doThrow(sqlException).when(connection).commit();
    doThrow(sqlException).when(connection).rollback();

    // Act Assert
    assertThatThrownBy(
            () -> {
              JDBCTransaction transaction = manager.start();
              Get get = new Get(new Key(new TextValue("p1", "val")));
              transaction.get(get);
              Put put =
                  new Put(new Key(new TextValue("p1", "val1")))
                      .withValue(new TextValue("v1", "val2"));
              transaction.put(put);
              transaction.commit();
            })
        .isInstanceOf(UnknownTransactionStatusException.class);
    verify(connection).close();
  }
}
