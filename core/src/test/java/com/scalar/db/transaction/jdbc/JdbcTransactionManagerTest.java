package com.scalar.db.transaction.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.io.Key;
import com.scalar.db.storage.jdbc.JdbcService;
import com.scalar.db.storage.jdbc.RdbEngine;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class JdbcTransactionManagerTest {

  private static final String NAMESPACE = "ns";
  private static final String TABLE = "tbl";
  private static final String ANY_ID = "id";

  @Mock private DatabaseConfig databaseConfig;
  @Mock private BasicDataSource dataSource;
  @Mock private BasicDataSource tableMetadataDataSource;
  @Mock private JdbcService jdbcService;
  @Mock private Connection connection;
  @Mock private SQLException sqlException;

  private JdbcTransactionManager manager;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    when(dataSource.getConnection()).thenReturn(connection);
    manager =
        new JdbcTransactionManager(
            databaseConfig,
            dataSource,
            tableMetadataDataSource,
            RdbEngine.createRdbEngineStrategy(RdbEngine.MYSQL),
            jdbcService);
  }

  @Test
  public void whenSomeOperationsExecutedAndCommit_shouldCallJdbcService() throws Exception {
    // Arrange
    when(jdbcService.scan(any(), any())).thenReturn(Collections.emptyList());
    when(jdbcService.put(any(), any())).thenReturn(true);
    when(jdbcService.delete(any(), any())).thenReturn(true);

    // Act
    DistributedTransaction transaction = manager.begin();
    Get get = new Get(new Key("p1", "val")).forNamespace(NAMESPACE).forTable(TABLE);
    transaction.get(get);
    Scan scan = new Scan(new Key("p1", "val")).forNamespace(NAMESPACE).forTable(TABLE);
    transaction.scan(scan);
    Put put =
        new Put(new Key("p1", "val1"))
            .withValue("v1", "val2")
            .forNamespace(NAMESPACE)
            .forTable(TABLE);
    transaction.put(put);
    Delete delete = new Delete(new Key("p1", "val1")).forNamespace(NAMESPACE).forTable(TABLE);
    transaction.delete(delete);
    transaction.mutate(Arrays.asList(put, delete));
    transaction.commit();

    // Assert
    verify(jdbcService).get(any(), any());
    verify(jdbcService).scan(any(), any());
    verify(jdbcService, times(2)).put(any(), any());
    verify(jdbcService, times(2)).delete(any(), any());
    verify(connection).commit();
    verify(connection).close();
  }

  @Test
  public void whenGetOperationsExecutedAndJdbcServiceThrowsSQLException_shouldThrowCrudException()
      throws Exception {
    // Arrange
    when(jdbcService.get(any(), any())).thenThrow(sqlException);

    // Act Assert
    assertThatThrownBy(
            () -> {
              DistributedTransaction transaction = manager.start();
              Get get = new Get(new Key("p1", "val")).forNamespace(NAMESPACE).forTable(TABLE);
              transaction.get(get);
            })
        .isInstanceOf(CrudException.class);
  }

  @Test
  public void get_withConflictError_shouldThrowCrudConflictException()
      throws SQLException, ExecutionException {
    // Arrange
    when(jdbcService.get(any(), any())).thenThrow(sqlException);
    when(sqlException.getErrorCode()).thenReturn(1213);

    // Act Assert
    assertThatThrownBy(
            () -> {
              DistributedTransaction transaction = manager.begin();
              Get get = new Get(new Key("p1", "val")).forNamespace(NAMESPACE).forTable(TABLE);
              transaction.get(get);
            })
        .isInstanceOf(CrudConflictException.class);
  }

  @Test
  public void whenScanOperationsExecutedAndJdbcServiceThrowsSQLException_shouldThrowCrudException()
      throws Exception {
    // Arrange
    when(jdbcService.scan(any(), any())).thenThrow(sqlException);

    // Act Assert
    assertThatThrownBy(
            () -> {
              DistributedTransaction transaction = manager.start();
              Scan scan = new Scan(new Key("p1", "val")).forNamespace(NAMESPACE).forTable(TABLE);
              transaction.scan(scan);
            })
        .isInstanceOf(CrudException.class);
  }

  @Test
  public void scan_withConflictError_shouldThrowCrudConflictException()
      throws SQLException, ExecutionException {
    // Arrange
    when(jdbcService.scan(any(), any())).thenThrow(sqlException);
    when(sqlException.getErrorCode()).thenReturn(1213);

    // Act Assert
    assertThatThrownBy(
            () -> {
              DistributedTransaction transaction = manager.begin();
              Scan scan = new Scan(new Key("p1", "val")).forNamespace(NAMESPACE).forTable(TABLE);
              transaction.scan(scan);
            })
        .isInstanceOf(CrudConflictException.class);
  }

  @Test
  public void whenPutOperationsExecutedAndJdbcServiceThrowsSQLException_shouldThrowCrudException()
      throws Exception {
    // Arrange
    when(jdbcService.put(any(), any())).thenThrow(sqlException);

    // Act Assert
    assertThatThrownBy(
            () -> {
              DistributedTransaction transaction = manager.start();
              Put put =
                  new Put(new Key("p1", "val1"))
                      .withValue("v1", "val2")
                      .forNamespace(NAMESPACE)
                      .forTable(TABLE);
              transaction.put(put);
            })
        .isInstanceOf(CrudException.class);
  }

  @Test
  public void put_withConflictError_shouldThrowCrudConflictException()
      throws SQLException, ExecutionException {
    // Arrange
    when(jdbcService.put(any(), any())).thenThrow(sqlException);
    when(sqlException.getErrorCode()).thenReturn(1213);

    // Act Assert
    assertThatThrownBy(
            () -> {
              DistributedTransaction transaction = manager.begin();
              Put put =
                  new Put(new Key("p1", "val1"))
                      .withValue("v1", "val2")
                      .forNamespace(NAMESPACE)
                      .forTable(TABLE);
              transaction.put(put);
            })
        .isInstanceOf(CrudConflictException.class);
  }

  @Test
  public void
      whenDeleteOperationsExecutedAndJdbcServiceThrowsSQLException_shouldThrowCrudException()
          throws Exception {
    // Arrange
    when(jdbcService.delete(any(), any())).thenThrow(sqlException);

    // Act Assert
    assertThatThrownBy(
            () -> {
              DistributedTransaction transaction = manager.start();
              Delete delete =
                  new Delete(new Key("p1", "val1")).forNamespace(NAMESPACE).forTable(TABLE);
              transaction.delete(delete);
            })
        .isInstanceOf(CrudException.class);
  }

  @Test
  public void delete_withConflictError_shouldThrowCrudConflictException()
      throws SQLException, ExecutionException {
    // Arrange
    when(jdbcService.delete(any(), any())).thenThrow(sqlException);
    when(sqlException.getErrorCode()).thenReturn(1213);

    // Act Assert
    assertThatThrownBy(
            () -> {
              DistributedTransaction transaction = manager.begin();
              Delete delete =
                  new Delete(new Key("p1", "val1")).forNamespace(NAMESPACE).forTable(TABLE);
              transaction.delete(delete);
            })
        .isInstanceOf(CrudConflictException.class);
  }

  @Test
  public void
      whenMutateOperationsExecutedAndJdbcServiceThrowsSQLException_shouldThrowCrudException()
          throws Exception {
    // Arrange
    when(jdbcService.put(any(), any())).thenThrow(sqlException);

    // Act Assert
    assertThatThrownBy(
            () -> {
              DistributedTransaction transaction = manager.start();
              Put put =
                  new Put(new Key("p1", "val1"))
                      .withValue("v1", "val2")
                      .forNamespace(NAMESPACE)
                      .forTable(TABLE);
              Delete delete =
                  new Delete(new Key("p1", "val1")).forNamespace(NAMESPACE).forTable(TABLE);
              transaction.mutate(Arrays.asList(put, delete));
            })
        .isInstanceOf(CrudException.class);
  }

  @Test
  public void mutate_withConflictError_shouldThrowCrudConflictException()
      throws SQLException, ExecutionException {
    // Arrange
    when(jdbcService.put(any(), any())).thenThrow(sqlException);
    when(sqlException.getErrorCode()).thenReturn(1213);

    // Act Assert
    assertThatThrownBy(
            () -> {
              DistributedTransaction transaction = manager.begin();
              Put put =
                  new Put(new Key("p1", "val1"))
                      .withValue("v1", "val2")
                      .forNamespace(NAMESPACE)
                      .forTable(TABLE);
              Delete delete =
                  new Delete(new Key("p1", "val1")).forNamespace(NAMESPACE).forTable(TABLE);
              transaction.mutate(Arrays.asList(put, delete));
            })
        .isInstanceOf(CrudConflictException.class);
  }

  @Test
  public void whenCommitFails_shouldThrowCommitExceptionAndRollback() throws Exception {
    // Arrange
    when(jdbcService.put(any(), any())).thenReturn(true);
    doThrow(sqlException).when(connection).commit();

    // Act Assert
    assertThatThrownBy(
            () -> {
              DistributedTransaction transaction = manager.start();
              Get get = new Get(new Key("p1", "val")).forNamespace(NAMESPACE).forTable(TABLE);
              transaction.get(get);
              Put put =
                  new Put(new Key("p1", "val1"))
                      .withValue("v1", "val2")
                      .forNamespace(NAMESPACE)
                      .forTable(TABLE);
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
    when(jdbcService.put(any(), any())).thenReturn(true);
    doThrow(sqlException).when(connection).rollback();

    // Act Assert
    assertThatThrownBy(
            () -> {
              DistributedTransaction transaction = manager.begin();
              Get get = new Get(new Key("p1", "val")).forNamespace(NAMESPACE).forTable(TABLE);
              transaction.get(get);
              Put put =
                  new Put(new Key("p1", "val1"))
                      .withValue("v1", "val2")
                      .forNamespace(NAMESPACE)
                      .forTable(TABLE);
              transaction.put(put);
              transaction.abort();
            })
        .isInstanceOf(AbortException.class);
    verify(connection).close();
  }

  @Test
  public void whenRollbackFails_shouldThrowRollbackException() throws Exception {
    // Arrange
    when(jdbcService.put(any(), any())).thenReturn(true);
    doThrow(sqlException).when(connection).rollback();

    // Act Assert
    assertThatThrownBy(
            () -> {
              DistributedTransaction transaction = manager.begin();
              Get get = new Get(new Key("p1", "val")).forNamespace(NAMESPACE).forTable(TABLE);
              transaction.get(get);
              Put put =
                  new Put(new Key("p1", "val1"))
                      .withValue("v1", "val2")
                      .forNamespace(NAMESPACE)
                      .forTable(TABLE);
              transaction.put(put);
              transaction.rollback();
            })
        .isInstanceOf(RollbackException.class);
    verify(connection).close();
  }

  @Test
  public void whenCommitAndRollbackFails_shouldThrowUnknownTransactionStatusException()
      throws Exception {
    // Arrange
    when(jdbcService.put(any(), any())).thenReturn(true);
    doThrow(sqlException).when(connection).commit();
    doThrow(sqlException).when(connection).rollback();

    // Act Assert
    assertThatThrownBy(
            () -> {
              DistributedTransaction transaction = manager.start();
              Get get = new Get(new Key("p1", "val")).forNamespace(NAMESPACE).forTable(TABLE);
              transaction.get(get);
              Put put =
                  new Put(new Key("p1", "val1"))
                      .withValue("v1", "val2")
                      .forNamespace(NAMESPACE)
                      .forTable(TABLE);
              transaction.put(put);
              transaction.commit();
            })
        .isInstanceOf(UnknownTransactionStatusException.class);
    verify(connection).close();
  }

  @Test
  public void begin_CalledTwiceWithSameTxId_ThrowTransactionException()
      throws TransactionException {
    // Arrange

    // Act Assert
    manager.begin(ANY_ID);
    assertThatThrownBy(() -> manager.begin(ANY_ID)).isInstanceOf(TransactionException.class);
  }

  @Test
  public void start_CalledTwiceWithSameTxId_ThrowTransactionException()
      throws TransactionException {
    // Arrange

    // Act Assert
    manager.start(ANY_ID);
    assertThatThrownBy(() -> manager.start(ANY_ID)).isInstanceOf(TransactionException.class);
  }

  @Test
  public void resume_CalledWithBegin_ReturnSameTransactionObject() throws TransactionException {
    // Arrange
    DistributedTransaction transaction1 = manager.begin(ANY_ID);

    // Act
    DistributedTransaction transaction2 = manager.resume(ANY_ID);

    // Assert
    assertThat(transaction1).isEqualTo(transaction2);
  }

  @Test
  public void resume_CalledWithoutBegin_ThrowTransactionNotFoundException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> manager.resume(ANY_ID))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void resume_CalledWithBeginAndCommit_ThrowTransactionNotFoundException()
      throws TransactionException {
    // Arrange
    DistributedTransaction transaction = manager.begin(ANY_ID);
    transaction.commit();

    // Act Assert
    assertThatThrownBy(() -> manager.resume(ANY_ID))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void resume_CalledWithBeginAndCommit_CommitExceptionThrown_ReturnSameTransactionObject()
      throws TransactionException, SQLException {
    // Arrange
    doThrow(SQLException.class).when(connection).commit();

    DistributedTransaction transaction1 = manager.begin(ANY_ID);
    try {
      transaction1.commit();
    } catch (CommitException ignored) {
      // expected
    }

    // Act
    DistributedTransaction transaction2 = manager.resume(ANY_ID);

    // Assert
    assertThat(transaction1).isEqualTo(transaction2);
  }

  @Test
  public void resume_CalledWithBeginAndRollback_ThrowTransactionNotFoundException()
      throws TransactionException {
    // Arrange
    DistributedTransaction transaction = manager.begin(ANY_ID);
    transaction.rollback();

    // Act Assert
    assertThatThrownBy(() -> manager.resume(ANY_ID))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void
      resume_CalledWithBeginAndRollback_RollbackExceptionThrown_ThrowTransactionNotFoundException()
          throws TransactionException, SQLException {
    // Arrange
    doThrow(SQLException.class).when(connection).rollback();

    DistributedTransaction transaction1 = manager.begin(ANY_ID);
    try {
      transaction1.rollback();
    } catch (RollbackException ignored) {
      // expected
    }

    // Act Assert
    assertThatThrownBy(() -> manager.resume(ANY_ID))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void join_CalledWithBegin_ReturnSameTransactionObject() throws TransactionException {
    // Arrange
    DistributedTransaction transaction1 = manager.begin(ANY_ID);

    // Act
    DistributedTransaction transaction2 = manager.join(ANY_ID);

    // Assert
    assertThat(transaction1).isEqualTo(transaction2);
  }

  @Test
  public void join_CalledWithoutBegin_ThrowTransactionNotFoundException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> manager.join(ANY_ID)).isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void join_CalledWithBeginAndCommit_ThrowTransactionNotFoundException()
      throws TransactionException {
    // Arrange
    DistributedTransaction transaction = manager.begin(ANY_ID);
    transaction.commit();

    // Act Assert
    assertThatThrownBy(() -> manager.join(ANY_ID)).isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void join_CalledWithBeginAndCommit_CommitExceptionThrown_ReturnSameTransactionObject()
      throws TransactionException, SQLException {
    // Arrange
    doThrow(SQLException.class).when(connection).commit();

    DistributedTransaction transaction1 = manager.begin(ANY_ID);
    try {
      transaction1.commit();
    } catch (CommitException ignored) {
      // expected
    }

    // Act
    DistributedTransaction transaction2 = manager.join(ANY_ID);

    // Assert
    assertThat(transaction1).isEqualTo(transaction2);
  }

  @Test
  public void join_CalledWithBeginAndRollback_ThrowTransactionNotFoundException()
      throws TransactionException {
    // Arrange
    DistributedTransaction transaction = manager.begin(ANY_ID);
    transaction.rollback();

    // Act Assert
    assertThatThrownBy(() -> manager.join(ANY_ID)).isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void
      join_CalledWithBeginAndRollback_RollbackExceptionThrown_ThrowTransactionNotFoundException()
          throws TransactionException, SQLException {
    // Arrange
    doThrow(SQLException.class).when(connection).rollback();

    DistributedTransaction transaction1 = manager.begin(ANY_ID);
    try {
      transaction1.rollback();
    } catch (RollbackException ignored) {
      // expected
    }

    // Act Assert
    assertThatThrownBy(() -> manager.join(ANY_ID)).isInstanceOf(TransactionNotFoundException.class);
  }
}
