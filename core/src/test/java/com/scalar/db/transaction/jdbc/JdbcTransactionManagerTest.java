package com.scalar.db.transaction.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TransactionCrudOperable;
import com.scalar.db.api.TransactionManagerCrudOperable;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.common.ActiveTransactionManagedDistributedTransactionManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.CommitConflictException;
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
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
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
  public void getScannerAndScannerOne_ShouldReturnScannerAndReturnProperResult() throws Exception {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    JdbcTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin();

    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val"))
            .build();

    TransactionCrudOperable.Scanner scanner = mock(TransactionCrudOperable.Scanner.class);
    when(transaction.getScanner(scan)).thenReturn(scanner);

    Result result1 = mock(Result.class);
    Result result2 = mock(Result.class);
    Result result3 = mock(Result.class);

    when(scanner.one())
        .thenReturn(Optional.of(result1))
        .thenReturn(Optional.of(result2))
        .thenReturn(Optional.of(result3))
        .thenReturn(Optional.empty());

    // Act Assert
    TransactionManagerCrudOperable.Scanner actual = spied.getScanner(scan);
    assertThat(actual.one()).hasValue(result1);
    assertThat(actual.one()).hasValue(result2);
    assertThat(actual.one()).hasValue(result3);
    assertThat(actual.one()).isEmpty();
    actual.close();

    verify(spied).begin();
    verify(transaction).commit();
    verify(scanner).close();
  }

  @Test
  public void getScannerAndScannerAll_ShouldReturnScannerAndReturnProperResults() throws Exception {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    JdbcTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin();

    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val"))
            .build();

    TransactionCrudOperable.Scanner scanner = mock(TransactionCrudOperable.Scanner.class);
    when(transaction.getScanner(scan)).thenReturn(scanner);

    Result result1 = mock(Result.class);
    Result result2 = mock(Result.class);
    Result result3 = mock(Result.class);

    when(scanner.all())
        .thenReturn(Arrays.asList(result1, result2, result3))
        .thenReturn(Collections.emptyList());

    // Act Assert
    TransactionManagerCrudOperable.Scanner actual = spied.getScanner(scan);
    List<Result> results = actual.all();
    assertThat(results).containsExactly(result1, result2, result3);
    assertThat(actual.all()).isEmpty();
    actual.close();

    verify(spied).begin();
    verify(transaction).commit();
    verify(scanner).close();
  }

  @Test
  public void getScannerAndScannerIterator_ShouldReturnScannerAndReturnProperResults()
      throws Exception {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    JdbcTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin();

    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val"))
            .build();

    TransactionCrudOperable.Scanner scanner = mock(TransactionCrudOperable.Scanner.class);
    when(transaction.getScanner(scan)).thenReturn(scanner);

    Result result1 = mock(Result.class);
    Result result2 = mock(Result.class);
    Result result3 = mock(Result.class);

    when(scanner.one())
        .thenReturn(Optional.of(result1))
        .thenReturn(Optional.of(result2))
        .thenReturn(Optional.of(result3))
        .thenReturn(Optional.empty());

    // Act Assert
    TransactionManagerCrudOperable.Scanner actual = spied.getScanner(scan);

    Iterator<Result> iterator = actual.iterator();
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isEqualTo(result1);
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isEqualTo(result2);
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isEqualTo(result3);
    assertThat(iterator.hasNext()).isFalse();
    actual.close();

    verify(spied).begin();
    verify(transaction).commit();
    verify(scanner).close();
  }

  @Test
  public void
      getScanner_TransactionNotFoundExceptionThrownByTransactionBegin_ShouldThrowCrudConflictException()
          throws TransactionException {
    // Arrange
    JdbcTransactionManager spied = spy(manager);
    doThrow(TransactionNotFoundException.class).when(spied).begin();

    Scan scan = mock(Scan.class);

    // Act Assert
    assertThatThrownBy(() -> spied.getScanner(scan)).isInstanceOf(CrudConflictException.class);

    verify(spied).begin();
  }

  @Test
  public void getScanner_TransactionExceptionThrownByTransactionBegin_ShouldThrowCrudException()
      throws TransactionException {
    // Arrange
    JdbcTransactionManager spied = spy(manager);
    doThrow(TransactionException.class).when(spied).begin();

    Scan scan = mock(Scan.class);

    // Act Assert
    assertThatThrownBy(() -> spied.getScanner(scan)).isInstanceOf(CrudException.class);

    verify(spied).begin();
  }

  @Test
  public void
      getScanner_CrudExceptionThrownByTransactionGetScanner_ShouldRollbackTransactionAndThrowCrudException()
          throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    JdbcTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin();

    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val"))
            .build();

    when(transaction.getScanner(scan)).thenThrow(CrudException.class);

    // Act Assert
    assertThatThrownBy(() -> spied.getScanner(scan)).isInstanceOf(CrudException.class);

    verify(spied).begin();
    verify(transaction).rollback();
  }

  @Test
  public void
      getScannerAndScannerOne_CrudExceptionThrownByScannerOne_ShouldRollbackTransactionAndThrowCrudException()
          throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    JdbcTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin();

    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val"))
            .build();

    TransactionCrudOperable.Scanner scanner = mock(TransactionCrudOperable.Scanner.class);
    when(transaction.getScanner(scan)).thenReturn(scanner);

    when(scanner.one()).thenThrow(CrudException.class);

    // Act Assert
    TransactionManagerCrudOperable.Scanner actual = spied.getScanner(scan);
    assertThatThrownBy(actual::one).isInstanceOf(CrudException.class);

    verify(spied).begin();
    verify(scanner).close();
    verify(transaction).rollback();
  }

  @Test
  public void
      getScannerAndScannerAll_CrudExceptionThrownByScannerAll_ShouldRollbackTransactionAndThrowCrudException()
          throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);
    JdbcTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin();

    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val"))
            .build();

    TransactionCrudOperable.Scanner scanner = mock(TransactionCrudOperable.Scanner.class);
    when(transaction.getScanner(scan)).thenReturn(scanner);

    when(scanner.all()).thenThrow(CrudException.class);

    // Act Assert
    TransactionManagerCrudOperable.Scanner actual = spied.getScanner(scan);
    assertThatThrownBy(actual::all).isInstanceOf(CrudException.class);

    verify(spied).begin();
    verify(scanner).close();
    verify(transaction).rollback();
  }

  @Test
  public void
      getScannerAndScannerClose_CrudExceptionThrownByScannerClose_ShouldRollbackTransactionAndThrowCrudException()
          throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);
    JdbcTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin();

    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val"))
            .build();

    TransactionCrudOperable.Scanner scanner = mock(TransactionCrudOperable.Scanner.class);
    when(transaction.getScanner(scan)).thenReturn(scanner);

    doThrow(CrudException.class).when(scanner).close();

    // Act Assert
    TransactionManagerCrudOperable.Scanner actual = spied.getScanner(scan);
    assertThatThrownBy(actual::close).isInstanceOf(CrudException.class);

    verify(spied).begin();
    verify(scanner).close();
    verify(transaction).rollback();
  }

  @Test
  public void
      getScannerAndScannerClose_CommitConflictExceptionThrownByTransactionCommit_ShouldRollbackTransactionAndThrowCrudConflictException()
          throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    JdbcTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin();
    doThrow(CommitConflictException.class).when(transaction).commit();

    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val"))
            .build();

    TransactionCrudOperable.Scanner scanner = mock(TransactionCrudOperable.Scanner.class);
    when(transaction.getScanner(scan)).thenReturn(scanner);

    // Act Assert
    TransactionManagerCrudOperable.Scanner actual = spied.getScanner(scan);
    assertThatThrownBy(actual::close).isInstanceOf(CrudConflictException.class);

    verify(spied).begin();
    verify(scanner).close();
    verify(transaction).rollback();
  }

  @Test
  public void
      getScannerAndScannerClose_UnknownTransactionStatusExceptionByTransactionCommit_ShouldThrowUnknownTransactionStatusException()
          throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    JdbcTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin();
    doThrow(UnknownTransactionStatusException.class).when(transaction).commit();

    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val"))
            .build();

    TransactionCrudOperable.Scanner scanner = mock(TransactionCrudOperable.Scanner.class);
    when(transaction.getScanner(scan)).thenReturn(scanner);

    // Act Assert
    TransactionManagerCrudOperable.Scanner actual = spied.getScanner(scan);
    assertThatThrownBy(actual::close).isInstanceOf(UnknownTransactionStatusException.class);

    verify(spied).begin();
    verify(scanner).close();
  }

  @Test
  public void
      getScannerAndScannerClose_CommitExceptionThrownByTransactionCommit_ShouldRollbackTransactionAndThrowCrudException()
          throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    JdbcTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin();
    doThrow(CommitException.class).when(transaction).commit();

    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofText("p1", "val"))
            .build();

    TransactionCrudOperable.Scanner scanner = mock(TransactionCrudOperable.Scanner.class);
    when(transaction.getScanner(scan)).thenReturn(scanner);

    // Act Assert
    TransactionManagerCrudOperable.Scanner actual = spied.getScanner(scan);
    assertThatThrownBy(actual::close).isInstanceOf(CrudException.class);

    verify(spied).begin();
    verify(scanner).close();
    verify(transaction).rollback();
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
    DistributedTransactionManager manager =
        new ActiveTransactionManagedDistributedTransactionManager(this.manager, -1);

    // Act Assert
    manager.begin(ANY_ID);
    assertThatThrownBy(() -> manager.begin(ANY_ID)).isInstanceOf(TransactionException.class);
  }

  @Test
  public void start_CalledTwiceWithSameTxId_ThrowTransactionException()
      throws TransactionException {
    // Arrange
    DistributedTransactionManager manager =
        new ActiveTransactionManagedDistributedTransactionManager(this.manager, -1);

    // Act Assert
    manager.start(ANY_ID);
    assertThatThrownBy(() -> manager.start(ANY_ID)).isInstanceOf(TransactionException.class);
  }

  @Test
  public void resume_CalledWithBegin_ReturnSameTransactionObject() throws TransactionException {
    // Arrange
    DistributedTransactionManager manager =
        new ActiveTransactionManagedDistributedTransactionManager(this.manager, -1);

    DistributedTransaction transaction1 = manager.begin(ANY_ID);

    // Act
    DistributedTransaction transaction2 = manager.resume(ANY_ID);

    // Assert
    assertThat(transaction1).isEqualTo(transaction2);
  }

  @Test
  public void resume_CalledWithoutBegin_ThrowTransactionNotFoundException() {
    // Arrange
    DistributedTransactionManager manager =
        new ActiveTransactionManagedDistributedTransactionManager(this.manager, -1);

    // Act Assert
    assertThatThrownBy(() -> manager.resume(ANY_ID))
        .isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void resume_CalledWithBeginAndCommit_ThrowTransactionNotFoundException()
      throws TransactionException {
    // Arrange
    DistributedTransactionManager manager =
        new ActiveTransactionManagedDistributedTransactionManager(this.manager, -1);

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
    DistributedTransactionManager manager =
        new ActiveTransactionManagedDistributedTransactionManager(this.manager, -1);

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
    DistributedTransactionManager manager =
        new ActiveTransactionManagedDistributedTransactionManager(this.manager, -1);

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
    DistributedTransactionManager manager =
        new ActiveTransactionManagedDistributedTransactionManager(this.manager, -1);

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
    DistributedTransactionManager manager =
        new ActiveTransactionManagedDistributedTransactionManager(this.manager, -1);

    DistributedTransaction transaction1 = manager.begin(ANY_ID);

    // Act
    DistributedTransaction transaction2 = manager.join(ANY_ID);

    // Assert
    assertThat(transaction1).isEqualTo(transaction2);
  }

  @Test
  public void join_CalledWithoutBegin_ThrowTransactionNotFoundException() {
    // Arrange
    DistributedTransactionManager manager =
        new ActiveTransactionManagedDistributedTransactionManager(this.manager, -1);

    // Act Assert
    assertThatThrownBy(() -> manager.join(ANY_ID)).isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void join_CalledWithBeginAndCommit_ThrowTransactionNotFoundException()
      throws TransactionException {
    // Arrange
    DistributedTransactionManager manager =
        new ActiveTransactionManagedDistributedTransactionManager(this.manager, -1);

    DistributedTransaction transaction = manager.begin(ANY_ID);
    transaction.commit();

    // Act Assert
    assertThatThrownBy(() -> manager.join(ANY_ID)).isInstanceOf(TransactionNotFoundException.class);
  }

  @Test
  public void join_CalledWithBeginAndCommit_CommitExceptionThrown_ReturnSameTransactionObject()
      throws TransactionException, SQLException {
    // Arrange
    DistributedTransactionManager manager =
        new ActiveTransactionManagedDistributedTransactionManager(this.manager, -1);

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
    DistributedTransactionManager manager =
        new ActiveTransactionManagedDistributedTransactionManager(this.manager, -1);

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
    DistributedTransactionManager manager =
        new ActiveTransactionManagedDistributedTransactionManager(this.manager, -1);

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

  @Test
  public void get_ShouldGet() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    JdbcTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin(any());

    Get get =
        Get.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("pk", 0)).build();

    Result result = mock(Result.class);
    when(transaction.get(get)).thenReturn(Optional.of(result));

    // Act
    Optional<Result> actual = spied.get(get);

    // Assert
    verify(spied).begin(any());
    verify(transaction).get(get);
    verify(transaction).commit();
    assertThat(actual).isEqualTo(Optional.of(result));
  }

  @Test
  public void
      get_TransactionNotFoundExceptionThrownByTransactionBegin_ShouldThrowCrudConflictException()
          throws TransactionException {
    // Arrange
    JdbcTransactionManager spied = spy(manager);
    doThrow(TransactionNotFoundException.class).when(spied).begin(any());

    Get get =
        Get.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("pk", 0)).build();

    // Act Assert
    assertThatThrownBy(() -> spied.get(get)).isInstanceOf(CrudConflictException.class);

    verify(spied).begin(any());
  }

  @Test
  public void get_TransactionExceptionThrownByTransactionBegin_ShouldThrowCrudException()
      throws TransactionException {
    // Arrange
    JdbcTransactionManager spied = spy(manager);
    doThrow(TransactionException.class).when(spied).begin(any());

    Get get =
        Get.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("pk", 0)).build();

    // Act Assert
    assertThatThrownBy(() -> spied.get(get)).isInstanceOf(CrudException.class);

    verify(spied).begin(any());
  }

  @Test
  public void get_CrudExceptionThrownByTransactionGet_ShouldThrowCrudException()
      throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    JdbcTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin(any());

    Get get =
        Get.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("pk", 0)).build();
    when(transaction.get(get)).thenThrow(CrudException.class);

    // Act Assert
    assertThatThrownBy(() -> spied.get(get)).isInstanceOf(CrudException.class);

    verify(spied).begin(any());
    verify(transaction).get(get);
    verify(transaction).rollback();
  }

  @Test
  public void
      get_CommitConflictExceptionThrownByTransactionCommit_ShouldThrowCrudConflictException()
          throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    JdbcTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin(any());

    Get get =
        Get.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("pk", 0)).build();
    doThrow(CommitConflictException.class).when(transaction).commit();

    // Act Assert
    assertThatThrownBy(() -> spied.get(get)).isInstanceOf(CrudConflictException.class);

    verify(spied).begin(any());
    verify(transaction).get(get);
    verify(transaction).rollback();
  }

  @Test
  public void
      get_UnknownTransactionStatusExceptionThrownByTransactionCommit_ShouldThrowUnknownTransactionStatusException()
          throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    JdbcTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin(any());

    Get get =
        Get.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("pk", 0)).build();
    doThrow(UnknownTransactionStatusException.class).when(transaction).commit();

    // Act Assert
    assertThatThrownBy(() -> spied.get(get)).isInstanceOf(UnknownTransactionStatusException.class);

    verify(spied).begin(any());
    verify(transaction).get(get);
    verify(transaction).commit();
  }

  @Test
  public void get_CommitExceptionThrownByTransactionCommit_ShouldThrowUCrudException()
      throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    JdbcTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin(any());

    Get get =
        Get.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("pk", 0)).build();
    doThrow(CommitException.class).when(transaction).commit();

    // Act Assert
    assertThatThrownBy(() -> spied.get(get)).isInstanceOf(CrudException.class);

    verify(spied).begin(any());
    verify(transaction).get(get);
    verify(transaction).commit();
  }

  @Test
  public void scan_ShouldScan() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    JdbcTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin(any());

    Scan scan =
        Scan.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("pk", 0)).build();

    List<Result> results =
        Arrays.asList(mock(Result.class), mock(Result.class), mock(Result.class));
    when(transaction.scan(scan)).thenReturn(results);

    // Act
    List<Result> actual = spied.scan(scan);

    // Assert
    verify(spied).begin(any());
    verify(transaction).scan(scan);
    verify(transaction).commit();
    assertThat(actual).isEqualTo(results);
  }

  @Test
  public void put_ShouldPut() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    JdbcTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin(any());

    Put put =
        Put.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("pk", 0))
            .intValue("col", 0)
            .build();

    // Act
    spied.put(put);

    // Assert
    verify(spied).begin(any());
    verify(transaction).put(put);
    verify(transaction).commit();
  }

  @Test
  public void put_MultiplePutsGiven_ShouldPut() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    JdbcTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin(any());

    List<Put> puts =
        Arrays.asList(
            Put.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.ofInt("pk", 0))
                .intValue("col", 0)
                .build(),
            Put.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.ofInt("pk", 1))
                .intValue("col", 1)
                .build(),
            Put.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.ofInt("pk", 2))
                .intValue("col", 2)
                .build());

    // Act
    spied.put(puts);

    // Assert
    verify(spied).begin(any());
    verify(transaction).put(puts);
    verify(transaction).commit();
  }

  @Test
  public void insert_ShouldInsert() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    JdbcTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin(any());

    Insert insert =
        Insert.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("pk", 0))
            .intValue("col", 0)
            .build();

    // Act
    spied.insert(insert);

    // Assert
    verify(spied).begin(any());
    verify(transaction).insert(insert);
    verify(transaction).commit();
  }

  @Test
  public void upsert_ShouldUpsert() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    JdbcTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin(any());

    Upsert upsert =
        Upsert.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("pk", 0))
            .intValue("col", 0)
            .build();

    // Act
    spied.upsert(upsert);

    // Assert
    verify(spied).begin(any());
    verify(transaction).upsert(upsert);
    verify(transaction).commit();
  }

  @Test
  public void update_ShouldUpdate() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    JdbcTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin(any());

    Update update =
        Update.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("pk", 0))
            .intValue("col", 0)
            .build();

    // Act
    spied.update(update);

    // Assert
    verify(spied).begin(any());
    verify(transaction).update(update);
    verify(transaction).commit();
  }

  @Test
  public void delete_ShouldDelete() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    JdbcTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin(any());

    Delete delete =
        Delete.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("pk", 0)).build();

    // Act
    spied.delete(delete);

    // Assert
    verify(spied).begin(any());
    verify(transaction).delete(delete);
    verify(transaction).commit();
  }

  @Test
  public void delete_MultipleDeletesGiven_ShouldDelete() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    JdbcTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin(any());

    List<Delete> deletes =
        Arrays.asList(
            Delete.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.ofInt("pk", 0))
                .build(),
            Delete.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.ofInt("pk", 1))
                .build(),
            Delete.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.ofInt("pk", 2))
                .build());

    // Act
    spied.delete(deletes);

    // Assert
    verify(spied).begin(any());
    verify(transaction).delete(deletes);
    verify(transaction).commit();
  }

  @Test
  public void mutate_ShouldMutate() throws TransactionException {
    // Arrange
    DistributedTransaction transaction = mock(DistributedTransaction.class);

    JdbcTransactionManager spied = spy(manager);
    doReturn(transaction).when(spied).begin(any());

    List<Mutation> mutations =
        Arrays.asList(
            Put.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.ofInt("pk", 0))
                .intValue("col", 0)
                .build(),
            Insert.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.ofInt("pk", 1))
                .intValue("col", 1)
                .build(),
            Upsert.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.ofInt("pk", 2))
                .intValue("col", 2)
                .build(),
            Update.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.ofInt("pk", 3))
                .intValue("col", 3)
                .build(),
            Delete.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.ofInt("pk", 4))
                .build());

    // Act
    spied.mutate(mutations);

    // Assert
    verify(spied).begin(any());
    verify(transaction).mutate(mutations);
    verify(transaction).commit();
  }
}
