package com.scalar.db.transaction.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.CrudOperable;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.MutationCondition;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TransactionCrudOperable;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.UnsatisfiedConditionException;
import com.scalar.db.io.Key;
import com.scalar.db.storage.jdbc.FailoverSimulatingDriver;
import com.scalar.db.storage.jdbc.JdbcCrudService;
import com.scalar.db.storage.jdbc.RdbEngineStrategy;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class JdbcTransactionTest {
  private static final String ANY_NAMESPACE = "namespace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_TEXT_3 = "text3";
  private static final String ANY_TEXT_4 = "text4";
  private static final String ANY_TX_ID = "any_id";

  private static final Put ANY_PUT =
      Put.newBuilder()
          .namespace("ns")
          .table("tbl")
          .partitionKey(Key.ofText("c1", "foo"))
          .condition(ConditionBuilder.putIfExists())
          .build();
  private static final Delete ANY_DELETE =
      Delete.newBuilder()
          .namespace("ns")
          .table("tbl")
          .partitionKey(Key.ofText("c1", "foo"))
          .condition(ConditionBuilder.deleteIfExists())
          .build();

  private JdbcTransaction transaction;
  @Mock private JdbcCrudService jdbcCrudService;
  @Mock private Connection connection;
  @Mock private RdbEngineStrategy rdbEngineStrategy;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    transaction = new JdbcTransaction(ANY_TX_ID, jdbcCrudService, connection, rdbEngineStrategy);
  }

  @Test
  public void getScannerAndScannerOne_ShouldReturnScannerAndShouldReturnProperResult()
      throws SQLException, ExecutionException, CrudException, IOException {
    // Arrange
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText("p1", "val"))
            .build();

    Result result1 = mock(Result.class);
    Result result2 = mock(Result.class);
    Result result3 = mock(Result.class);

    Scanner scanner = mock(Scanner.class);
    when(scanner.one())
        .thenReturn(Optional.of(result1))
        .thenReturn(Optional.of(result2))
        .thenReturn(Optional.of(result3))
        .thenReturn(Optional.empty());

    when(jdbcCrudService.getScanner(scan, connection, false)).thenReturn(scanner);

    // Act Assert
    TransactionCrudOperable.Scanner actual = transaction.getScanner(scan);
    assertThat(actual.one()).hasValue(result1);
    assertThat(actual.one()).hasValue(result2);
    assertThat(actual.one()).hasValue(result3);
    assertThat(actual.one()).isEmpty();
    actual.close();

    verify(jdbcCrudService).getScanner(scan, connection, false);
    verify(scanner).close();
  }

  @Test
  public void getScannerAndScannerAll_ShouldReturnScannerAndShouldReturnProperResults()
      throws SQLException, ExecutionException, CrudException, IOException {
    // Arrange
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText("p1", "val"))
            .build();

    Result result1 = mock(Result.class);
    Result result2 = mock(Result.class);
    Result result3 = mock(Result.class);

    Scanner scanner = mock(Scanner.class);
    when(scanner.all()).thenReturn(Arrays.asList(result1, result2, result3));

    when(jdbcCrudService.getScanner(scan, connection, false)).thenReturn(scanner);

    // Act Assert
    TransactionCrudOperable.Scanner actual = transaction.getScanner(scan);
    assertThat(actual.all()).containsExactly(result1, result2, result3);
    actual.close();

    verify(jdbcCrudService).getScanner(scan, connection, false);
    verify(scanner).close();
  }

  @Test
  public void getScannerAndScannerIterator_ShouldReturnScannerAndShouldReturnProperResults()
      throws SQLException, ExecutionException, CrudException, IOException {
    // Arrange
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText("p1", "val"))
            .build();

    Result result1 = mock(Result.class);
    Result result2 = mock(Result.class);
    Result result3 = mock(Result.class);

    Scanner scanner = mock(Scanner.class);
    when(scanner.one())
        .thenReturn(Optional.of(result1))
        .thenReturn(Optional.of(result2))
        .thenReturn(Optional.of(result3))
        .thenReturn(Optional.empty());

    when(jdbcCrudService.getScanner(scan, connection, false)).thenReturn(scanner);

    // Act Assert
    TransactionCrudOperable.Scanner actual = transaction.getScanner(scan);

    Iterator<Result> iterator = actual.iterator();
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isEqualTo(result1);
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isEqualTo(result2);
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isEqualTo(result3);
    assertThat(iterator.hasNext()).isFalse();
    actual.close();

    verify(jdbcCrudService).getScanner(scan, connection, false);
    verify(scanner).close();
  }

  @Test
  public void getScanner_WhenSQLExceptionThrownByJdbcCrudService_ShouldThrowCrudException()
      throws SQLException, ExecutionException {
    // Arrange
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText("p1", "val"))
            .build();

    when(jdbcCrudService.getScanner(scan, connection, false)).thenThrow(SQLException.class);

    // Act Assert
    assertThatThrownBy(() -> transaction.getScanner(scan)).isInstanceOf(CrudException.class);
  }

  @Test
  public void getScanner_WhenExecutionExceptionThrownByJdbcCrudService_ShouldThrowCrudException()
      throws SQLException, ExecutionException {
    // Arrange
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText("p1", "val"))
            .build();

    ExecutionException executionException = mock(ExecutionException.class);
    when(executionException.getMessage()).thenReturn("error");
    when(jdbcCrudService.getScanner(scan, connection, false)).thenThrow(executionException);

    // Act Assert
    assertThatThrownBy(() -> transaction.getScanner(scan)).isInstanceOf(CrudException.class);
  }

  @Test
  public void
      getScannerAndScannerOne_WhenExecutionExceptionThrownByScannerOne_ShouldThrowCrudException()
          throws SQLException, ExecutionException, CrudException {
    // Arrange
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText("p1", "val"))
            .build();

    Scanner scanner = mock(Scanner.class);

    ExecutionException executionException = mock(ExecutionException.class);
    when(executionException.getMessage()).thenReturn("error");
    when(scanner.one()).thenThrow(executionException);

    when(jdbcCrudService.getScanner(scan, connection, false)).thenReturn(scanner);

    // Act Assert
    TransactionCrudOperable.Scanner actual = transaction.getScanner(scan);
    assertThatThrownBy(actual::one).isInstanceOf(CrudException.class);
  }

  @Test
  public void
      getScannerAndScannerAll_WhenExecutionExceptionThrownByScannerAll_ShouldThrowCrudException()
          throws SQLException, ExecutionException, CrudException {
    // Arrange
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText("p1", "val"))
            .build();

    Scanner scanner = mock(Scanner.class);

    ExecutionException executionException = mock(ExecutionException.class);
    when(executionException.getMessage()).thenReturn("error");
    when(scanner.all()).thenThrow(executionException);

    when(jdbcCrudService.getScanner(scan, connection, false)).thenReturn(scanner);

    // Act Assert
    TransactionCrudOperable.Scanner actual = transaction.getScanner(scan);
    assertThatThrownBy(actual::all).isInstanceOf(CrudException.class);
  }

  @Test
  public void
      getScannerAndScannerClose_WhenIOExceptionThrownByScannerClose_ShouldThrowCrudException()
          throws SQLException, ExecutionException, CrudException, IOException {
    // Arrange
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText("p1", "val"))
            .build();

    Scanner scanner = mock(Scanner.class);

    IOException ioException = mock(IOException.class);
    when(ioException.getMessage()).thenReturn("error");
    doThrow(ioException).when(scanner).close();

    when(jdbcCrudService.getScanner(scan, connection, false)).thenReturn(scanner);

    // Act Assert
    TransactionCrudOperable.Scanner actual = transaction.getScanner(scan);
    assertThatThrownBy(actual::close).isInstanceOf(CrudException.class);
  }

  @Test
  public void put_putDoesNotSucceed_shouldThrowUnsatisfiedConditionException()
      throws SQLException, ExecutionException {
    // Arrange
    when(jdbcCrudService.put(any(), any())).thenReturn(false);

    // Act Assert
    assertThatThrownBy(() -> transaction.put(ANY_PUT))
        .isInstanceOf(UnsatisfiedConditionException.class);
    verify(jdbcCrudService).put(ANY_PUT, connection);
  }

  @Test
  public void put_putSucceed_shouldThrowUnsatisfiedConditionException()
      throws SQLException, ExecutionException {
    // Arrange
    when(jdbcCrudService.put(any(), any())).thenReturn(true);

    // Act Assert
    assertThatCode(() -> transaction.put(ANY_PUT)).doesNotThrowAnyException();
    verify(jdbcCrudService).put(ANY_PUT, connection);
  }

  @Test
  public void delete_deleteDoesNotSucceed_shouldThrowUnsatisfiedConditionException()
      throws SQLException, ExecutionException {
    // Arrange
    when(jdbcCrudService.delete(any(), any())).thenReturn(false);

    // Act Assert
    assertThatThrownBy(() -> transaction.delete(ANY_DELETE))
        .isInstanceOf(UnsatisfiedConditionException.class);
    verify(jdbcCrudService).delete(ANY_DELETE, connection);
  }

  @Test
  public void delete_deleteSucceed_shouldThrowUnsatisfiedConditionException()
      throws SQLException, ExecutionException {
    // Arrange
    when(jdbcCrudService.delete(any(), any())).thenReturn(true);

    // Act Assert
    assertThatCode(() -> transaction.delete(ANY_DELETE)).doesNotThrowAnyException();
    verify(jdbcCrudService).delete(ANY_DELETE, connection);
  }

  @ParameterizedTest
  @MethodSource("provideConditionalPuts")
  public void put_unsatisfiedConditionExceptionThrown_shouldContainsAppropriateMessage(
      MutationCondition condition, String exceptionMessage)
      throws SQLException, ExecutionException {
    // Arrange
    when(jdbcCrudService.put(any(), any())).thenReturn(false);
    Put put1 = Put.newBuilder(ANY_PUT).condition(condition).build();

    // Act Assert
    assertThatThrownBy(() -> transaction.put(put1))
        .isInstanceOf(UnsatisfiedConditionException.class)
        .hasMessageContaining(ANY_TX_ID)
        .hasMessageContaining(exceptionMessage);
    verify(jdbcCrudService).put(put1, connection);
  }

  private static Stream<Arguments> provideConditionalPuts() {
    return Stream.of(
        Arguments.of(
            ConditionBuilder.putIf(ConditionBuilder.column("c1").isNullText()).build(),
            "The PutIf condition of the Put operation is not satisfied. Targeting column(s): [c1]"),
        Arguments.of(
            ConditionBuilder.putIf(ConditionBuilder.column("c1").isNullText())
                .and(ConditionBuilder.column("c2").isEqualToText("a"))
                .build(),
            "The PutIf condition of the Put operation is not satisfied. Targeting column(s): [c1, c2]"),
        Arguments.of(
            ConditionBuilder.putIfExists(),
            "The PutIfExists condition of the Put operation is not satisfied. Targeting column(s): null"),
        Arguments.of(
            ConditionBuilder.putIfNotExists(),
            "The PutIfNotExists condition of the Put operation is not satisfied. Targeting column(s): null"));
  }

  @ParameterizedTest
  @MethodSource("provideConditionalDeletes")
  public void delete_whenUnsatisfiedConditionExceptionThrown_shouldContainsAppropriateMessage(
      MutationCondition condition, String exceptionMessage)
      throws SQLException, ExecutionException {
    // Arrange
    when(jdbcCrudService.delete(any(), any())).thenReturn(false);
    Delete delete1 = Delete.newBuilder(ANY_DELETE).condition(condition).build();

    // Act Assert
    assertThatThrownBy(() -> transaction.delete(delete1))
        .isInstanceOf(UnsatisfiedConditionException.class)
        .hasMessageContaining(ANY_TX_ID)
        .hasMessageContaining(exceptionMessage);
    verify(jdbcCrudService).delete(delete1, connection);
  }

  private static Stream<Arguments> provideConditionalDeletes() {
    return Stream.of(
        Arguments.of(
            ConditionBuilder.deleteIf(ConditionBuilder.column("c1").isNullText()).build(),
            "The DeleteIf condition of the Delete operation is not satisfied. Targeting column(s): [c1]"),
        Arguments.of(
            ConditionBuilder.deleteIf(ConditionBuilder.column("c1").isNullText())
                .and(ConditionBuilder.column("c2").isEqualToText("a"))
                .build(),
            "The DeleteIf condition of the Delete operation is not satisfied. Targeting column(s): [c1, c2]"),
        Arguments.of(
            ConditionBuilder.deleteIfExists(),
            "The DeleteIfExists condition of the Delete operation is not satisfied. Targeting column(s): null"));
  }

  @Test
  public void insert_InsertGiven_WhenRecordDoesNotExist_ShouldCallJdbcCrudServiceProperly()
      throws CrudException, ExecutionException, SQLException {
    // Arrange
    Insert insert =
        Insert.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();
    Put expectedPut =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .condition(ConditionBuilder.putIfNotExists())
            .build();

    when(jdbcCrudService.put(expectedPut, connection)).thenReturn(true);

    // Act
    transaction.insert(insert);

    // Assert
    verify(jdbcCrudService).put(expectedPut, connection);
  }

  @Test
  public void insert_InsertGiven_WhenRecordExists_ShouldThrowCrudConflictException()
      throws ExecutionException, SQLException {
    // Arrange
    Insert insert =
        Insert.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();
    Put expectedPut =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .condition(ConditionBuilder.putIfNotExists())
            .build();

    when(jdbcCrudService.put(expectedPut, connection)).thenReturn(false);

    // Act Assert
    assertThatThrownBy(() -> transaction.insert(insert)).isInstanceOf(CrudConflictException.class);

    verify(jdbcCrudService).put(expectedPut, connection);
  }

  @Test
  public void insert_InsertGiven_WhenSQLExceptionThrownByJdbcCrudService_ShouldThrowCrudException()
      throws SQLException, ExecutionException {
    // Arrange
    Insert insert =
        Insert.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();
    Put put =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .condition(ConditionBuilder.putIfNotExists())
            .build();

    when(jdbcCrudService.put(put, connection)).thenThrow(SQLException.class);

    // Act Assert
    assertThatThrownBy(() -> transaction.insert(insert)).isInstanceOf(CrudException.class);
  }

  @Test
  public void
      insert_InsertGiven_WhenExecutionExceptionThrownByJdbcCrudService_ShouldThrowCrudException()
          throws SQLException, ExecutionException {
    // Arrange
    Insert insert =
        Insert.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();
    Put put =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .condition(ConditionBuilder.putIfNotExists())
            .build();

    ExecutionException executionException = mock(ExecutionException.class);
    when(executionException.getMessage()).thenReturn("error");
    when(jdbcCrudService.put(put, connection)).thenThrow(executionException);

    // Act Assert
    assertThatThrownBy(() -> transaction.insert(insert)).isInstanceOf(CrudException.class);
  }

  @Test
  public void upsert_UpsertGiven_ShouldCallJdbcCrudServiceProperly()
      throws CrudException, ExecutionException, SQLException {
    // Arrange
    Upsert upsert =
        Upsert.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();
    Put expectedPut =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();

    when(jdbcCrudService.put(expectedPut, connection)).thenReturn(true);

    // Act
    transaction.upsert(upsert);

    // Assert
    verify(jdbcCrudService).put(expectedPut, connection);
  }

  @Test
  public void upsert_UpsertGiven_WhenSQLExceptionThrownByJdbcCrudService_ShouldThrowCrudException()
      throws SQLException, ExecutionException {
    // Arrange
    Upsert upsert =
        Upsert.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();
    Put put =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();

    when(jdbcCrudService.put(put, connection)).thenThrow(SQLException.class);

    // Act Assert
    assertThatThrownBy(() -> transaction.upsert(upsert)).isInstanceOf(CrudException.class);
  }

  @Test
  public void
      upsert_UpsertGiven_WhenExecutionExceptionThrownByJdbcCrudService_ShouldThrowCrudException()
          throws SQLException, ExecutionException {
    // Arrange
    Upsert upsert =
        Upsert.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();
    Put put =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();

    ExecutionException executionException = mock(ExecutionException.class);
    when(executionException.getMessage()).thenReturn("error");
    when(jdbcCrudService.put(put, connection)).thenThrow(executionException);

    // Act Assert
    assertThatThrownBy(() -> transaction.upsert(upsert)).isInstanceOf(CrudException.class);
  }

  @Test
  public void
      update_UpdateWithoutConditionGiven_WhenRecordExists_ShouldCallJdbcCrudServiceProperly()
          throws CrudException, ExecutionException, SQLException {
    // Arrange
    Update update =
        Update.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();
    Put expectedPut =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .condition(ConditionBuilder.putIfExists())
            .build();

    when(jdbcCrudService.put(expectedPut, connection)).thenReturn(true);

    // Act
    transaction.update(update);

    // Assert
    verify(jdbcCrudService).put(expectedPut, connection);
  }

  @Test
  public void update_UpdateWithoutConditionGiven_WhenRecordDoesNotExist_ShouldDoNothing()
      throws ExecutionException, SQLException {
    // Arrange
    Update update =
        Update.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();
    Put expectedPut =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .condition(ConditionBuilder.putIfExists())
            .build();

    when(jdbcCrudService.put(expectedPut, connection)).thenReturn(false);

    // Act Assert
    assertThatCode(() -> transaction.update(update)).doesNotThrowAnyException();

    verify(jdbcCrudService).put(expectedPut, connection);
  }

  @Test
  public void
      update_UpdateWithUpdateIfConditionGiven_WhenConditionSatisfied_ShouldCallJdbcCrudServiceProperly()
          throws CrudException, ExecutionException, SQLException {
    // Arrange
    Update update =
        Update.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_4)
            .condition(
                ConditionBuilder.updateIf(
                        ConditionBuilder.column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                    .build())
            .build();
    Put expectedPut =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_4)
            .condition(
                ConditionBuilder.putIf(
                        ConditionBuilder.column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                    .build())
            .build();

    when(jdbcCrudService.put(expectedPut, connection)).thenReturn(true);

    // Act
    transaction.update(update);

    // Assert
    verify(jdbcCrudService).put(expectedPut, connection);
  }

  @Test
  public void
      update_UpdateWithUpdateIfExistsConditionGiven_WhenConditionSatisfied_ShouldCallJdbcCrudServiceProperly()
          throws CrudException, ExecutionException, SQLException {
    // Arrange
    Update update =
        Update.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_4)
            .condition(ConditionBuilder.updateIfExists())
            .build();
    Put expectedPut =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_4)
            .condition(ConditionBuilder.putIfExists())
            .build();

    when(jdbcCrudService.put(expectedPut, connection)).thenReturn(true);

    // Act
    transaction.update(update);

    // Assert
    verify(jdbcCrudService).put(expectedPut, connection);
  }

  @Test
  public void
      update_UpdateWithUpdateIfConditionGiven_WhenConditionNotSatisfied_ShouldThrowUnsatisfiedConditionException()
          throws ExecutionException, SQLException {
    // Arrange
    Update update =
        Update.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_4)
            .condition(
                ConditionBuilder.updateIf(
                        ConditionBuilder.column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                    .build())
            .build();
    Put expectedPut =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_4)
            .condition(
                ConditionBuilder.putIf(
                        ConditionBuilder.column(ANY_NAME_3).isEqualToText(ANY_TEXT_3))
                    .build())
            .build();

    when(jdbcCrudService.put(expectedPut, connection)).thenReturn(false);

    // Act Assert
    assertThatThrownBy(() -> transaction.update(update))
        .isInstanceOf(UnsatisfiedConditionException.class);

    verify(jdbcCrudService).put(expectedPut, connection);
  }

  @Test
  public void
      update_UpdateWithUpdateIfExistsConditionGiven_WhenConditionNotSatisfied_ShouldThrowUnsatisfiedConditionException()
          throws ExecutionException, SQLException {
    // Arrange
    Update update =
        Update.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_4)
            .condition(ConditionBuilder.updateIfExists())
            .build();
    Put expectedPut =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_4)
            .condition(ConditionBuilder.putIfExists())
            .build();

    when(jdbcCrudService.put(expectedPut, connection)).thenReturn(false);

    // Act Assert
    assertThatThrownBy(() -> transaction.update(update))
        .isInstanceOf(UnsatisfiedConditionException.class);

    verify(jdbcCrudService).put(expectedPut, connection);
  }

  @Test
  public void update_UpdateGiven_WhenSQLExceptionThrownByJdbcCrudService_ShouldThrowCrudException()
      throws SQLException, ExecutionException {
    // Arrange
    Update update =
        Update.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();
    Put put =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .condition(ConditionBuilder.putIfExists())
            .build();

    when(jdbcCrudService.put(put, connection)).thenThrow(SQLException.class);

    // Act Assert
    assertThatThrownBy(() -> transaction.update(update)).isInstanceOf(CrudException.class);
  }

  @Test
  public void
      update_UpdateGiven_WhenExecutionExceptionThrownByJdbcCrudService_ShouldThrowCrudException()
          throws SQLException, ExecutionException {
    // Arrange
    Update update =
        Update.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();
    Put put =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .condition(ConditionBuilder.putIfExists())
            .build();

    ExecutionException executionException = mock(ExecutionException.class);
    when(executionException.getMessage()).thenReturn("error");
    when(jdbcCrudService.put(put, connection)).thenThrow(executionException);

    // Act Assert
    assertThatThrownBy(() -> transaction.update(update)).isInstanceOf(CrudException.class);
  }

  @Test
  public void mutate_MutationsGiven_ShouldCallJdbcCrudServiceProperly()
      throws CrudException, ExecutionException, SQLException {
    // Arrange
    Put put =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();
    Insert insert =
        Insert.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();
    Upsert upsert =
        Upsert.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_3))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();
    Update update =
        Update.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_4))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();
    Delete delete =
        Delete.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .build();

    Put expectedPutFromInsert =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .condition(ConditionBuilder.putIfNotExists())
            .build();
    Put expectedPutFromUpsert =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_3))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();
    Put expectedPutFromUpdate =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_4))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .condition(ConditionBuilder.putIfExists())
            .build();

    when(jdbcCrudService.put(put, connection)).thenReturn(true);
    when(jdbcCrudService.put(expectedPutFromInsert, connection)).thenReturn(true);
    when(jdbcCrudService.put(expectedPutFromUpsert, connection)).thenReturn(true);
    when(jdbcCrudService.put(expectedPutFromUpdate, connection)).thenReturn(true);
    when(jdbcCrudService.delete(delete, connection)).thenReturn(true);

    // Act
    transaction.mutate(Arrays.asList(put, insert, upsert, update, delete));

    // Assert
    verify(jdbcCrudService).put(put, connection);
    verify(jdbcCrudService).put(expectedPutFromInsert, connection);
    verify(jdbcCrudService).put(expectedPutFromUpsert, connection);
    verify(jdbcCrudService).put(expectedPutFromUpdate, connection);
    verify(jdbcCrudService).delete(delete, connection);
  }

  @Test
  public void mutate_EmptyMutationsGiven_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> transaction.mutate(Collections.emptyList()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void batch_OperationsGiven_ShouldCallJdbcCrudServiceProperly()
      throws CrudException, ExecutionException, SQLException {
    // Arrange
    Get get =
        Get.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .build();
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_2))
            .build();
    Put put =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();
    Insert insert =
        Insert.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();
    Upsert upsert =
        Upsert.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_3))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();
    Update update =
        Update.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_4))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();
    Delete delete =
        Delete.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .build();

    Result result1 = mock(Result.class);
    Result result2 = mock(Result.class);
    Result result3 = mock(Result.class);

    Put expectedPutFromInsert =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_2))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .condition(ConditionBuilder.putIfNotExists())
            .build();
    Put expectedPutFromUpsert =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_3))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .build();
    Put expectedPutFromUpdate =
        Put.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_4))
            .textValue(ANY_NAME_3, ANY_TEXT_3)
            .condition(ConditionBuilder.putIfExists())
            .build();

    when(jdbcCrudService.get(get, connection)).thenReturn(Optional.of(result1));
    when(jdbcCrudService.scan(scan, connection)).thenReturn(Arrays.asList(result2, result3));
    when(jdbcCrudService.put(put, connection)).thenReturn(true);
    when(jdbcCrudService.put(expectedPutFromInsert, connection)).thenReturn(true);
    when(jdbcCrudService.put(expectedPutFromUpsert, connection)).thenReturn(true);
    when(jdbcCrudService.put(expectedPutFromUpdate, connection)).thenReturn(true);
    when(jdbcCrudService.delete(delete, connection)).thenReturn(true);

    // Act
    List<CrudOperable.BatchResult> results =
        transaction.batch(Arrays.asList(get, scan, put, insert, upsert, update, delete));

    // Assert
    verify(jdbcCrudService).get(get, connection);
    verify(jdbcCrudService).scan(scan, connection);
    verify(jdbcCrudService).put(put, connection);
    verify(jdbcCrudService).put(expectedPutFromInsert, connection);
    verify(jdbcCrudService).put(expectedPutFromUpsert, connection);
    verify(jdbcCrudService).put(expectedPutFromUpdate, connection);
    verify(jdbcCrudService).delete(delete, connection);
    assertThat(results).hasSize(7);
    assertThat(results.get(0).getType()).isEqualTo(CrudOperable.BatchResult.Type.GET);
    assertThat(results.get(0).getGetResult()).hasValue(result1);
    assertThat(results.get(1).getType()).isEqualTo(CrudOperable.BatchResult.Type.SCAN);
    assertThat(results.get(1).getScanResult()).containsExactly(result2, result3);
    assertThat(results.get(2).getType()).isEqualTo(CrudOperable.BatchResult.Type.PUT);
    assertThat(results.get(3).getType()).isEqualTo(CrudOperable.BatchResult.Type.INSERT);
    assertThat(results.get(4).getType()).isEqualTo(CrudOperable.BatchResult.Type.UPSERT);
    assertThat(results.get(5).getType()).isEqualTo(CrudOperable.BatchResult.Type.UPDATE);
    assertThat(results.get(6).getType()).isEqualTo(CrudOperable.BatchResult.Type.DELETE);
  }

  @Test
  public void batch_EmptyOperationsGiven_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> transaction.batch(Collections.emptyList()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  /**
   * {@link JdbcTransaction#commit()} infers the transaction outcome from whether the follow-up
   * {@code rollback()} succeeds. That inference is sound only because HikariCP evicts a connection
   * whose SQLState starts with "08", which is exactly the range the AWS Advanced JDBC Wrapper uses
   * to report a failover. Setting HikariCP's {@code exceptionOverrideClassName} -- which AWS
   * recommends -- would keep the connection alive, let the rollback succeed as a no-op, and turn an
   * unknown outcome into a {@link com.scalar.db.exception.transaction.CommitException} whose
   * contract tells the caller it is safe to retry from the beginning.
   *
   * <p>These tests run against a real HikariCP pool so the eviction is genuinely exercised. See
   * {@code JdbcFailoverExceptionBehaviorTest} for the pool-level counterpart.
   */
  @ParameterizedTest
  @ValueSource(strings = {"08001", "08S02", "08007"})
  public void commit_WhenFailoverSqlStateThrown_ShouldThrowUnknownTransactionStatusException(
      String sqlState) throws SQLException {
    // Arrange
    try (HikariDataSource dataSource = FailoverSimulatingDriver.createDataSource()) {
      Connection pooledConnection = dataSource.getConnection();
      JdbcTransaction target =
          new JdbcTransaction(ANY_TX_ID, jdbcCrudService, pooledConnection, rdbEngineStrategy);
      FailoverSimulatingDriver.failOnCommitWith(sqlState);

      // Act Assert
      assertThatThrownBy(target::commit).isInstanceOf(UnknownTransactionStatusException.class);
    } finally {
      FailoverSimulatingDriver.reset();
    }
  }

  @Test
  public void commit_WhenConflictThrownAndConnectionAlive_ShouldThrowCommitConflictException()
      throws SQLException {
    // Arrange
    try (HikariDataSource dataSource = FailoverSimulatingDriver.createDataSource()) {
      Connection pooledConnection = dataSource.getConnection();
      JdbcTransaction target =
          new JdbcTransaction(ANY_TX_ID, jdbcCrudService, pooledConnection, rdbEngineStrategy);
      when(rdbEngineStrategy.isConflict(any(SQLException.class))).thenReturn(true);
      // 40001 is a genuine serialization failure. HikariCP does not evict, so the rollback
      // succeeds and the outcome is known to be a failure rather than unknown.
      FailoverSimulatingDriver.failOnCommitWith("40001");

      // Act Assert
      assertThatThrownBy(target::commit).isInstanceOf(CommitConflictException.class);
    } finally {
      FailoverSimulatingDriver.reset();
    }
  }

  @Test
  public void commit_WhenNonConflictThrownAndConnectionAlive_ShouldThrowPlainCommitException()
      throws SQLException {
    // Arrange
    try (HikariDataSource dataSource = FailoverSimulatingDriver.createDataSource()) {
      Connection pooledConnection = dataSource.getConnection();
      JdbcTransaction target =
          new JdbcTransaction(ANY_TX_ID, jdbcCrudService, pooledConnection, rdbEngineStrategy);
      when(rdbEngineStrategy.isConflict(any(SQLException.class))).thenReturn(false);
      // 42000 is a syntax error or access rule violation: neither a conflict nor a connection
      // exception. HikariCP does not evict, so the rollback succeeds and the transaction is known
      // to have failed. Reporting it as unknown would send the caller reconciling a transaction
      // that plainly did not commit.
      FailoverSimulatingDriver.failOnCommitWith("42000");

      // Act Assert
      assertThatThrownBy(target::commit)
          .isInstanceOf(CommitException.class)
          .isNotInstanceOf(CommitConflictException.class);
    } finally {
      FailoverSimulatingDriver.reset();
    }
  }

  /**
   * The scanner reports a read failure as an {@link ExecutionException} carrying the {@link
   * SQLException} as its cause, so without unwrapping it, iterating one would be the only CRUD path
   * that fails to recognize a conflict.
   */
  @Test
  public void getScannerAndScannerOne_WhenConflictThrown_ShouldThrowCrudConflictException()
      throws ExecutionException, SQLException, CrudException {
    // Arrange
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText("p1", "val"))
            .build();
    Scanner scanner = mock(Scanner.class);
    when(scanner.one())
        .thenThrow(
            new ExecutionException(
                "Fetching the next result failed",
                new SQLException("Serialization failure", "40001")));
    when(jdbcCrudService.getScanner(scan, connection, false)).thenReturn(scanner);
    when(rdbEngineStrategy.isConflict(any(SQLException.class))).thenReturn(true);

    // Act Assert
    TransactionCrudOperable.Scanner actual = transaction.getScanner(scan);
    assertThatThrownBy(actual::one).isInstanceOf(CrudConflictException.class);
  }

  @Test
  public void getScannerAndScannerAll_WhenConflictThrown_ShouldThrowCrudConflictException()
      throws ExecutionException, SQLException, CrudException {
    // Arrange
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText("p1", "val"))
            .build();
    Scanner scanner = mock(Scanner.class);
    when(scanner.all())
        .thenThrow(
            new ExecutionException(
                "Fetching the next result failed",
                new SQLException("Serialization failure", "40001")));
    when(jdbcCrudService.getScanner(scan, connection, false)).thenReturn(scanner);
    when(rdbEngineStrategy.isConflict(any(SQLException.class))).thenReturn(true);

    // Act Assert
    TransactionCrudOperable.Scanner actual = transaction.getScanner(scan);
    assertThatThrownBy(actual::all).isInstanceOf(CrudConflictException.class);
  }

  @Test
  public void getScannerAndScannerOne_WhenNonConflictThrown_ShouldThrowPlainCrudException()
      throws ExecutionException, SQLException, CrudException {
    // Arrange
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText("p1", "val"))
            .build();
    Scanner scanner = mock(Scanner.class);
    when(scanner.one())
        .thenThrow(
            new ExecutionException(
                "Fetching the next result failed", new SQLException("Undefined table", "42P01")));
    when(jdbcCrudService.getScanner(scan, connection, false)).thenReturn(scanner);

    // Act Assert
    TransactionCrudOperable.Scanner actual = transaction.getScanner(scan);
    assertThatThrownBy(actual::one)
        .isInstanceOf(CrudException.class)
        .isNotInstanceOf(CrudConflictException.class);
  }

  @Test
  public void commit_WhenFailoverSqlStateThrown_ShouldKeepOriginalFailureAsSuppressed()
      throws SQLException {
    // Arrange
    try (HikariDataSource dataSource = FailoverSimulatingDriver.createDataSource()) {
      Connection pooledConnection = dataSource.getConnection();
      JdbcTransaction target =
          new JdbcTransaction(ANY_TX_ID, jdbcCrudService, pooledConnection, rdbEngineStrategy);
      FailoverSimulatingDriver.failOnCommitWith("08007");

      // Act
      Throwable thrown = catchThrowable(target::commit);

      // Assert
      assertThat(thrown).isInstanceOf(UnknownTransactionStatusException.class);
      // Without this the caller only learns that a rollback failed, which says nothing about the
      // failover that made the outcome unknown in the first place.
      assertThat(thrown.getSuppressed()).hasSize(1);
      assertThat(thrown.getSuppressed()[0]).isInstanceOf(SQLException.class);
      assertThat(((SQLException) thrown.getSuppressed()[0]).getSQLState()).isEqualTo("08007");
    } finally {
      FailoverSimulatingDriver.reset();
    }
  }
}
