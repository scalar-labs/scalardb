package com.scalar.db.transaction.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Delete;
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
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.UnsatisfiedConditionException;
import com.scalar.db.io.Key;
import com.scalar.db.storage.jdbc.JdbcService;
import com.scalar.db.storage.jdbc.RdbEngineStrategy;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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
  @Mock private JdbcService jdbcService;
  @Mock private Connection connection;
  @Mock private RdbEngineStrategy rdbEngineStrategy;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    transaction = new JdbcTransaction(ANY_TX_ID, jdbcService, connection, rdbEngineStrategy);
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

    when(jdbcService.getScanner(scan, connection, false)).thenReturn(scanner);

    // Act Assert
    TransactionCrudOperable.Scanner actual = transaction.getScanner(scan);
    assertThat(actual.one()).hasValue(result1);
    assertThat(actual.one()).hasValue(result2);
    assertThat(actual.one()).hasValue(result3);
    assertThat(actual.one()).isEmpty();
    actual.close();

    verify(jdbcService).getScanner(scan, connection, false);
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

    when(jdbcService.getScanner(scan, connection, false)).thenReturn(scanner);

    // Act Assert
    TransactionCrudOperable.Scanner actual = transaction.getScanner(scan);
    assertThat(actual.all()).containsExactly(result1, result2, result3);
    actual.close();

    verify(jdbcService).getScanner(scan, connection, false);
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

    when(jdbcService.getScanner(scan, connection, false)).thenReturn(scanner);

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

    verify(jdbcService).getScanner(scan, connection, false);
    verify(scanner).close();
  }

  @Test
  public void getScanner_WhenSQLExceptionThrownByJdbcService_ShouldThrowCrudException()
      throws SQLException, ExecutionException {
    // Arrange
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText("p1", "val"))
            .build();

    when(jdbcService.getScanner(scan, connection, false)).thenThrow(SQLException.class);

    // Act Assert
    assertThatThrownBy(() -> transaction.getScanner(scan)).isInstanceOf(CrudException.class);
  }

  @Test
  public void getScanner_WhenExecutionExceptionThrownByJdbcService_ShouldThrowCrudException()
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
    when(jdbcService.getScanner(scan, connection, false)).thenThrow(executionException);

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

    when(jdbcService.getScanner(scan, connection, false)).thenReturn(scanner);

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

    when(jdbcService.getScanner(scan, connection, false)).thenReturn(scanner);

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

    when(jdbcService.getScanner(scan, connection, false)).thenReturn(scanner);

    // Act Assert
    TransactionCrudOperable.Scanner actual = transaction.getScanner(scan);
    assertThatThrownBy(actual::close).isInstanceOf(CrudException.class);
  }

  @Test
  public void put_putDoesNotSucceed_shouldThrowUnsatisfiedConditionException()
      throws SQLException, ExecutionException {
    // Arrange
    when(jdbcService.put(any(), any())).thenReturn(false);

    // Act Assert
    assertThatThrownBy(() -> transaction.put(ANY_PUT))
        .isInstanceOf(UnsatisfiedConditionException.class);
    verify(jdbcService).put(ANY_PUT, connection);
  }

  @Test
  public void put_putSucceed_shouldThrowUnsatisfiedConditionException()
      throws SQLException, ExecutionException {
    // Arrange
    when(jdbcService.put(any(), any())).thenReturn(true);

    // Act Assert
    assertThatCode(() -> transaction.put(ANY_PUT)).doesNotThrowAnyException();
    verify(jdbcService).put(ANY_PUT, connection);
  }

  @Test
  public void delete_deleteDoesNotSucceed_shouldThrowUnsatisfiedConditionException()
      throws SQLException, ExecutionException {
    // Arrange
    when(jdbcService.delete(any(), any())).thenReturn(false);

    // Act Assert
    assertThatThrownBy(() -> transaction.delete(ANY_DELETE))
        .isInstanceOf(UnsatisfiedConditionException.class);
    verify(jdbcService).delete(ANY_DELETE, connection);
  }

  @Test
  public void delete_deleteSucceed_shouldThrowUnsatisfiedConditionException()
      throws SQLException, ExecutionException {
    // Arrange
    when(jdbcService.delete(any(), any())).thenReturn(true);

    // Act Assert
    assertThatCode(() -> transaction.delete(ANY_DELETE)).doesNotThrowAnyException();
    verify(jdbcService).delete(ANY_DELETE, connection);
  }

  @ParameterizedTest
  @MethodSource("provideConditionalPuts")
  public void put_unsatisfiedConditionExceptionThrown_shouldContainsAppropriateMessage(
      MutationCondition condition, String exceptionMessage)
      throws SQLException, ExecutionException {
    // Arrange
    when(jdbcService.put(any(), any())).thenReturn(false);
    Put put1 = Put.newBuilder(ANY_PUT).condition(condition).build();

    // Act Assert
    assertThatThrownBy(() -> transaction.put(put1))
        .isInstanceOf(UnsatisfiedConditionException.class)
        .hasMessageContaining(ANY_TX_ID)
        .hasMessageContaining(exceptionMessage);
    verify(jdbcService).put(put1, connection);
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
    when(jdbcService.delete(any(), any())).thenReturn(false);
    Delete delete1 = Delete.newBuilder(ANY_DELETE).condition(condition).build();

    // Act Assert
    assertThatThrownBy(() -> transaction.delete(delete1))
        .isInstanceOf(UnsatisfiedConditionException.class)
        .hasMessageContaining(ANY_TX_ID)
        .hasMessageContaining(exceptionMessage);
    verify(jdbcService).delete(delete1, connection);
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
  public void insert_InsertGiven_WhenRecordDoesNotExist_ShouldCallJdbcServiceProperly()
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

    when(jdbcService.put(expectedPut, connection)).thenReturn(true);

    // Act
    transaction.insert(insert);

    // Assert
    verify(jdbcService).put(expectedPut, connection);
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

    when(jdbcService.put(expectedPut, connection)).thenReturn(false);

    // Act Assert
    assertThatThrownBy(() -> transaction.insert(insert)).isInstanceOf(CrudConflictException.class);

    verify(jdbcService).put(expectedPut, connection);
  }

  @Test
  public void insert_InsertGiven_WhenSQLExceptionThrownByJdbcService_ShouldThrowCrudException()
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

    when(jdbcService.put(put, connection)).thenThrow(SQLException.class);

    // Act Assert
    assertThatThrownBy(() -> transaction.insert(insert)).isInstanceOf(CrudException.class);
  }

  @Test
  public void
      insert_InsertGiven_WhenExecutionExceptionThrownByJdbcService_ShouldThrowCrudException()
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
    when(jdbcService.put(put, connection)).thenThrow(executionException);

    // Act Assert
    assertThatThrownBy(() -> transaction.insert(insert)).isInstanceOf(CrudException.class);
  }

  @Test
  public void upsert_UpsertGiven_ShouldCallJdbcServiceProperly()
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

    when(jdbcService.put(expectedPut, connection)).thenReturn(true);

    // Act
    transaction.upsert(upsert);

    // Assert
    verify(jdbcService).put(expectedPut, connection);
  }

  @Test
  public void upsert_UpsertGiven_WhenSQLExceptionThrownByJdbcService_ShouldThrowCrudException()
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

    when(jdbcService.put(put, connection)).thenThrow(SQLException.class);

    // Act Assert
    assertThatThrownBy(() -> transaction.upsert(upsert)).isInstanceOf(CrudException.class);
  }

  @Test
  public void
      upsert_UpsertGiven_WhenExecutionExceptionThrownByJdbcService_ShouldThrowCrudException()
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
    when(jdbcService.put(put, connection)).thenThrow(executionException);

    // Act Assert
    assertThatThrownBy(() -> transaction.upsert(upsert)).isInstanceOf(CrudException.class);
  }

  @Test
  public void update_UpdateWithoutConditionGiven_WhenRecordExists_ShouldCallJdbcServiceProperly()
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

    when(jdbcService.put(expectedPut, connection)).thenReturn(true);

    // Act
    transaction.update(update);

    // Assert
    verify(jdbcService).put(expectedPut, connection);
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

    when(jdbcService.put(expectedPut, connection)).thenReturn(false);

    // Act Assert
    assertThatCode(() -> transaction.update(update)).doesNotThrowAnyException();

    verify(jdbcService).put(expectedPut, connection);
  }

  @Test
  public void
      update_UpdateWithUpdateIfConditionGiven_WhenConditionSatisfied_ShouldCallJdbcServiceProperly()
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

    when(jdbcService.put(expectedPut, connection)).thenReturn(true);

    // Act
    transaction.update(update);

    // Assert
    verify(jdbcService).put(expectedPut, connection);
  }

  @Test
  public void
      update_UpdateWithUpdateIfExistsConditionGiven_WhenConditionSatisfied_ShouldCallJdbcServiceProperly()
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

    when(jdbcService.put(expectedPut, connection)).thenReturn(true);

    // Act
    transaction.update(update);

    // Assert
    verify(jdbcService).put(expectedPut, connection);
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

    when(jdbcService.put(expectedPut, connection)).thenReturn(false);

    // Act Assert
    assertThatThrownBy(() -> transaction.update(update))
        .isInstanceOf(UnsatisfiedConditionException.class);

    verify(jdbcService).put(expectedPut, connection);
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

    when(jdbcService.put(expectedPut, connection)).thenReturn(false);

    // Act Assert
    assertThatThrownBy(() -> transaction.update(update))
        .isInstanceOf(UnsatisfiedConditionException.class);

    verify(jdbcService).put(expectedPut, connection);
  }

  @Test
  public void update_UpdateGiven_WhenSQLExceptionThrownByJdbcService_ShouldThrowCrudException()
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

    when(jdbcService.put(put, connection)).thenThrow(SQLException.class);

    // Act Assert
    assertThatThrownBy(() -> transaction.update(update)).isInstanceOf(CrudException.class);
  }

  @Test
  public void
      update_UpdateGiven_WhenExecutionExceptionThrownByJdbcService_ShouldThrowCrudException()
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
    when(jdbcService.put(put, connection)).thenThrow(executionException);

    // Act Assert
    assertThatThrownBy(() -> transaction.update(update)).isInstanceOf(CrudException.class);
  }
}
