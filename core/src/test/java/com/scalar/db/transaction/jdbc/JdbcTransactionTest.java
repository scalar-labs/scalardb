package com.scalar.db.transaction.jdbc;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Insert;
import com.scalar.db.api.MutationCondition;
import com.scalar.db.api.Put;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.RecordNotFoundException;
import com.scalar.db.exception.transaction.UnsatisfiedConditionException;
import com.scalar.db.io.Key;
import com.scalar.db.storage.jdbc.JdbcService;
import com.scalar.db.storage.jdbc.RdbEngineStrategy;
import java.sql.Connection;
import java.sql.SQLException;
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

    when(jdbcService.put(put, connection)).thenThrow(ExecutionException.class);

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

    when(jdbcService.put(put, connection)).thenThrow(ExecutionException.class);

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
  public void
      update_UpdateWithoutConditionGiven_WhenRecordDoesNotExist_ShouldThrowRecordNotFoundException()
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
    assertThatThrownBy(() -> transaction.update(update))
        .isInstanceOf(RecordNotFoundException.class);

    verify(jdbcService).put(expectedPut, connection);
  }

  @Test
  public void update_UpdateWithConditionGiven_WhenConditionSatisfied_ShouldCallJdbcServiceProperly()
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
      update_UpdateWithConditionGiven_WhenConditionNotSatisfied_ShouldThrowUnsatisfiedConditionException()
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

    when(jdbcService.put(put, connection)).thenThrow(ExecutionException.class);

    // Act Assert
    assertThatThrownBy(() -> transaction.update(update)).isInstanceOf(CrudException.class);
  }
}
