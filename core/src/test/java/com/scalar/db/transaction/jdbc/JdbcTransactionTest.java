package com.scalar.db.transaction.jdbc;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Delete;
import com.scalar.db.api.MutationCondition;
import com.scalar.db.api.Put;
import com.scalar.db.exception.storage.ExecutionException;
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
}
