package com.scalar.db.storage.objectstorage;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class StatementHandlerTest {

  private static final String COLUMN_NAME_1 = "col1";
  private static final String COLUMN_NAME_2 = "col2";

  @Mock private TableMetadata metadata;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    when(metadata.getColumnDataType(anyString())).thenReturn(DataType.INT);
  }

  @Test
  public void validateConditions_WithEqConditionAndMatchingValue_ShouldNotThrowException() {
    // Arrange
    ObjectStorageRecord record = createRecord(10);
    ConditionalExpression condition = ConditionBuilder.column(COLUMN_NAME_1).isEqualToInt(10);
    List<ConditionalExpression> expressions = Collections.singletonList(condition);

    // Act Assert
    assertThatCode(() -> StatementHandler.validateConditions(record, expressions, metadata))
        .doesNotThrowAnyException();
  }

  @Test
  public void validateConditions_WithEqConditionAndDifferentValue_ShouldThrowExecutionException() {
    // Arrange
    ObjectStorageRecord record = createRecord(10);
    ConditionalExpression condition = ConditionBuilder.column(COLUMN_NAME_1).isEqualToInt(20);
    List<ConditionalExpression> expressions = Collections.singletonList(condition);

    // Act Assert
    assertThatThrownBy(() -> StatementHandler.validateConditions(record, expressions, metadata))
        .isInstanceOf(ExecutionException.class);
  }

  @Test
  public void validateConditions_WithEqConditionAndNullValue_ShouldThrowExecutionException() {
    // Arrange
    ObjectStorageRecord record = createRecordWithNull();
    ConditionalExpression condition = ConditionBuilder.column(COLUMN_NAME_1).isEqualToInt(10);
    List<ConditionalExpression> expressions = Collections.singletonList(condition);

    // Act Assert
    assertThatThrownBy(() -> StatementHandler.validateConditions(record, expressions, metadata))
        .isInstanceOf(ExecutionException.class);
  }

  @Test
  public void validateConditions_WithNeConditionAndDifferentValue_ShouldNotThrowException() {
    // Arrange
    ObjectStorageRecord record = createRecord(10);
    ConditionalExpression condition = ConditionBuilder.column(COLUMN_NAME_1).isNotEqualToInt(20);
    List<ConditionalExpression> expressions = Collections.singletonList(condition);

    // Act Assert
    assertThatCode(() -> StatementHandler.validateConditions(record, expressions, metadata))
        .doesNotThrowAnyException();
  }

  @Test
  public void validateConditions_WithNeConditionAndSameValue_ShouldThrowExecutionException() {
    // Arrange
    ObjectStorageRecord record = createRecord(10);
    ConditionalExpression condition = ConditionBuilder.column(COLUMN_NAME_1).isNotEqualToInt(10);
    List<ConditionalExpression> expressions = Collections.singletonList(condition);

    // Act Assert
    assertThatThrownBy(() -> StatementHandler.validateConditions(record, expressions, metadata))
        .isInstanceOf(ExecutionException.class);
  }

  @Test
  public void validateConditions_WithGtConditionAndGreaterValue_ShouldNotThrowException() {
    // Arrange
    ObjectStorageRecord record = createRecord(20);
    ConditionalExpression condition = ConditionBuilder.column(COLUMN_NAME_1).isGreaterThanInt(10);
    List<ConditionalExpression> expressions = Collections.singletonList(condition);

    // Act Assert
    assertThatCode(() -> StatementHandler.validateConditions(record, expressions, metadata))
        .doesNotThrowAnyException();
  }

  @Test
  public void validateConditions_WithGtConditionAndSameValue_ShouldThrowExecutionException() {
    // Arrange
    ObjectStorageRecord record = createRecord(10);
    ConditionalExpression condition = ConditionBuilder.column(COLUMN_NAME_1).isGreaterThanInt(10);
    List<ConditionalExpression> expressions = Collections.singletonList(condition);

    // Act Assert
    assertThatThrownBy(() -> StatementHandler.validateConditions(record, expressions, metadata))
        .isInstanceOf(ExecutionException.class);
  }

  @Test
  public void validateConditions_WithGteConditionAndGreaterValue_ShouldNotThrowException() {
    // Arrange
    ObjectStorageRecord record = createRecord(20);
    ConditionalExpression condition =
        ConditionBuilder.column(COLUMN_NAME_1).isGreaterThanOrEqualToInt(10);
    List<ConditionalExpression> expressions = Collections.singletonList(condition);

    // Act Assert
    assertThatCode(() -> StatementHandler.validateConditions(record, expressions, metadata))
        .doesNotThrowAnyException();
  }

  @Test
  public void validateConditions_WithGteConditionAndSameValue_ShouldNotThrowException() {
    // Arrange
    ObjectStorageRecord record = createRecord(10);
    ConditionalExpression condition =
        ConditionBuilder.column(COLUMN_NAME_1).isGreaterThanOrEqualToInt(10);
    List<ConditionalExpression> expressions = Collections.singletonList(condition);

    // Act Assert
    assertThatCode(() -> StatementHandler.validateConditions(record, expressions, metadata))
        .doesNotThrowAnyException();
  }

  @Test
  public void validateConditions_WithGteConditionAndSmallerValue_ShouldThrowExecutionException() {
    // Arrange
    ObjectStorageRecord record = createRecord(5);
    ConditionalExpression condition =
        ConditionBuilder.column(COLUMN_NAME_1).isGreaterThanOrEqualToInt(10);
    List<ConditionalExpression> expressions = Collections.singletonList(condition);

    // Act Assert
    assertThatThrownBy(() -> StatementHandler.validateConditions(record, expressions, metadata))
        .isInstanceOf(ExecutionException.class);
  }

  @Test
  public void validateConditions_WithLtConditionAndSmallerValue_ShouldNotThrowException() {
    // Arrange
    ObjectStorageRecord record = createRecord(5);
    ConditionalExpression condition = ConditionBuilder.column(COLUMN_NAME_1).isLessThanInt(10);
    List<ConditionalExpression> expressions = Collections.singletonList(condition);

    // Act Assert
    assertThatCode(() -> StatementHandler.validateConditions(record, expressions, metadata))
        .doesNotThrowAnyException();
  }

  @Test
  public void validateConditions_WithLtConditionAndSameValue_ShouldThrowExecutionException() {
    // Arrange
    ObjectStorageRecord record = createRecord(10);
    ConditionalExpression condition = ConditionBuilder.column(COLUMN_NAME_1).isLessThanInt(10);
    List<ConditionalExpression> expressions = Collections.singletonList(condition);

    // Act Assert
    assertThatThrownBy(() -> StatementHandler.validateConditions(record, expressions, metadata))
        .isInstanceOf(ExecutionException.class);
  }

  @Test
  public void validateConditions_WithLteConditionAndSmallerValue_ShouldNotThrowException() {
    // Arrange
    ObjectStorageRecord record = createRecord(5);
    ConditionalExpression condition =
        ConditionBuilder.column(COLUMN_NAME_1).isLessThanOrEqualToInt(10);
    List<ConditionalExpression> expressions = Collections.singletonList(condition);

    // Act Assert
    assertThatCode(() -> StatementHandler.validateConditions(record, expressions, metadata))
        .doesNotThrowAnyException();
  }

  @Test
  public void validateConditions_WithLteConditionAndSameValue_ShouldNotThrowException() {
    // Arrange
    ObjectStorageRecord record = createRecord(10);
    ConditionalExpression condition =
        ConditionBuilder.column(COLUMN_NAME_1).isLessThanOrEqualToInt(10);
    List<ConditionalExpression> expressions = Collections.singletonList(condition);

    // Act Assert
    assertThatCode(() -> StatementHandler.validateConditions(record, expressions, metadata))
        .doesNotThrowAnyException();
  }

  @Test
  public void validateConditions_WithLteConditionAndGreaterValue_ShouldThrowExecutionException() {
    // Arrange
    ObjectStorageRecord record = createRecord(20);
    ConditionalExpression condition =
        ConditionBuilder.column(COLUMN_NAME_1).isLessThanOrEqualToInt(10);
    List<ConditionalExpression> expressions = Collections.singletonList(condition);

    // Act Assert
    assertThatThrownBy(() -> StatementHandler.validateConditions(record, expressions, metadata))
        .isInstanceOf(ExecutionException.class);
  }

  @Test
  public void validateConditions_WithIsNullConditionAndNullValue_ShouldNotThrowException() {
    // Arrange
    ObjectStorageRecord record = createRecordWithNull();
    ConditionalExpression condition = ConditionBuilder.column(COLUMN_NAME_1).isNullInt();
    List<ConditionalExpression> expressions = Collections.singletonList(condition);

    // Act Assert
    assertThatCode(() -> StatementHandler.validateConditions(record, expressions, metadata))
        .doesNotThrowAnyException();
  }

  @Test
  public void
      validateConditions_WithIsNullConditionAndNonNullValue_ShouldThrowExecutionException() {
    // Arrange
    ObjectStorageRecord record = createRecord(10);
    ConditionalExpression condition = ConditionBuilder.column(COLUMN_NAME_1).isNullInt();
    List<ConditionalExpression> expressions = Collections.singletonList(condition);

    // Act Assert
    assertThatThrownBy(() -> StatementHandler.validateConditions(record, expressions, metadata))
        .isInstanceOf(ExecutionException.class);
  }

  @Test
  public void validateConditions_WithIsNotNullConditionAndNonNullValue_ShouldNotThrowException() {
    // Arrange
    ObjectStorageRecord record = createRecord(10);
    ConditionalExpression condition = ConditionBuilder.column(COLUMN_NAME_1).isNotNullInt();
    List<ConditionalExpression> expressions = Collections.singletonList(condition);

    // Act Assert
    assertThatCode(() -> StatementHandler.validateConditions(record, expressions, metadata))
        .doesNotThrowAnyException();
  }

  @Test
  public void
      validateConditions_WithIsNotNullConditionAndNullValue_ShouldThrowExecutionException() {
    // Arrange
    ObjectStorageRecord record = createRecordWithNull();
    ConditionalExpression condition = ConditionBuilder.column(COLUMN_NAME_1).isNotNullInt();
    List<ConditionalExpression> expressions = Collections.singletonList(condition);

    // Act Assert
    assertThatThrownBy(() -> StatementHandler.validateConditions(record, expressions, metadata))
        .isInstanceOf(ExecutionException.class);
  }

  @Test
  public void validateConditions_WithMultipleConditionsAllMatching_ShouldNotThrowException() {
    // Arrange
    ObjectStorageRecord record = createRecordWithMultipleColumns();
    when(metadata.getColumnDataType(COLUMN_NAME_1)).thenReturn(DataType.INT);
    when(metadata.getColumnDataType(COLUMN_NAME_2)).thenReturn(DataType.TEXT);

    ConditionalExpression condition1 = ConditionBuilder.column(COLUMN_NAME_1).isEqualToInt(10);
    ConditionalExpression condition2 =
        ConditionBuilder.column(COLUMN_NAME_2).isEqualToText("value");
    List<ConditionalExpression> expressions = Arrays.asList(condition1, condition2);

    // Act Assert
    assertThatCode(() -> StatementHandler.validateConditions(record, expressions, metadata))
        .doesNotThrowAnyException();
  }

  @Test
  public void
      validateConditions_WithMultipleConditionsOneNotMatching_ShouldThrowExecutionException() {
    // Arrange
    ObjectStorageRecord record = createRecordWithMultipleColumns();
    when(metadata.getColumnDataType(COLUMN_NAME_1)).thenReturn(DataType.INT);
    when(metadata.getColumnDataType(COLUMN_NAME_2)).thenReturn(DataType.TEXT);

    ConditionalExpression condition1 = ConditionBuilder.column(COLUMN_NAME_1).isEqualToInt(10);
    ConditionalExpression condition2 =
        ConditionBuilder.column(COLUMN_NAME_2).isEqualToText("different");
    List<ConditionalExpression> expressions = Arrays.asList(condition1, condition2);

    // Act Assert
    assertThatThrownBy(() -> StatementHandler.validateConditions(record, expressions, metadata))
        .isInstanceOf(ExecutionException.class);
  }

  private ObjectStorageRecord createRecord(int value) {
    return ObjectStorageRecord.newBuilder()
        .id("id")
        .partitionKey(new HashMap<>())
        .clusteringKey(new HashMap<>())
        .values(Collections.singletonMap(COLUMN_NAME_1, value))
        .build();
  }

  private ObjectStorageRecord createRecordWithNull() {
    return ObjectStorageRecord.newBuilder()
        .id("id")
        .partitionKey(new HashMap<>())
        .clusteringKey(new HashMap<>())
        .values(Collections.singletonMap(COLUMN_NAME_1, null))
        .build();
  }

  private ObjectStorageRecord createRecordWithMultipleColumns() {
    return ObjectStorageRecord.newBuilder()
        .id("id")
        .partitionKey(new HashMap<>())
        .clusteringKey(new HashMap<>())
        .values(new HashMap<>(ImmutableMap.of(COLUMN_NAME_1, 10, COLUMN_NAME_2, "value")))
        .build();
  }
}
