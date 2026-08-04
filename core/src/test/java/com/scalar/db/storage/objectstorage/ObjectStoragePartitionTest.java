package com.scalar.db.storage.objectstorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Put;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.CollationComparator;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ObjectStoragePartitionTest {
  private static final String NAMESPACE = "test_namespace";
  private static final String TABLE = "test_table";
  private static final String PARTITION_KEY_NAME = "pk";
  private static final String CLUSTERING_KEY_NAME = "ck";
  private static final String COLUMN_NAME_1 = "col1";
  private static final String COLUMN_NAME_2 = "col2";
  private static final String PARTITION_KEY_VALUE = "pk1";
  private static final String CLUSTERING_KEY_VALUE = "ck1";
  private static final String RECORD_ID_1 = "record1";
  private static final String RECORD_ID_2 = "record2";
  private static final int INT_VALUE_1 = 10;
  private static final int INT_VALUE_2 = 20;

  @Mock private TableMetadata metadata;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    when(metadata.getPartitionKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList(PARTITION_KEY_NAME)));
    when(metadata.getClusteringKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList(CLUSTERING_KEY_NAME)));
    when(metadata.getColumnDataType(anyString())).thenReturn(DataType.INT);
  }

  @Test
  public void getRecord_WhenRecordExists_ShouldReturnRecord() {
    // Arrange
    Map<String, ObjectStorageRecord> records = new HashMap<>();
    ObjectStorageRecord record = createRecord(RECORD_ID_1, INT_VALUE_1);
    records.put(RECORD_ID_1, record);
    ObjectStoragePartition partition = createObjectStoragePartition(records);

    // Act
    Optional<ObjectStorageRecord> result = partition.getRecord(RECORD_ID_1);

    // Assert
    assertThat(result).isPresent();
    assertThat(result).hasValue(record);
  }

  @Test
  public void getRecord_WhenRecordDoesNotExist_ShouldReturnEmpty() {
    // Arrange
    ObjectStoragePartition partition = createObjectStoragePartition(new HashMap<>());

    // Act Assert
    assertThat(partition.getRecord(RECORD_ID_1)).isEmpty();
  }

  @Test
  public void getRecords_WhenPartitionHasRecords_ShouldReturnAllRecords() {
    // Arrange
    Map<String, ObjectStorageRecord> records = new HashMap<>();
    ObjectStorageRecord record1 = createRecord(RECORD_ID_1, INT_VALUE_1);
    ObjectStorageRecord record2 = createRecord(RECORD_ID_2, INT_VALUE_2);
    records.put(RECORD_ID_1, record1);
    records.put(RECORD_ID_2, record2);
    ObjectStoragePartition partition = createObjectStoragePartition(records);

    // Act
    Map<String, ObjectStorageRecord> result = partition.getRecords();

    // Assert
    assertThat(result).hasSize(2);
    assertThat(result.values()).containsExactlyInAnyOrder(record1, record2);
    assertThat(result).containsKey(RECORD_ID_1);
    assertThat(result).containsKey(RECORD_ID_2);
  }

  @Test
  public void isEmpty_WhenPartitionHasNoRecords_ShouldReturnTrue() {
    // Arrange
    ObjectStoragePartition partition = createObjectStoragePartition(new HashMap<>());

    // Act & Assert
    assertThat(partition.isEmpty()).isTrue();
  }

  @Test
  public void isEmpty_WhenPartitionHasRecords_ShouldReturnFalse() {
    // Arrange
    Map<String, ObjectStorageRecord> records = new HashMap<>();
    records.put(RECORD_ID_1, createRecord(RECORD_ID_1, INT_VALUE_1));
    ObjectStoragePartition partition = createObjectStoragePartition(records);

    // Act & Assert
    assertThat(partition.isEmpty()).isFalse();
  }

  @Test
  public void applyPut_WithoutConditionAndRecordDoesNotExist_ShouldInsertRecord() throws Exception {
    // Arrange
    ObjectStoragePartition partition = createObjectStoragePartition(new HashMap<>());
    Put put = createPut(INT_VALUE_1);

    // Act
    partition.applyPut(put, metadata, Optional.empty());

    // Assert
    Map<String, ObjectStorageRecord> records = partition.getRecords();
    assertThat(records).hasSize(1);
    ObjectStorageRecord record = records.values().iterator().next();
    assertThat(record.getValues()).containsEntry(COLUMN_NAME_1, INT_VALUE_1);
    assertThat(record.getValues()).containsEntry(COLUMN_NAME_2, INT_VALUE_2);
  }

  @Test
  public void applyPut_WithoutConditionAndRecordExists_ShouldUpdateRecord() throws Exception {
    // Arrange
    Map<String, ObjectStorageRecord> records = new HashMap<>();
    String recordId =
        PARTITION_KEY_VALUE + ObjectStorageUtils.CONCATENATED_KEY_DELIMITER + CLUSTERING_KEY_VALUE;
    records.put(recordId, createRecord(recordId, INT_VALUE_1));
    ObjectStoragePartition partition = createObjectStoragePartition(records);
    Put put = createPut(INT_VALUE_2);

    // Act
    partition.applyPut(put, metadata, Optional.empty());

    // Assert
    Map<String, ObjectStorageRecord> resultRecords = partition.getRecords();
    assertThat(resultRecords).hasSize(1);
    ObjectStorageRecord record = resultRecords.values().iterator().next();
    assertThat(record.getValues()).containsEntry(COLUMN_NAME_1, INT_VALUE_2);
    assertThat(record.getValues()).containsEntry(COLUMN_NAME_2, INT_VALUE_2);
  }

  @Test
  public void applyPut_WithPutIfNotExistsAndRecordDoesNotExist_ShouldInsertRecord()
      throws Exception {
    // Arrange
    ObjectStoragePartition partition = createObjectStoragePartition(new HashMap<>());
    Put put =
        Put.newBuilder(createPut(INT_VALUE_1)).condition(ConditionBuilder.putIfNotExists()).build();

    // Act
    partition.applyPut(put, metadata, Optional.empty());

    // Assert
    assertThat(partition.getRecords()).hasSize(1);
  }

  @Test
  public void applyPut_WithPutIfNotExistsAndRecordExists_ShouldThrowNoMutationException() {
    // Arrange
    Map<String, ObjectStorageRecord> records = new HashMap<>();
    String recordId =
        PARTITION_KEY_VALUE + ObjectStorageUtils.CONCATENATED_KEY_DELIMITER + CLUSTERING_KEY_VALUE;
    records.put(recordId, createRecord(recordId, INT_VALUE_1));
    ObjectStoragePartition partition = createObjectStoragePartition(records);
    Put put =
        Put.newBuilder(createPut(INT_VALUE_1)).condition(ConditionBuilder.putIfNotExists()).build();

    // Act & Assert
    assertThatThrownBy(() -> partition.applyPut(put, metadata, Optional.empty()))
        .isInstanceOf(NoMutationException.class);
  }

  @Test
  public void applyPut_WithPutIfExistsAndRecordExists_ShouldUpdateRecord() throws Exception {
    // Arrange
    Map<String, ObjectStorageRecord> records = new HashMap<>();
    String recordId =
        PARTITION_KEY_VALUE + ObjectStorageUtils.CONCATENATED_KEY_DELIMITER + CLUSTERING_KEY_VALUE;
    records.put(recordId, createRecord(recordId, INT_VALUE_1));
    ObjectStoragePartition partition = createObjectStoragePartition(records);
    Put put =
        Put.newBuilder(createPut(INT_VALUE_2)).condition(ConditionBuilder.putIfExists()).build();

    // Act
    partition.applyPut(put, metadata, Optional.empty());

    // Assert
    Map<String, ObjectStorageRecord> resultRecords = partition.getRecords();
    assertThat(resultRecords).hasSize(1);
    assertThat(resultRecords.values().iterator().next().getValues())
        .containsEntry(COLUMN_NAME_1, INT_VALUE_2);
  }

  @Test
  public void applyPut_WithPutIfExistsAndRecordDoesNotExist_ShouldThrowNoMutationException() {
    // Arrange
    ObjectStoragePartition partition = createObjectStoragePartition(new HashMap<>());
    Put put =
        Put.newBuilder(createPut(INT_VALUE_1)).condition(ConditionBuilder.putIfExists()).build();

    // Act & Assert
    assertThatThrownBy(() -> partition.applyPut(put, metadata, Optional.empty()))
        .isInstanceOf(NoMutationException.class);
  }

  @Test
  public void applyPut_WithPutIfAndConditionMatches_ShouldUpdateRecord() throws Exception {
    // Arrange
    Map<String, ObjectStorageRecord> records = new HashMap<>();
    String recordId =
        PARTITION_KEY_VALUE + ObjectStorageUtils.CONCATENATED_KEY_DELIMITER + CLUSTERING_KEY_VALUE;
    records.put(recordId, createRecord(recordId, INT_VALUE_1));
    ObjectStoragePartition partition = createObjectStoragePartition(records);
    Put put =
        Put.newBuilder(createPut(INT_VALUE_2))
            .condition(
                ConditionBuilder.putIf(
                        ConditionBuilder.column(COLUMN_NAME_1).isEqualToInt(INT_VALUE_1))
                    .build())
            .build();

    // Act
    partition.applyPut(put, metadata, Optional.empty());

    // Assert
    Map<String, ObjectStorageRecord> resultRecords = partition.getRecords();
    assertThat(resultRecords).hasSize(1);
    assertThat(resultRecords.values().iterator().next().getValues())
        .containsEntry(COLUMN_NAME_1, INT_VALUE_2);
  }

  @Test
  public void applyPut_WithPutIfAndConditionDoesNotMatch_ShouldThrowNoMutationException() {
    // Arrange
    Map<String, ObjectStorageRecord> records = new HashMap<>();
    String recordId =
        PARTITION_KEY_VALUE + ObjectStorageUtils.CONCATENATED_KEY_DELIMITER + CLUSTERING_KEY_VALUE;
    records.put(recordId, createRecord(recordId, INT_VALUE_1));
    ObjectStoragePartition partition = createObjectStoragePartition(records);
    Put put =
        Put.newBuilder(createPut(INT_VALUE_2))
            .condition(
                ConditionBuilder.putIf(
                        ConditionBuilder.column(COLUMN_NAME_1).isEqualToInt(INT_VALUE_2))
                    .build())
            .build();

    // Act & Assert
    assertThatThrownBy(() -> partition.applyPut(put, metadata, Optional.empty()))
        .isInstanceOf(NoMutationException.class);
  }

  @Test
  public void applyPut_WithPutIfAndRecordDoesNotExist_ShouldThrowNoMutationException() {
    // Arrange
    ObjectStoragePartition partition = createObjectStoragePartition(new HashMap<>());
    Put put =
        Put.newBuilder(createPut(INT_VALUE_1))
            .condition(
                ConditionBuilder.putIf(
                        ConditionBuilder.column(COLUMN_NAME_1).isEqualToInt(INT_VALUE_1))
                    .build())
            .build();

    // Act & Assert
    assertThatThrownBy(() -> partition.applyPut(put, metadata, Optional.empty()))
        .isInstanceOf(NoMutationException.class);
  }

  @Test
  public void applyDelete_WithoutConditionAndRecordExists_ShouldRemoveRecord() throws Exception {
    // Arrange
    Map<String, ObjectStorageRecord> records = new HashMap<>();
    String recordId =
        PARTITION_KEY_VALUE + ObjectStorageUtils.CONCATENATED_KEY_DELIMITER + CLUSTERING_KEY_VALUE;
    records.put(recordId, createRecord(recordId, INT_VALUE_1));
    ObjectStoragePartition partition = createObjectStoragePartition(records);
    Delete delete = createDelete();

    // Act
    partition.applyDelete(delete, metadata, Optional.empty());

    // Assert
    assertThat(partition.isEmpty()).isTrue();
  }

  @Test
  public void applyDelete_WithoutConditionAndRecordDoesNotExist_ShouldDoNothing() throws Exception {
    // Arrange
    ObjectStoragePartition partition = createObjectStoragePartition(new HashMap<>());
    Delete delete = createDelete();

    // Act
    partition.applyDelete(delete, metadata, Optional.empty());

    // Assert
    assertThat(partition.isEmpty()).isTrue();
  }

  @Test
  public void applyDelete_WithDeleteIfExistsAndRecordExists_ShouldRemoveRecord() throws Exception {
    // Arrange
    Map<String, ObjectStorageRecord> records = new HashMap<>();
    String recordId =
        PARTITION_KEY_VALUE + ObjectStorageUtils.CONCATENATED_KEY_DELIMITER + CLUSTERING_KEY_VALUE;
    records.put(recordId, createRecord(recordId, INT_VALUE_1));
    ObjectStoragePartition partition = createObjectStoragePartition(records);
    Delete delete =
        Delete.newBuilder(createDelete()).condition(ConditionBuilder.deleteIfExists()).build();

    // Act
    partition.applyDelete(delete, metadata, Optional.empty());

    // Assert
    assertThat(partition.isEmpty()).isTrue();
  }

  @Test
  public void applyDelete_WithDeleteIfExistsAndRecordDoesNotExist_ShouldThrowNoMutationException() {
    // Arrange
    ObjectStoragePartition partition = createObjectStoragePartition(new HashMap<>());
    Delete delete =
        Delete.newBuilder(createDelete()).condition(ConditionBuilder.deleteIfExists()).build();

    // Act & Assert
    assertThatThrownBy(() -> partition.applyDelete(delete, metadata, Optional.empty()))
        .isInstanceOf(NoMutationException.class);
  }

  @Test
  public void applyDelete_WithDeleteIfAndConditionMatches_ShouldRemoveRecord() throws Exception {
    // Arrange
    Map<String, ObjectStorageRecord> records = new HashMap<>();
    String recordId =
        PARTITION_KEY_VALUE + ObjectStorageUtils.CONCATENATED_KEY_DELIMITER + CLUSTERING_KEY_VALUE;
    records.put(recordId, createRecord(recordId, INT_VALUE_1));
    ObjectStoragePartition partition = createObjectStoragePartition(records);
    Delete delete =
        Delete.newBuilder(createDelete())
            .condition(
                ConditionBuilder.deleteIf(
                        ConditionBuilder.column(COLUMN_NAME_1).isEqualToInt(INT_VALUE_1))
                    .build())
            .build();

    // Act
    partition.applyDelete(delete, metadata, Optional.empty());

    // Assert
    assertThat(partition.isEmpty()).isTrue();
  }

  @Test
  public void applyDelete_WithDeleteIfAndConditionDoesNotMatch_ShouldThrowNoMutationException() {
    // Arrange
    Map<String, ObjectStorageRecord> records = new HashMap<>();
    String recordId =
        PARTITION_KEY_VALUE + ObjectStorageUtils.CONCATENATED_KEY_DELIMITER + CLUSTERING_KEY_VALUE;
    records.put(recordId, createRecord(recordId, INT_VALUE_1));
    ObjectStoragePartition partition = createObjectStoragePartition(records);
    Delete delete =
        Delete.newBuilder(createDelete())
            .condition(
                ConditionBuilder.deleteIf(
                        ConditionBuilder.column(COLUMN_NAME_1).isEqualToInt(INT_VALUE_2))
                    .build())
            .build();

    // Act & Assert
    assertThatThrownBy(() -> partition.applyDelete(delete, metadata, Optional.empty()))
        .isInstanceOf(NoMutationException.class);
  }

  @Test
  public void applyDelete_WithDeleteIfAndRecordDoesNotExist_ShouldThrowNoMutationException() {
    // Arrange
    ObjectStoragePartition partition = createObjectStoragePartition(new HashMap<>());
    Delete delete =
        Delete.newBuilder(createDelete())
            .condition(
                ConditionBuilder.deleteIf(
                        ConditionBuilder.column(COLUMN_NAME_1).isEqualToInt(INT_VALUE_1))
                    .build())
            .build();

    // Act & Assert
    assertThatThrownBy(() -> partition.applyDelete(delete, metadata, Optional.empty()))
        .isInstanceOf(NoMutationException.class);
  }

  private ObjectStoragePartition createObjectStoragePartition(
      Map<String, ObjectStorageRecord> records) {
    return new ObjectStoragePartition(records);
  }

  private ObjectStorageRecord createRecord(String recordId, int value) {
    Map<String, Object> values = new HashMap<>();
    values.put(COLUMN_NAME_1, value);
    values.put(COLUMN_NAME_2, INT_VALUE_2);
    return ObjectStorageRecord.newBuilder()
        .id(recordId)
        .partitionKey(Collections.singletonMap(PARTITION_KEY_NAME, PARTITION_KEY_VALUE))
        .clusteringKey(Collections.singletonMap(CLUSTERING_KEY_NAME, CLUSTERING_KEY_VALUE))
        .values(values)
        .build();
  }

  private Put createPut(int value1) {
    return Put.newBuilder()
        .namespace(NAMESPACE)
        .table(TABLE)
        .partitionKey(Key.ofText(PARTITION_KEY_NAME, PARTITION_KEY_VALUE))
        .clusteringKey(
            Key.ofText(CLUSTERING_KEY_NAME, ObjectStoragePartitionTest.CLUSTERING_KEY_VALUE))
        .intValue(COLUMN_NAME_1, value1)
        .intValue(COLUMN_NAME_2, ObjectStoragePartitionTest.INT_VALUE_2)
        .build();
  }

  private Delete createDelete() {
    return Delete.newBuilder()
        .namespace(NAMESPACE)
        .table(TABLE)
        .partitionKey(Key.ofText(PARTITION_KEY_NAME, PARTITION_KEY_VALUE))
        .clusteringKey(
            Key.ofText(CLUSTERING_KEY_NAME, ObjectStoragePartitionTest.CLUSTERING_KEY_VALUE))
        .build();
  }

  @Test
  public void areConditionsMet_WithEqConditionAndSameValue_ShouldReturnTrue() {
    // Arrange
    ObjectStoragePartition partition = createObjectStoragePartition(new HashMap<>());
    ObjectStorageRecord record = createRecordForConditionTest(INT_VALUE_1);
    ConditionalExpression condition =
        ConditionBuilder.column(COLUMN_NAME_1).isEqualToInt(INT_VALUE_1);

    // Act
    boolean result =
        partition.areConditionsMet(
            record, Collections.singletonList(condition), metadata, Optional.empty());

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void areConditionsMet_WithEqConditionAndDifferentValue_ShouldReturnFalse() {
    // Arrange
    ObjectStoragePartition partition = createObjectStoragePartition(new HashMap<>());
    ObjectStorageRecord record = createRecordForConditionTest(INT_VALUE_1);
    ConditionalExpression condition =
        ConditionBuilder.column(COLUMN_NAME_1).isEqualToInt(INT_VALUE_2);

    // Act
    boolean result =
        partition.areConditionsMet(
            record, Collections.singletonList(condition), metadata, Optional.empty());

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void areConditionsMet_WithEqConditionAndNullValue_ShouldReturnFalse() {
    // Arrange
    ObjectStoragePartition partition = createObjectStoragePartition(new HashMap<>());
    ObjectStorageRecord record = createRecordForConditionTestWithNull();
    ConditionalExpression condition =
        ConditionBuilder.column(COLUMN_NAME_1).isEqualToInt(INT_VALUE_1);

    // Act
    boolean result =
        partition.areConditionsMet(
            record, Collections.singletonList(condition), metadata, Optional.empty());

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void areConditionsMet_WithNeConditionAndDifferentValue_ShouldReturnTrue() {
    // Arrange
    ObjectStoragePartition partition = createObjectStoragePartition(new HashMap<>());
    ObjectStorageRecord record = createRecordForConditionTest(INT_VALUE_1);
    ConditionalExpression condition =
        ConditionBuilder.column(COLUMN_NAME_1).isNotEqualToInt(INT_VALUE_2);

    // Act
    boolean result =
        partition.areConditionsMet(
            record, Collections.singletonList(condition), metadata, Optional.empty());

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void areConditionsMet_WithNeConditionAndSameValue_ShouldReturnFalse() {
    // Arrange
    ObjectStoragePartition partition = createObjectStoragePartition(new HashMap<>());
    ObjectStorageRecord record = createRecordForConditionTest(INT_VALUE_1);
    ConditionalExpression condition =
        ConditionBuilder.column(COLUMN_NAME_1).isNotEqualToInt(INT_VALUE_1);

    // Act
    boolean result =
        partition.areConditionsMet(
            record, Collections.singletonList(condition), metadata, Optional.empty());

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void areConditionsMet_WithGtConditionAndGreaterValue_ShouldReturnTrue() {
    // Arrange
    ObjectStoragePartition partition = createObjectStoragePartition(new HashMap<>());
    ObjectStorageRecord record = createRecordForConditionTest(INT_VALUE_2);
    ConditionalExpression condition =
        ConditionBuilder.column(COLUMN_NAME_1).isGreaterThanInt(INT_VALUE_1);

    // Act
    boolean result =
        partition.areConditionsMet(
            record, Collections.singletonList(condition), metadata, Optional.empty());

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void areConditionsMet_WithGtConditionAndSameValue_ShouldReturnFalse() {
    // Arrange
    ObjectStoragePartition partition = createObjectStoragePartition(new HashMap<>());
    ObjectStorageRecord record = createRecordForConditionTest(INT_VALUE_1);
    ConditionalExpression condition =
        ConditionBuilder.column(COLUMN_NAME_1).isGreaterThanInt(INT_VALUE_1);

    // Act
    boolean result =
        partition.areConditionsMet(
            record, Collections.singletonList(condition), metadata, Optional.empty());

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void areConditionsMet_WithGteConditionAndGreaterValue_ShouldReturnTrue() {
    // Arrange
    ObjectStoragePartition partition = createObjectStoragePartition(new HashMap<>());
    ObjectStorageRecord record = createRecordForConditionTest(INT_VALUE_2);
    ConditionalExpression condition =
        ConditionBuilder.column(COLUMN_NAME_1).isGreaterThanOrEqualToInt(INT_VALUE_1);

    // Act
    boolean result =
        partition.areConditionsMet(
            record, Collections.singletonList(condition), metadata, Optional.empty());

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void areConditionsMet_WithGteConditionAndSameValue_ShouldReturnTrue() {
    // Arrange
    ObjectStoragePartition partition = createObjectStoragePartition(new HashMap<>());
    ObjectStorageRecord record = createRecordForConditionTest(INT_VALUE_1);
    ConditionalExpression condition =
        ConditionBuilder.column(COLUMN_NAME_1).isGreaterThanOrEqualToInt(INT_VALUE_1);

    // Act
    boolean result =
        partition.areConditionsMet(
            record, Collections.singletonList(condition), metadata, Optional.empty());

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void areConditionsMet_WithGteConditionAndSmallerValue_ShouldReturnFalse() {
    // Arrange
    ObjectStoragePartition partition = createObjectStoragePartition(new HashMap<>());
    ObjectStorageRecord record = createRecordForConditionTest(INT_VALUE_1);
    ConditionalExpression condition =
        ConditionBuilder.column(COLUMN_NAME_1).isGreaterThanOrEqualToInt(INT_VALUE_2);

    // Act
    boolean result =
        partition.areConditionsMet(
            record, Collections.singletonList(condition), metadata, Optional.empty());

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void areConditionsMet_WithLtConditionAndSmallerValue_ShouldReturnTrue() {
    // Arrange
    ObjectStoragePartition partition = createObjectStoragePartition(new HashMap<>());
    ObjectStorageRecord record = createRecordForConditionTest(INT_VALUE_1);
    ConditionalExpression condition =
        ConditionBuilder.column(COLUMN_NAME_1).isLessThanInt(INT_VALUE_2);

    // Act
    boolean result =
        partition.areConditionsMet(
            record, Collections.singletonList(condition), metadata, Optional.empty());

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void areConditionsMet_WithLtConditionAndSameValue_ShouldReturnFalse() {
    // Arrange
    ObjectStoragePartition partition = createObjectStoragePartition(new HashMap<>());
    ObjectStorageRecord record = createRecordForConditionTest(INT_VALUE_1);
    ConditionalExpression condition =
        ConditionBuilder.column(COLUMN_NAME_1).isLessThanInt(INT_VALUE_1);

    // Act
    boolean result =
        partition.areConditionsMet(
            record, Collections.singletonList(condition), metadata, Optional.empty());

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void areConditionsMet_WithLteConditionAndSmallerValue_ShouldReturnTrue() {
    // Arrange
    ObjectStoragePartition partition = createObjectStoragePartition(new HashMap<>());
    ObjectStorageRecord record = createRecordForConditionTest(INT_VALUE_1);
    ConditionalExpression condition =
        ConditionBuilder.column(COLUMN_NAME_1).isLessThanOrEqualToInt(INT_VALUE_2);

    // Act
    boolean result =
        partition.areConditionsMet(
            record, Collections.singletonList(condition), metadata, Optional.empty());

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void areConditionsMet_WithLteConditionAndSameValue_ShouldReturnTrue() {
    // Arrange
    ObjectStoragePartition partition = createObjectStoragePartition(new HashMap<>());
    ObjectStorageRecord record = createRecordForConditionTest(INT_VALUE_1);
    ConditionalExpression condition =
        ConditionBuilder.column(COLUMN_NAME_1).isLessThanOrEqualToInt(INT_VALUE_1);

    // Act
    boolean result =
        partition.areConditionsMet(
            record, Collections.singletonList(condition), metadata, Optional.empty());

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void areConditionsMet_WithLteConditionAndGreaterValue_ShouldReturnFalse() {
    // Arrange
    ObjectStoragePartition partition = createObjectStoragePartition(new HashMap<>());
    ObjectStorageRecord record = createRecordForConditionTest(INT_VALUE_2);
    ConditionalExpression condition =
        ConditionBuilder.column(COLUMN_NAME_1).isLessThanOrEqualToInt(INT_VALUE_1);

    // Act
    boolean result =
        partition.areConditionsMet(
            record, Collections.singletonList(condition), metadata, Optional.empty());

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void areConditionsMet_WithIsNullConditionAndNullValue_ShouldReturnTrue() {
    // Arrange
    ObjectStoragePartition partition = createObjectStoragePartition(new HashMap<>());
    ObjectStorageRecord record = createRecordForConditionTestWithNull();
    ConditionalExpression condition = ConditionBuilder.column(COLUMN_NAME_1).isNullInt();

    // Act
    boolean result =
        partition.areConditionsMet(
            record, Collections.singletonList(condition), metadata, Optional.empty());

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void areConditionsMet_WithIsNullConditionAndNonNullValue_ShouldReturnFalse() {
    // Arrange
    ObjectStoragePartition partition = createObjectStoragePartition(new HashMap<>());
    ObjectStorageRecord record = createRecordForConditionTest(INT_VALUE_1);
    ConditionalExpression condition = ConditionBuilder.column(COLUMN_NAME_1).isNullInt();

    // Act
    boolean result =
        partition.areConditionsMet(
            record, Collections.singletonList(condition), metadata, Optional.empty());

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void areConditionsMet_WithIsNotNullConditionAndNonNullValue_ShouldReturnTrue() {
    // Arrange
    ObjectStoragePartition partition = createObjectStoragePartition(new HashMap<>());
    ObjectStorageRecord record = createRecordForConditionTest(INT_VALUE_1);
    ConditionalExpression condition = ConditionBuilder.column(COLUMN_NAME_1).isNotNullInt();

    // Act
    boolean result =
        partition.areConditionsMet(
            record, Collections.singletonList(condition), metadata, Optional.empty());

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void areConditionsMet_WithIsNotNullConditionAndNullValue_ShouldReturnFalse() {
    // Arrange
    ObjectStoragePartition partition = createObjectStoragePartition(new HashMap<>());
    ObjectStorageRecord record = createRecordForConditionTestWithNull();
    ConditionalExpression condition = ConditionBuilder.column(COLUMN_NAME_1).isNotNullInt();

    // Act
    boolean result =
        partition.areConditionsMet(
            record, Collections.singletonList(condition), metadata, Optional.empty());

    // Assert
    assertThat(result).isFalse();
  }

  // ---- Collation-aware range operators on TEXT ----

  private static CollationComparator caseInsensitiveIcuCollation() {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, "localhost");
    props.setProperty(DatabaseConfig.COLLATION, "ICU");
    props.setProperty(DatabaseConfig.COLLATION_ICU_STRENGTH, "PRIMARY");
    return CollationComparator.from(new DatabaseConfig(props)).get();
  }

  private static CollationComparator binaryCollation() {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, "localhost");
    props.setProperty(DatabaseConfig.COLLATION, "BINARY");
    return CollationComparator.from(new DatabaseConfig(props)).get();
  }

  private ObjectStorageRecord createTextRecordForConditionTest(String value) {
    return ObjectStorageRecord.newBuilder()
        .id(RECORD_ID_1)
        .partitionKey(new HashMap<>())
        .clusteringKey(new HashMap<>())
        .values(Collections.singletonMap(COLUMN_NAME_1, value))
        .build();
  }

  @Test
  public void
      areConditionsMet_WithCaseInsensitiveCollationAndGtConditionOnText_ShouldReturnTrueWhereNaturalWouldReturnFalse() {
    // Arrange: actual TEXT "B", condition col > "a". Natural UTF-16/byte order has 'B'(0x42) <
    // 'a'(0x61), so under a case-insensitive ICU collation ('b' > 'a') the condition is met.
    when(metadata.getColumnDataType(COLUMN_NAME_1)).thenReturn(DataType.TEXT);
    ObjectStoragePartition partition = createObjectStoragePartition(new HashMap<>());
    ObjectStorageRecord record = createTextRecordForConditionTest("B");
    ConditionalExpression condition = ConditionBuilder.column(COLUMN_NAME_1).isGreaterThanText("a");

    // Act
    boolean result =
        partition.areConditionsMet(
            record,
            Collections.singletonList(condition),
            metadata,
            Optional.of(caseInsensitiveIcuCollation()));

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void areConditionsMet_WithoutCollationAndGtConditionOnText_ShouldReturnFalse() {
    // Arrange: unset collation keeps natural order: 'B'(0x42) < 'a'(0x61), so col > "a" is not met.
    when(metadata.getColumnDataType(COLUMN_NAME_1)).thenReturn(DataType.TEXT);
    ObjectStoragePartition partition = createObjectStoragePartition(new HashMap<>());
    ObjectStorageRecord record = createTextRecordForConditionTest("B");
    ConditionalExpression condition = ConditionBuilder.column(COLUMN_NAME_1).isGreaterThanText("a");

    // Act
    boolean result =
        partition.areConditionsMet(
            record, Collections.singletonList(condition), metadata, Optional.empty());

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void areConditionsMet_WithBinaryCollationAndEqConditionOnText_ShouldStayByteExact() {
    // Arrange: BINARY collation equality is byte-exact: "B" != "b".
    when(metadata.getColumnDataType(COLUMN_NAME_1)).thenReturn(DataType.TEXT);
    ObjectStoragePartition partition = createObjectStoragePartition(new HashMap<>());
    ObjectStorageRecord record = createTextRecordForConditionTest("B");
    ConditionalExpression condition = ConditionBuilder.column(COLUMN_NAME_1).isEqualToText("b");

    // Act
    boolean result =
        partition.areConditionsMet(
            record, Collections.singletonList(condition), metadata, Optional.of(binaryCollation()));

    // Assert
    assertThat(result).isFalse();
  }

  // ---- Collation-aware EQ/NE on TEXT (equality follows the collation) ----

  @Test
  public void
      areConditionsMet_WithCaseInsensitiveCollationAndEqConditionOnText_ShouldReturnTrueWhereByteExactWould() {
    // Arrange: actual TEXT "Apple", condition col = "apple". Under a case-insensitive ICU collation
    // "Apple" collate-equals "apple" so the condition is met.
    when(metadata.getColumnDataType(COLUMN_NAME_1)).thenReturn(DataType.TEXT);
    ObjectStoragePartition partition = createObjectStoragePartition(new HashMap<>());
    ObjectStorageRecord record = createTextRecordForConditionTest("Apple");
    ConditionalExpression condition = ConditionBuilder.column(COLUMN_NAME_1).isEqualToText("apple");

    // Act
    boolean result =
        partition.areConditionsMet(
            record,
            Collections.singletonList(condition),
            metadata,
            Optional.of(caseInsensitiveIcuCollation()));

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void areConditionsMet_WithoutCollationAndEqConditionOnText_ShouldStayByteExact() {
    // Arrange: unset collation keeps byte-exact equality: "Apple" != "apple".
    when(metadata.getColumnDataType(COLUMN_NAME_1)).thenReturn(DataType.TEXT);
    ObjectStoragePartition partition = createObjectStoragePartition(new HashMap<>());
    ObjectStorageRecord record = createTextRecordForConditionTest("Apple");
    ConditionalExpression condition = ConditionBuilder.column(COLUMN_NAME_1).isEqualToText("apple");

    // Act
    boolean result =
        partition.areConditionsMet(
            record, Collections.singletonList(condition), metadata, Optional.empty());

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void
      areConditionsMet_WithCaseInsensitiveCollationAndNeConditionOnText_ShouldBehaveAsNegationOfEq() {
    // Arrange: actual TEXT "Apple", condition col != "apple". Under a case-insensitive ICU
    // collation
    // "Apple" collate-equals "apple", so the NE condition is not met (exact negation of EQ).
    when(metadata.getColumnDataType(COLUMN_NAME_1)).thenReturn(DataType.TEXT);
    ObjectStoragePartition partition = createObjectStoragePartition(new HashMap<>());
    ObjectStorageRecord record = createTextRecordForConditionTest("Apple");
    ConditionalExpression condition =
        ConditionBuilder.column(COLUMN_NAME_1).isNotEqualToText("apple");

    // Act
    boolean result =
        partition.areConditionsMet(
            record,
            Collections.singletonList(condition),
            metadata,
            Optional.of(caseInsensitiveIcuCollation()));

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void areConditionsMet_WithCollationAndEqConditionOnNonText_ShouldStayNatural() {
    // Arrange: non-TEXT (INT) EQ is unaffected by the collation; different values do not match.
    ObjectStoragePartition partition = createObjectStoragePartition(new HashMap<>());
    ObjectStorageRecord record = createRecordForConditionTest(INT_VALUE_1);
    ConditionalExpression condition =
        ConditionBuilder.column(COLUMN_NAME_1).isEqualToInt(INT_VALUE_2);

    // Act
    boolean result =
        partition.areConditionsMet(
            record,
            Collections.singletonList(condition),
            metadata,
            Optional.of(caseInsensitiveIcuCollation()));

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void areConditionsMet_WithCollationAndEqConditionAndNullTextValue_ShouldReturnFalse() {
    // Arrange: null actual TEXT value with EQ stays byte-exact (no NPE) and returns false.
    when(metadata.getColumnDataType(COLUMN_NAME_1)).thenReturn(DataType.TEXT);
    ObjectStoragePartition partition = createObjectStoragePartition(new HashMap<>());
    ObjectStorageRecord record = createTextRecordForConditionTest(null);
    ConditionalExpression condition = ConditionBuilder.column(COLUMN_NAME_1).isEqualToText("apple");

    // Act
    boolean result =
        partition.areConditionsMet(
            record,
            Collections.singletonList(condition),
            metadata,
            Optional.of(caseInsensitiveIcuCollation()));

    // Assert
    assertThat(result).isFalse();
  }

  private ObjectStorageRecord createRecordForConditionTest(int value) {
    return ObjectStorageRecord.newBuilder()
        .id(RECORD_ID_1)
        .partitionKey(new HashMap<>())
        .clusteringKey(new HashMap<>())
        .values(Collections.singletonMap(COLUMN_NAME_1, value))
        .build();
  }

  private ObjectStorageRecord createRecordForConditionTestWithNull() {
    return ObjectStorageRecord.newBuilder()
        .id(RECORD_ID_1)
        .partitionKey(new HashMap<>())
        .clusteringKey(new HashMap<>())
        .values(Collections.singletonMap(COLUMN_NAME_1, null))
        .build();
  }
}
