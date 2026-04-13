package com.scalar.db.storage.objectstorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ParquetSerializerTest {

  @Mock private TableMetadata metadata;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  private void setupBasicMetadata() {
    when(metadata.getPartitionKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList("pk")));
    when(metadata.getClusteringKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList("ck")));
    when(metadata.getColumnNames())
        .thenReturn(new LinkedHashSet<>(Arrays.asList("pk", "ck", "col1")));
    when(metadata.getColumnDataType("pk")).thenReturn(DataType.TEXT);
    when(metadata.getColumnDataType("ck")).thenReturn(DataType.TEXT);
    when(metadata.getColumnDataType("col1")).thenReturn(DataType.INT);
  }

  @Test
  public void serializeDeserialize_SingleRecord_ShouldRoundTrip() {
    // Arrange
    setupBasicMetadata();
    Map<String, ObjectStorageRecord> records = new HashMap<>();
    ObjectStorageRecord record =
        ObjectStorageRecord.newBuilder()
            .id("pk1:ck1")
            .partitionKey(Collections.singletonMap("pk", "pk1"))
            .clusteringKey(Collections.singletonMap("ck", "ck1"))
            .values(Collections.singletonMap("col1", 42))
            .build();
    records.put("pk1:ck1", record);
    ObjectStoragePartition partition = new ObjectStoragePartition(records);

    // Act
    byte[] serialized = ParquetSerializer.serialize(partition, metadata);
    ObjectStoragePartition deserialized = ParquetSerializer.deserialize(serialized, metadata);

    // Assert
    assertThat(deserialized.getRecords()).hasSize(1);
    Optional<ObjectStorageRecord> result = deserialized.getRecord("pk1:ck1");
    assertThat(result).isPresent();
    assertThat(result.get().getId()).isEqualTo("pk1:ck1");
    assertThat(result.get().getPartitionKey()).containsEntry("pk", "pk1");
    assertThat(result.get().getClusteringKey()).containsEntry("ck", "ck1");
    assertThat(result.get().getValues()).containsEntry("col1", 42);
  }

  @Test
  public void serializeDeserialize_MultipleRecords_ShouldRoundTrip() {
    // Arrange
    setupBasicMetadata();
    Map<String, ObjectStorageRecord> records = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      String id = "pk1:ck" + i;
      ObjectStorageRecord record =
          ObjectStorageRecord.newBuilder()
              .id(id)
              .partitionKey(Collections.singletonMap("pk", "pk1"))
              .clusteringKey(Collections.singletonMap("ck", "ck" + i))
              .values(Collections.singletonMap("col1", i * 10))
              .build();
      records.put(id, record);
    }
    ObjectStoragePartition partition = new ObjectStoragePartition(records);

    // Act
    byte[] serialized = ParquetSerializer.serialize(partition, metadata);
    ObjectStoragePartition deserialized = ParquetSerializer.deserialize(serialized, metadata);

    // Assert
    assertThat(deserialized.getRecords()).hasSize(5);
    for (int i = 0; i < 5; i++) {
      String id = "pk1:ck" + i;
      Optional<ObjectStorageRecord> result = deserialized.getRecord(id);
      assertThat(result).isPresent();
      assertThat(result.get().getValues()).containsEntry("col1", i * 10);
    }
  }

  @Test
  public void serializeDeserialize_EmptyPartition_ShouldRoundTrip() {
    // Arrange
    setupBasicMetadata();
    ObjectStoragePartition partition = new ObjectStoragePartition(new HashMap<>());

    // Act
    byte[] serialized = ParquetSerializer.serialize(partition, metadata);
    ObjectStoragePartition deserialized = ParquetSerializer.deserialize(serialized, metadata);

    // Assert
    assertThat(deserialized.getRecords()).isEmpty();
    assertThat(deserialized.isEmpty()).isTrue();
  }

  @Test
  public void serializeDeserialize_NullValues_ShouldRoundTrip() {
    // Arrange
    setupBasicMetadata();
    Map<String, ObjectStorageRecord> records = new HashMap<>();
    Map<String, Object> values = new HashMap<>();
    values.put("col1", null);
    ObjectStorageRecord record =
        ObjectStorageRecord.newBuilder()
            .id("pk1:ck1")
            .partitionKey(Collections.singletonMap("pk", "pk1"))
            .clusteringKey(Collections.singletonMap("ck", "ck1"))
            .values(values)
            .build();
    records.put("pk1:ck1", record);
    ObjectStoragePartition partition = new ObjectStoragePartition(records);

    // Act
    byte[] serialized = ParquetSerializer.serialize(partition, metadata);
    ObjectStoragePartition deserialized = ParquetSerializer.deserialize(serialized, metadata);

    // Assert
    assertThat(deserialized.getRecords()).hasSize(1);
    Optional<ObjectStorageRecord> result = deserialized.getRecord("pk1:ck1");
    assertThat(result).isPresent();
    assertThat(result.get().getValues().get("col1")).isNull();
  }

  @Test
  public void serializeDeserialize_AllDataTypes_ShouldRoundTrip() {
    // Arrange
    when(metadata.getPartitionKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList("pk")));
    when(metadata.getClusteringKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList("ck")));
    when(metadata.getColumnNames())
        .thenReturn(
            new LinkedHashSet<>(
                Arrays.asList(
                    "pk",
                    "ck",
                    "bool_col",
                    "int_col",
                    "bigint_col",
                    "float_col",
                    "double_col",
                    "text_col",
                    "blob_col",
                    "date_col",
                    "time_col",
                    "timestamp_col",
                    "timestamptz_col")));
    when(metadata.getColumnDataType("pk")).thenReturn(DataType.TEXT);
    when(metadata.getColumnDataType("ck")).thenReturn(DataType.TEXT);
    when(metadata.getColumnDataType("bool_col")).thenReturn(DataType.BOOLEAN);
    when(metadata.getColumnDataType("int_col")).thenReturn(DataType.INT);
    when(metadata.getColumnDataType("bigint_col")).thenReturn(DataType.BIGINT);
    when(metadata.getColumnDataType("float_col")).thenReturn(DataType.FLOAT);
    when(metadata.getColumnDataType("double_col")).thenReturn(DataType.DOUBLE);
    when(metadata.getColumnDataType("text_col")).thenReturn(DataType.TEXT);
    when(metadata.getColumnDataType("blob_col")).thenReturn(DataType.BLOB);
    when(metadata.getColumnDataType("date_col")).thenReturn(DataType.DATE);
    when(metadata.getColumnDataType("time_col")).thenReturn(DataType.TIME);
    when(metadata.getColumnDataType("timestamp_col")).thenReturn(DataType.TIMESTAMP);
    when(metadata.getColumnDataType("timestamptz_col")).thenReturn(DataType.TIMESTAMPTZ);

    Map<String, Object> values = new HashMap<>();
    values.put("bool_col", true);
    values.put("int_col", 42);
    values.put("bigint_col", 123456789L);
    values.put("float_col", 1.23f);
    values.put("double_col", 4.56789);
    values.put("text_col", "hello world");
    values.put("blob_col", new byte[] {1, 2, 3, 4, 5});
    values.put("date_col", 19000); // encoded date
    values.put("time_col", 43200000000000L); // encoded time
    values.put("timestamp_col", 1700000000000L); // encoded timestamp
    values.put("timestamptz_col", 1700000000000L); // encoded timestamptz

    Map<String, ObjectStorageRecord> records = new HashMap<>();
    ObjectStorageRecord record =
        ObjectStorageRecord.newBuilder()
            .id("pk1:ck1")
            .partitionKey(Collections.singletonMap("pk", "pk1"))
            .clusteringKey(Collections.singletonMap("ck", "ck1"))
            .values(values)
            .build();
    records.put("pk1:ck1", record);
    ObjectStoragePartition partition = new ObjectStoragePartition(records);

    // Act
    byte[] serialized = ParquetSerializer.serialize(partition, metadata);
    ObjectStoragePartition deserialized = ParquetSerializer.deserialize(serialized, metadata);

    // Assert
    assertThat(deserialized.getRecords()).hasSize(1);
    ObjectStorageRecord result = deserialized.getRecord("pk1:ck1").get();
    Map<String, Object> resultValues = result.getValues();
    assertThat(resultValues.get("bool_col")).isEqualTo(true);
    assertThat(resultValues.get("int_col")).isEqualTo(42);
    assertThat(resultValues.get("bigint_col")).isEqualTo(123456789L);
    assertThat(((Number) resultValues.get("float_col")).floatValue()).isEqualTo(1.23f);
    assertThat(resultValues.get("double_col")).isEqualTo(4.56789);
    assertThat(resultValues.get("text_col")).isEqualTo("hello world");
    assertThat((byte[]) resultValues.get("blob_col")).isEqualTo(new byte[] {1, 2, 3, 4, 5});
    assertThat(resultValues.get("date_col")).isEqualTo(19000);
    assertThat(resultValues.get("time_col")).isEqualTo(43200000000000L);
    assertThat(resultValues.get("timestamp_col")).isEqualTo(1700000000000L);
    assertThat(resultValues.get("timestamptz_col")).isEqualTo(1700000000000L);
  }

  @Test
  public void serializeDeserialize_BooleanType_ShouldRoundTrip() {
    // Arrange
    when(metadata.getPartitionKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList("pk")));
    when(metadata.getClusteringKeyNames()).thenReturn(new LinkedHashSet<>());
    when(metadata.getColumnNames())
        .thenReturn(new LinkedHashSet<>(Arrays.asList("pk", "bool_col")));
    when(metadata.getColumnDataType("pk")).thenReturn(DataType.TEXT);
    when(metadata.getColumnDataType("bool_col")).thenReturn(DataType.BOOLEAN);

    Map<String, ObjectStorageRecord> records = new HashMap<>();
    ObjectStorageRecord record =
        ObjectStorageRecord.newBuilder()
            .id("pk1")
            .partitionKey(Collections.singletonMap("pk", "pk1"))
            .clusteringKey(Collections.emptyMap())
            .values(Collections.singletonMap("bool_col", false))
            .build();
    records.put("pk1", record);
    ObjectStoragePartition partition = new ObjectStoragePartition(records);

    // Act
    byte[] serialized = ParquetSerializer.serialize(partition, metadata);
    ObjectStoragePartition deserialized = ParquetSerializer.deserialize(serialized, metadata);

    // Assert
    ObjectStorageRecord result = deserialized.getRecord("pk1").get();
    assertThat(result.getValues().get("bool_col")).isEqualTo(false);
  }
}
