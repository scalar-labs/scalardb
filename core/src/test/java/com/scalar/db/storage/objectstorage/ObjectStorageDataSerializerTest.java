package com.scalar.db.storage.objectstorage;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ObjectStorageDataSerializerTest {

  static Stream<Arguments> serializerProvider() {
    return Stream.of(
        Arguments.of(
            "CBOR",
            ObjectStorageDataSerializerFactory.create(
                ObjectStorageFormat.CBOR, ObjectStorageCompression.NONE)),
        Arguments.of(
            "Fory",
            ObjectStorageDataSerializerFactory.create(
                ObjectStorageFormat.FORY, ObjectStorageCompression.NONE)),
        Arguments.of(
            "Ion",
            ObjectStorageDataSerializerFactory.create(
                ObjectStorageFormat.ION, ObjectStorageCompression.NONE)),
        Arguments.of(
            "JSON",
            ObjectStorageDataSerializerFactory.create(
                ObjectStorageFormat.JSON, ObjectStorageCompression.NONE)),
        Arguments.of(
            "Smile",
            ObjectStorageDataSerializerFactory.create(
                ObjectStorageFormat.SMILE, ObjectStorageCompression.NONE)));
  }

  static Stream<Arguments> serializerWithGzipProvider() {
    return Stream.of(
        Arguments.of(
            "CBOR+GZIP",
            ObjectStorageDataSerializerFactory.create(
                ObjectStorageFormat.CBOR, ObjectStorageCompression.GZIP)),
        Arguments.of(
            "Fory+GZIP",
            ObjectStorageDataSerializerFactory.create(
                ObjectStorageFormat.FORY, ObjectStorageCompression.GZIP)),
        Arguments.of(
            "Ion+GZIP",
            ObjectStorageDataSerializerFactory.create(
                ObjectStorageFormat.ION, ObjectStorageCompression.GZIP)),
        Arguments.of(
            "JSON+GZIP",
            ObjectStorageDataSerializerFactory.create(
                ObjectStorageFormat.JSON, ObjectStorageCompression.GZIP)),
        Arguments.of(
            "Smile+GZIP",
            ObjectStorageDataSerializerFactory.create(
                ObjectStorageFormat.SMILE, ObjectStorageCompression.GZIP)));
  }

  static Stream<Arguments> serializerWithZstdProvider() {
    return Stream.of(
        Arguments.of(
            "CBOR+ZSTD",
            ObjectStorageDataSerializerFactory.create(
                ObjectStorageFormat.CBOR, ObjectStorageCompression.ZSTD)),
        Arguments.of(
            "Fory+ZSTD",
            ObjectStorageDataSerializerFactory.create(
                ObjectStorageFormat.FORY, ObjectStorageCompression.ZSTD)),
        Arguments.of(
            "Ion+ZSTD",
            ObjectStorageDataSerializerFactory.create(
                ObjectStorageFormat.ION, ObjectStorageCompression.ZSTD)),
        Arguments.of(
            "JSON+ZSTD",
            ObjectStorageDataSerializerFactory.create(
                ObjectStorageFormat.JSON, ObjectStorageCompression.ZSTD)),
        Arguments.of(
            "Smile+ZSTD",
            ObjectStorageDataSerializerFactory.create(
                ObjectStorageFormat.SMILE, ObjectStorageCompression.ZSTD)));
  }

  static Stream<Arguments> allSerializerProvider() {
    return Stream.of(
            serializerProvider(), serializerWithGzipProvider(), serializerWithZstdProvider())
        .flatMap(s -> s);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("allSerializerProvider")
  void roundTrip_EmptyPartition(String name, ObjectStorageDataSerializer serializer) {
    // Arrange
    ObjectStoragePartition partition = new ObjectStoragePartition(Collections.emptyMap());

    // Act
    byte[] data = serializer.serialize(partition);
    ObjectStoragePartition result = serializer.deserialize(data);

    // Assert
    assertThat(result.isEmpty()).isTrue();
    assertThat(result.getRecords()).isEmpty();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("allSerializerProvider")
  void roundTrip_SingleRecord(String name, ObjectStorageDataSerializer serializer) {
    // Arrange
    Map<String, Object> partitionKey = new HashMap<>();
    partitionKey.put("pk1", "value1");
    Map<String, Object> clusteringKey = new HashMap<>();
    clusteringKey.put("ck1", "value2");
    Map<String, Object> values = new HashMap<>();
    values.put("col1", "text_value");
    values.put("col2", 42);
    values.put("col3", true);

    ObjectStorageRecord record =
        ObjectStorageRecord.newBuilder()
            .id("record1")
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .values(values)
            .build();

    Map<String, ObjectStorageRecord> records = new HashMap<>();
    records.put("record1", record);
    ObjectStoragePartition partition = new ObjectStoragePartition(records);

    // Act
    byte[] data = serializer.serialize(partition);
    ObjectStoragePartition result = serializer.deserialize(data);

    // Assert
    assertThat(result.isEmpty()).isFalse();
    assertThat(result.getRecords()).hasSize(1);
    assertThat(result.getRecord("record1")).isPresent();

    ObjectStorageRecord resultRecord = result.getRecord("record1").get();
    assertThat(resultRecord.getId()).isEqualTo("record1");
    assertThat(resultRecord.getPartitionKey()).containsEntry("pk1", "value1");
    assertThat(resultRecord.getClusteringKey()).containsEntry("ck1", "value2");
    assertThat(resultRecord.getValues()).containsEntry("col1", "text_value");
    assertThat(resultRecord.getValues().get("col3")).isEqualTo(true);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("allSerializerProvider")
  void roundTrip_MultipleRecords(String name, ObjectStorageDataSerializer serializer) {
    // Arrange
    Map<String, ObjectStorageRecord> records = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      String recordId = "record_" + i;
      Map<String, Object> values = new HashMap<>();
      values.put("col1", "value_" + i);
      values.put("col2", i);

      ObjectStorageRecord record =
          ObjectStorageRecord.newBuilder()
              .id(recordId)
              .partitionKey(Collections.singletonMap("pk", "pk_value"))
              .clusteringKey(Collections.singletonMap("ck", "ck_" + i))
              .values(values)
              .build();
      records.put(recordId, record);
    }
    ObjectStoragePartition partition = new ObjectStoragePartition(records);

    // Act
    byte[] data = serializer.serialize(partition);
    ObjectStoragePartition result = serializer.deserialize(data);

    // Assert
    assertThat(result.getRecords()).hasSize(10);
    for (int i = 0; i < 10; i++) {
      String recordId = "record_" + i;
      assertThat(result.getRecord(recordId)).isPresent();
      assertThat(result.getRecord(recordId).get().getValues()).containsEntry("col1", "value_" + i);
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("allSerializerProvider")
  void roundTrip_RecordWithNullValues(String name, ObjectStorageDataSerializer serializer) {
    // Arrange
    Map<String, Object> values = new HashMap<>();
    values.put("col1", null);
    values.put("col2", "non-null");

    ObjectStorageRecord record =
        ObjectStorageRecord.newBuilder()
            .id("record1")
            .partitionKey(Collections.singletonMap("pk", "value"))
            .clusteringKey(Collections.emptyMap())
            .values(values)
            .build();

    Map<String, ObjectStorageRecord> records = new HashMap<>();
    records.put("record1", record);
    ObjectStoragePartition partition = new ObjectStoragePartition(records);

    // Act
    byte[] data = serializer.serialize(partition);
    ObjectStoragePartition result = serializer.deserialize(data);

    // Assert
    assertThat(result.getRecord("record1")).isPresent();
    ObjectStorageRecord resultRecord = result.getRecord("record1").get();
    assertThat(resultRecord.getValues()).containsEntry("col1", null);
    assertThat(resultRecord.getValues()).containsEntry("col2", "non-null");
  }
}
