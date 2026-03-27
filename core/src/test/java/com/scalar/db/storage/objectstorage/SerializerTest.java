package com.scalar.db.storage.objectstorage;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.type.TypeReference;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class SerializerTest {

  private static ObjectStorageRecord createRecord(Map<String, Object> values) {
    return new ObjectStorageRecord("id1", null, null, values);
  }

  @Test
  public void serializeAsBytes_andDeserializeBytes_ShouldRoundTrip() {
    // Arrange
    Map<String, Object> values = new HashMap<>();
    values.put("col1", "value1");
    values.put("col2", 42);
    ObjectStoragePartition partition = new ObjectStoragePartition(null);
    partition.putRecord("record1", createRecord(values));

    // Act
    byte[] serialized = Serializer.serializeAsBytes(partition);
    ObjectStoragePartition deserialized =
        Serializer.deserialize(serialized, new TypeReference<ObjectStoragePartition>() {});

    // Assert
    assertThat(deserialized.getRecords()).containsKey("record1");
    assertThat(deserialized.getRecord("record1").get().getValues().get("col1")).isEqualTo("value1");
    assertThat(deserialized.getRecord("record1").get().getValues().get("col2")).isEqualTo(42);
  }

  @Test
  public void deserializeBytes_JsonFormatGiven_ShouldDeserializeCorrectly() {
    // Arrange - serialize with legacy JSON method, then convert to bytes
    Map<String, Object> values = new HashMap<>();
    values.put("col1", "value1");
    values.put("col2", 42);
    ObjectStoragePartition partition = new ObjectStoragePartition(null);
    partition.putRecord("record1", createRecord(values));

    String jsonString = Serializer.serialize(partition);
    byte[] jsonBytes = jsonString.getBytes(StandardCharsets.UTF_8);

    // Act - deserialize using byte[] overload (should auto-detect JSON)
    ObjectStoragePartition deserialized =
        Serializer.deserialize(jsonBytes, new TypeReference<ObjectStoragePartition>() {});

    // Assert
    assertThat(deserialized.getRecords()).containsKey("record1");
    assertThat(deserialized.getRecord("record1").get().getValues().get("col1")).isEqualTo("value1");
    assertThat(deserialized.getRecord("record1").get().getValues().get("col2")).isEqualTo(42);
  }

  @Test
  public void serializeAsBytes_ShouldNotStartWithOpenBrace() {
    // Arrange
    ObjectStoragePartition partition = new ObjectStoragePartition(null);

    // Act
    byte[] serialized = Serializer.serializeAsBytes(partition);

    // Assert - CBOR should not start with '{' (0x7B), which is used for JSON detection
    assertThat(serialized.length).isGreaterThan(0);
    assertThat(serialized[0]).isNotEqualTo((byte) '{');
  }

  @Test
  public void serializeAsBytes_andDeserializeBytes_ObjectStoragePartition_ShouldRoundTrip() {
    // Arrange
    Map<String, Object> values1 = new HashMap<>();
    values1.put("pk", "pk_value");
    values1.put("ck", "ck_value");
    values1.put("data", "some_data");
    Map<String, Object> values2 = new HashMap<>();
    values2.put("pk", "pk_value");
    values2.put("ck", "ck_value2");
    values2.put("data", null);
    ObjectStoragePartition partition = new ObjectStoragePartition(null);
    partition.putRecord("record1", createRecord(values1));
    partition.putRecord("record2", createRecord(values2));

    // Act - use ObjectStoragePartition's own serialize/deserialize
    byte[] serialized = partition.serialize();
    ObjectStoragePartition deserialized = ObjectStoragePartition.deserialize(serialized);

    // Assert
    assertThat(deserialized.getRecords()).hasSize(2);
    assertThat(deserialized.getRecord("record1").get().getValues().get("pk")).isEqualTo("pk_value");
    assertThat(deserialized.getRecord("record2").get().getValues().get("data")).isNull();
  }

  @Test
  public void serializeAsBytes_andDeserializeBytes_MetadataTable_ShouldRoundTrip() {
    // Arrange
    Map<String, ObjectStorageTableMetadata> metadataTable = new HashMap<>();
    Map<String, String> columns = new HashMap<>();
    columns.put("pk", "TEXT");
    columns.put("ck", "INT");
    columns.put("value", "BOOLEAN");
    metadataTable.put(
        "ns.table1",
        ObjectStorageTableMetadata.newBuilder()
            .partitionKeyNames(new LinkedHashSet<>(Collections.singletonList("pk")))
            .clusteringKeyNames(new LinkedHashSet<>(Collections.singletonList("ck")))
            .columns(columns)
            .build());

    // Act
    byte[] serialized = Serializer.serializeAsBytes(metadataTable);
    Map<String, ObjectStorageTableMetadata> deserialized =
        Serializer.deserialize(
            serialized, new TypeReference<Map<String, ObjectStorageTableMetadata>>() {});

    // Assert
    assertThat(deserialized).containsKey("ns.table1");
    assertThat(deserialized.get("ns.table1").getPartitionKeyNames()).containsExactly("pk");
    assertThat(deserialized.get("ns.table1").getClusteringKeyNames()).containsExactly("ck");
    assertThat(deserialized.get("ns.table1").getColumns()).containsEntry("value", "BOOLEAN");
  }

  @Test
  public void deserializeBytes_JsonMetadataTable_ShouldDeserializeCorrectly() {
    // Arrange - simulate legacy JSON-encoded metadata
    Map<String, ObjectStorageTableMetadata> metadataTable = new HashMap<>();
    metadataTable.put(
        "ns.table1",
        ObjectStorageTableMetadata.newBuilder()
            .partitionKeyNames(new LinkedHashSet<>(Collections.singletonList("pk")))
            .build());

    String jsonString = Serializer.serialize(metadataTable);
    byte[] jsonBytes = jsonString.getBytes(StandardCharsets.UTF_8);

    // Act - deserialize using byte[] overload
    Map<String, ObjectStorageTableMetadata> deserialized =
        Serializer.deserialize(
            jsonBytes, new TypeReference<Map<String, ObjectStorageTableMetadata>>() {});

    // Assert
    assertThat(deserialized).containsKey("ns.table1");
    assertThat(deserialized.get("ns.table1").getPartitionKeyNames()).containsExactly("pk");
  }
}
