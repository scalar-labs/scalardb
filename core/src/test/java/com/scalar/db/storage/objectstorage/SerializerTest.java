package com.scalar.db.storage.objectstorage;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import org.junit.jupiter.api.Test;

public class SerializerTest {

  private static ObjectStorageRecord createRecord(Map<String, Object> values) {
    return new ObjectStorageRecord("id1", null, null, values);
  }

  @Test
  public void serialize_andDeserializeBytes_ShouldRoundTrip() {
    // Arrange
    Map<String, Object> values = new HashMap<>();
    values.put("col1", "value1");
    values.put("col2", 42);
    ObjectStoragePartition partition = new ObjectStoragePartition(null);
    partition.putRecord("record1", createRecord(values));

    // Act
    byte[] serialized = Serializer.serialize(partition);
    ObjectStoragePartition deserialized =
        Serializer.deserialize(serialized, new TypeReference<ObjectStoragePartition>() {});

    // Assert
    assertThat(deserialized.getRecords()).containsKey("record1");
    assertThat(deserialized.getRecord("record1").get().getValues().get("col1")).isEqualTo("value1");
    assertThat(deserialized.getRecord("record1").get().getValues().get("col2")).isEqualTo(42);
  }

  @Test
  public void serialize_ShouldProduceValidBsonStructure() throws Exception {
    // Arrange
    ObjectStoragePartition partition = new ObjectStoragePartition(null);

    // Act
    byte[] serialized = Serializer.serialize(partition);

    // Assert - serialized data is GZIP-compressed, so decompress first
    assertThat(serialized.length).isGreaterThanOrEqualTo(2);
    assertThat(serialized[0]).isEqualTo((byte) 0x1f); // GZIP magic number
    assertThat(serialized[1]).isEqualTo((byte) 0x8b);

    // Decompress to get the raw BSON
    byte[] bson = decompress(serialized);

    // BSON starts with 4-byte LE size matching data.length and ends with 0x00
    assertThat(bson.length).isGreaterThanOrEqualTo(5);
    int size =
        (bson[0] & 0xFF)
            | ((bson[1] & 0xFF) << 8)
            | ((bson[2] & 0xFF) << 16)
            | ((bson[3] & 0xFF) << 24);
    assertThat(size).isEqualTo(bson.length);
    assertThat(bson[bson.length - 1]).isEqualTo((byte) 0x00);
  }

  @Test
  public void serialize_andDeserializeBytes_ObjectStoragePartition_ShouldRoundTrip() {
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
  public void serialize_andDeserializeBytes_MetadataTable_ShouldRoundTrip() {
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
    byte[] serialized = Serializer.serialize(metadataTable);
    Map<String, ObjectStorageTableMetadata> deserialized =
        Serializer.deserialize(
            serialized, new TypeReference<Map<String, ObjectStorageTableMetadata>>() {});

    // Assert
    assertThat(deserialized).containsKey("ns.table1");
    assertThat(deserialized.get("ns.table1").getPartitionKeyNames()).containsExactly("pk");
    assertThat(deserialized.get("ns.table1").getClusteringKeyNames()).containsExactly("ck");
    assertThat(deserialized.get("ns.table1").getColumns()).containsEntry("value", "BOOLEAN");
  }

  private static byte[] decompress(byte[] data) throws Exception {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
        GZIPInputStream gzis = new GZIPInputStream(bais);
        ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      byte[] buf = new byte[4096];
      int n;
      while ((n = gzis.read(buf)) != -1) {
        out.write(buf, 0, n);
      }
      return out.toByteArray();
    }
  }
}
