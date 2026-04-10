package com.scalar.db.storage.objectstorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@Tag("benchmark")
public class ParquetSerializerBenchmarkTest {

  private static final int RECORD_COUNT = 100_000;

  private static ObjectStoragePartition partition;
  private static TableMetadata metadata;

  @BeforeAll
  static void setUp() {
    System.out.println("=== Benchmark Setup ===");
    System.out.println("Generating " + RECORD_COUNT + " records...");

    metadata = Mockito.mock(TableMetadata.class);
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
                    "col_text",
                    "col_int",
                    "col_bigint",
                    "col_double",
                    "col_boolean",
                    "tx_id",
                    "tx_state",
                    "tx_version",
                    "tx_prepared_at",
                    "tx_committed_at",
                    "before_col_text",
                    "before_col_int",
                    "before_col_bigint",
                    "before_col_double",
                    "before_col_boolean",
                    "before_tx_id",
                    "before_tx_state",
                    "before_tx_version",
                    "before_tx_prepared_at",
                    "before_tx_committed_at")));
    when(metadata.getColumnDataType("pk")).thenReturn(DataType.TEXT);
    when(metadata.getColumnDataType("ck")).thenReturn(DataType.TEXT);
    when(metadata.getColumnDataType("col_text")).thenReturn(DataType.TEXT);
    when(metadata.getColumnDataType("col_int")).thenReturn(DataType.INT);
    when(metadata.getColumnDataType("col_bigint")).thenReturn(DataType.BIGINT);
    when(metadata.getColumnDataType("col_double")).thenReturn(DataType.DOUBLE);
    when(metadata.getColumnDataType("col_boolean")).thenReturn(DataType.BOOLEAN);
    when(metadata.getColumnDataType("tx_id")).thenReturn(DataType.TEXT);
    when(metadata.getColumnDataType("tx_state")).thenReturn(DataType.INT);
    when(metadata.getColumnDataType("tx_version")).thenReturn(DataType.INT);
    when(metadata.getColumnDataType("tx_prepared_at")).thenReturn(DataType.BIGINT);
    when(metadata.getColumnDataType("tx_committed_at")).thenReturn(DataType.BIGINT);
    when(metadata.getColumnDataType("before_col_text")).thenReturn(DataType.TEXT);
    when(metadata.getColumnDataType("before_col_int")).thenReturn(DataType.INT);
    when(metadata.getColumnDataType("before_col_bigint")).thenReturn(DataType.BIGINT);
    when(metadata.getColumnDataType("before_col_double")).thenReturn(DataType.DOUBLE);
    when(metadata.getColumnDataType("before_col_boolean")).thenReturn(DataType.BOOLEAN);
    when(metadata.getColumnDataType("before_tx_id")).thenReturn(DataType.TEXT);
    when(metadata.getColumnDataType("before_tx_state")).thenReturn(DataType.INT);
    when(metadata.getColumnDataType("before_tx_version")).thenReturn(DataType.INT);
    when(metadata.getColumnDataType("before_tx_prepared_at")).thenReturn(DataType.BIGINT);
    when(metadata.getColumnDataType("before_tx_committed_at")).thenReturn(DataType.BIGINT);

    Map<String, ObjectStorageRecord> records = new HashMap<>(RECORD_COUNT);
    for (int i = 0; i < RECORD_COUNT; i++) {
      String recordId = "record_" + i;

      Map<String, Object> partitionKey = new HashMap<>(1);
      partitionKey.put("pk", "partition_key_value");

      Map<String, Object> clusteringKey = new HashMap<>(1);
      clusteringKey.put("ck", "clustering_key_" + i);

      Map<String, Object> values = new HashMap<>(20);

      // User columns
      values.put("col_text", "user_text_value_" + i);
      values.put("col_int", i);
      values.put("col_bigint", (long) i * 1000L);
      values.put("col_double", i * 1.23);
      values.put("col_boolean", i % 2 == 0);

      // Transaction metadata columns
      String txId = UUID.randomUUID().toString();
      values.put("tx_id", txId);
      values.put("tx_state", 1);
      values.put("tx_version", 1);
      values.put("tx_prepared_at", System.currentTimeMillis());
      values.put("tx_committed_at", System.currentTimeMillis());

      // Before image columns (user columns)
      values.put("before_col_text", "before_text_value_" + i);
      values.put("before_col_int", i - 1);
      values.put("before_col_bigint", (long) (i - 1) * 1000L);
      values.put("before_col_double", (i - 1) * 1.23);
      values.put("before_col_boolean", (i - 1) % 2 == 0);

      // Before image columns (transaction metadata)
      values.put("before_tx_id", UUID.randomUUID().toString());
      values.put("before_tx_state", 1);
      values.put("before_tx_version", 1);
      values.put("before_tx_prepared_at", System.currentTimeMillis());
      values.put("before_tx_committed_at", System.currentTimeMillis());

      ObjectStorageRecord record =
          ObjectStorageRecord.newBuilder()
              .id(recordId)
              .partitionKey(partitionKey)
              .clusteringKey(clusteringKey)
              .values(values)
              .build();
      records.put(recordId, record);
    }
    partition = new ObjectStoragePartition(records);

    long estimatedBytes = (long) RECORD_COUNT * 1024L;
    System.out.printf("Records generated: %,d%n", RECORD_COUNT);
    System.out.printf(
        "Estimated in-memory size: ~%.2f GiB%n", estimatedBytes / (1024.0 * 1024.0 * 1024.0));
    System.out.println();
  }

  @Test
  void benchmark() {
    // Warmup
    System.gc();
    warmup();

    // Measure serialize memory
    System.gc();
    long serializeBaseline = usedMemory();

    long serializeStart = System.nanoTime();
    byte[] serialized = ParquetSerializer.serialize(partition, metadata);
    long serializeEnd = System.nanoTime();

    long serializePeak = usedMemory();
    long serializeMs = (serializeEnd - serializeStart) / 1_000_000;
    long serializedSize = serialized.length;
    long serializeMemory = serializePeak - serializeBaseline;

    System.gc();

    // Measure deserialize memory
    long deserializeBaseline = usedMemory();

    long deserializeStart = System.nanoTime();
    ObjectStoragePartition deserialized = ParquetSerializer.deserialize(serialized, metadata);
    long deserializeEnd = System.nanoTime();

    long deserializeMemory = usedMemory() - deserializeBaseline;
    long deserializeMs = (deserializeEnd - deserializeStart) / 1_000_000;
    long totalMs = serializeMs + deserializeMs;

    serialized = null;

    // Correctness check
    assertThat(deserialized.getRecords()).hasSize(RECORD_COUNT);

    deserialized = null;

    long estimatedInMemoryBytes = (long) RECORD_COUNT * 1024L;
    double compressionRatio = (double) estimatedInMemoryBytes / serializedSize;

    // Print results
    System.out.printf("--- Benchmark Result: Parquet+Snappy (row-per-record) ---%n");
    System.out.printf("  Serialize time:     %,d ms%n", serializeMs);
    System.out.printf("  Deserialize time:   %,d ms%n", deserializeMs);
    System.out.printf("  Total round-trip:   %,d ms%n", totalMs);
    System.out.printf(
        Locale.US,
        "  Serialized size:    %.2f MiB (%,d bytes)%n",
        serializedSize / (1024.0 * 1024.0),
        serializedSize);
    System.out.printf(Locale.US, "  Compression ratio:  %.2fx%n", compressionRatio);
    System.out.printf(
        Locale.US, "  Serialize memory:   %.2f MiB%n", serializeMemory / (1024.0 * 1024.0));
    System.out.printf(
        Locale.US, "  Deserialize memory: %.2f MiB%n", deserializeMemory / (1024.0 * 1024.0));
    System.out.println();
  }

  private static long usedMemory() {
    Runtime rt = Runtime.getRuntime();
    return rt.totalMemory() - rt.freeMemory();
  }

  private void warmup() {
    Map<String, ObjectStorageRecord> smallRecords = new HashMap<>(1);
    ObjectStorageRecord record =
        ObjectStorageRecord.newBuilder()
            .id("warmup")
            .partitionKey(Collections.singletonMap("pk", (Object) "warmup"))
            .clusteringKey(Collections.singletonMap("ck", (Object) "warmup"))
            .values(Collections.singletonMap("col_text", (Object) "warmup"))
            .build();
    smallRecords.put("warmup", record);
    ObjectStoragePartition smallPartition = new ObjectStoragePartition(smallRecords);

    byte[] data = ParquetSerializer.serialize(smallPartition, metadata);
    ParquetSerializer.deserialize(data, metadata);
  }
}
