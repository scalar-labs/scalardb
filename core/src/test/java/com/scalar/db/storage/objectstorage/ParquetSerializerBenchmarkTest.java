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
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

@Tag("benchmark")
public class ParquetSerializerBenchmarkTest {

  private static final int RECORD_COUNT = 500_000;
  private static final int WARMUP_ITERATIONS = 2;
  private static final int BENCHMARK_ITERATIONS = 5;

  private static ObjectStoragePartition partition;
  private static TableMetadata metadata;
  private static long estimatedRecordSize;

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
                    "col_blob",
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
                    "before_col_blob",
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
    when(metadata.getColumnDataType("col_blob")).thenReturn(DataType.BLOB);
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
    when(metadata.getColumnDataType("before_col_blob")).thenReturn(DataType.BLOB);
    when(metadata.getColumnDataType("before_tx_id")).thenReturn(DataType.TEXT);
    when(metadata.getColumnDataType("before_tx_state")).thenReturn(DataType.INT);
    when(metadata.getColumnDataType("before_tx_version")).thenReturn(DataType.INT);
    when(metadata.getColumnDataType("before_tx_prepared_at")).thenReturn(DataType.BIGINT);
    when(metadata.getColumnDataType("before_tx_committed_at")).thenReturn(DataType.BIGINT);

    Random random = new Random(42);
    Map<String, ObjectStorageRecord> records = new HashMap<>(RECORD_COUNT);
    for (int i = 0; i < RECORD_COUNT; i++) {
      String recordId = "record_" + i;

      Map<String, Object> partitionKey = new HashMap<>(1);
      partitionKey.put("pk", "partition_key_value");

      Map<String, Object> clusteringKey = new HashMap<>(1);
      clusteringKey.put("ck", "clustering_key_" + i);

      Map<String, Object> values = new HashMap<>(22);

      // User columns
      values.put("col_text", "user_text_value_" + i);
      values.put("col_int", i);
      values.put("col_bigint", (long) i * 1000L);
      values.put("col_double", i * 1.23);
      values.put("col_boolean", i % 2 == 0);
      byte[] blobData = new byte[1024]; // 1 KiB
      random.nextBytes(blobData);
      values.put("col_blob", blobData);

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
      byte[] beforeBlobData = new byte[1024]; // 1 KiB
      random.nextBytes(beforeBlobData);
      values.put("before_col_blob", beforeBlobData);

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

    // Print record structure
    ObjectStorageRecord sample = records.values().iterator().next();
    estimatedRecordSize = estimateRecordSize(sample);
    long estimatedPartitionSize = (long) RECORD_COUNT * estimatedRecordSize;
    System.out.printf("Records generated: %,d%n", RECORD_COUNT);
    System.out.printf("Estimated record size: %,d bytes/record%n", estimatedRecordSize);
    System.out.printf(
        "Estimated partition size: %.2f MiB%n", estimatedPartitionSize / (1024.0 * 1024.0));
    System.out.println();
  }

  static Stream<Arguments> compressionProvider() {
    return Stream.of(
        Arguments.of("Parquet", CompressionCodecName.UNCOMPRESSED),
        Arguments.of("Parquet+GZIP", CompressionCodecName.GZIP),
        Arguments.of("Parquet+SNAPPY", CompressionCodecName.SNAPPY),
        Arguments.of("Parquet+ZSTD", CompressionCodecName.ZSTD));
  }

  static Stream<Arguments> allCompressionProvider() {
    Stream<Arguments> all = compressionProvider();
    String targets = System.getProperty("benchmark.targets");
    if (targets == null || targets.isEmpty()) {
      return all;
    }
    Set<String> targetSet =
        Arrays.stream(targets.split(","))
            .map(String::trim)
            .map(s -> s.toUpperCase(Locale.ROOT))
            .collect(Collectors.toSet());
    return all.filter(
        args -> {
          String name = ((String) args.get()[0]).toUpperCase(Locale.ROOT);
          return targetSet.contains(name);
        });
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("allCompressionProvider")
  void benchmark(String name, CompressionCodecName codec) {
    // Warmup with production data
    System.gc();
    warmup(codec);

    long[] serializeTimes = new long[BENCHMARK_ITERATIONS];
    long[] deserializeTimes = new long[BENCHMARK_ITERATIONS];
    long[] serializeMemories = new long[BENCHMARK_ITERATIONS];
    long[] deserializeMemories = new long[BENCHMARK_ITERATIONS];
    long serializedSize = 0;

    for (int iter = 0; iter < BENCHMARK_ITERATIONS; iter++) {
      // Measure serialize memory
      System.gc();
      long serializeBaseline = usedMemory();

      long serializeStart = System.nanoTime();
      byte[] serialized = ParquetSerializer.serialize(partition, metadata, codec);
      long serializeEnd = System.nanoTime();

      long serializePeak = usedMemory();
      serializeTimes[iter] = (serializeEnd - serializeStart) / 1_000_000;
      serializedSize = serialized.length;
      serializeMemories[iter] = serializePeak - serializeBaseline;

      // GC between phases to avoid serialize garbage inflating deserialize measurement
      System.gc();

      // Measure deserialize memory
      long deserializeBaseline = usedMemory();

      long deserializeStart = System.nanoTime();
      ObjectStoragePartition deserialized = ParquetSerializer.deserialize(serialized, metadata);
      long deserializeEnd = System.nanoTime();

      deserializeMemories[iter] = usedMemory() - deserializeBaseline;
      deserializeTimes[iter] = (deserializeEnd - deserializeStart) / 1_000_000;

      // Release for GC
      serialized = null;

      // Correctness check
      assertThat(deserialized.getRecords()).hasSize(RECORD_COUNT);
      deserialized = null;
    }

    // Estimate in-memory size for compression ratio
    long estimatedInMemoryBytes = (long) RECORD_COUNT * estimatedRecordSize;
    double compressionRatio = (double) estimatedInMemoryBytes / serializedSize;

    // Print results
    System.out.printf("--- Benchmark Result: %s ---%n", name);
    System.out.printf("  Iterations: %d (warmup: %d)%n", BENCHMARK_ITERATIONS, WARMUP_ITERATIONS);
    for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
      long roundTrip = serializeTimes[i] + deserializeTimes[i];
      System.out.printf(
          "  [%d] Serialize: %,7d ms  Deserialize: %,7d ms  Round-trip: %,7d ms%n",
          i + 1, serializeTimes[i], deserializeTimes[i], roundTrip);
    }
    long avgSerialize = Arrays.stream(serializeTimes).sum() / BENCHMARK_ITERATIONS;
    long avgDeserialize = Arrays.stream(deserializeTimes).sum() / BENCHMARK_ITERATIONS;
    long avgRoundTrip = avgSerialize + avgDeserialize;
    double avgSerMem = Arrays.stream(serializeMemories).average().orElse(0);
    double avgDesMem = Arrays.stream(deserializeMemories).average().orElse(0);
    System.out.printf("  [Avg] Serialize time:     %,d ms%n", avgSerialize);
    System.out.printf("  [Avg] Deserialize time:   %,d ms%n", avgDeserialize);
    System.out.printf("  [Avg] Total round-trip:   %,d ms%n", avgRoundTrip);
    System.out.printf(
        Locale.US,
        "  Serialized size:    %.2f MiB (%,d bytes)%n",
        serializedSize / (1024.0 * 1024.0),
        serializedSize);
    System.out.printf(Locale.US, "  Compression ratio:  %.2fx%n", compressionRatio);
    System.out.printf(
        Locale.US, "  [Avg] Serialize memory:   %.2f MiB%n", avgSerMem / (1024.0 * 1024.0));
    System.out.printf(
        Locale.US, "  [Avg] Deserialize memory: %.2f MiB%n", avgDesMem / (1024.0 * 1024.0));
    System.out.println();
  }

  private static long usedMemory() {
    Runtime rt = Runtime.getRuntime();
    return rt.totalMemory() - rt.freeMemory();
  }

  private static long estimateRecordSize(ObjectStorageRecord record) {
    long size = 0;
    // id
    size += record.getId().length();
    // partitionKey, clusteringKey, values
    size += estimateMapSize(record.getPartitionKey());
    size += estimateMapSize(record.getClusteringKey());
    size += estimateMapSize(record.getValues());
    return size;
  }

  private static long estimateMapSize(Map<String, Object> map) {
    long size = 0;
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      size += entry.getKey().length();
      Object v = entry.getValue();
      if (v instanceof byte[]) {
        size += ((byte[]) v).length;
      } else if (v instanceof String) {
        size += ((String) v).length();
      } else if (v instanceof Long) {
        size += 8;
      } else if (v instanceof Double) {
        size += 8;
      } else if (v instanceof Integer) {
        size += 4;
      } else if (v instanceof Boolean) {
        size += 1;
      }
    }
    return size;
  }

  private void warmup(CompressionCodecName codec) {
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      byte[] data = ParquetSerializer.serialize(partition, metadata, codec);
      ParquetSerializer.deserialize(data, metadata);
    }
  }
}
