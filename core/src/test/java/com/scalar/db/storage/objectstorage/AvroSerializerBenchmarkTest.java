package com.scalar.db.storage.objectstorage;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.avro.file.CodecFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Tag("benchmark")
public class AvroSerializerBenchmarkTest {

  private static final int RECORD_COUNT = 500_000;

  private static ObjectStoragePartition partition;
  private static ObjectStorageTableMetadata metadata;

  @BeforeAll
  static void setUp() {
    System.out.println("=== Benchmark Setup ===");
    System.out.println("Generating " + RECORD_COUNT + " records...");

    LinkedHashSet<String> partitionKeyNames = new LinkedHashSet<>(Collections.singletonList("pk"));
    LinkedHashSet<String> clusteringKeyNames = new LinkedHashSet<>(Collections.singletonList("ck"));

    Map<String, String> columns = new LinkedHashMap<>();
    columns.put("pk", "text");
    columns.put("ck", "text");
    columns.put("col_text", "text");
    columns.put("col_int", "int");
    columns.put("col_bigint", "bigint");
    columns.put("col_double", "double");
    columns.put("col_boolean", "boolean");
    columns.put("tx_id", "text");
    columns.put("tx_state", "int");
    columns.put("tx_version", "int");
    columns.put("tx_prepared_at", "bigint");
    columns.put("tx_committed_at", "bigint");
    columns.put("before_col_text", "text");
    columns.put("before_col_int", "int");
    columns.put("before_col_bigint", "bigint");
    columns.put("before_col_double", "double");
    columns.put("before_col_boolean", "boolean");
    columns.put("before_tx_id", "text");
    columns.put("before_tx_state", "int");
    columns.put("before_tx_version", "int");
    columns.put("before_tx_prepared_at", "bigint");
    columns.put("before_tx_committed_at", "bigint");

    metadata =
        ObjectStorageTableMetadata.newBuilder()
            .partitionKeyNames(partitionKeyNames)
            .clusteringKeyNames(clusteringKeyNames)
            .columns(columns)
            .build();

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

  static Stream<Arguments> codecProvider() {
    return Stream.of(
        Arguments.of("Avro", CodecFactory.nullCodec()),
        Arguments.of("Avro+GZIP", CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL)),
        Arguments.of(
            "Avro+ZSTD", CodecFactory.zstandardCodec(CodecFactory.DEFAULT_ZSTANDARD_LEVEL)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("codecProvider")
  void benchmark(String name, CodecFactory codecFactory) {
    // Warmup with a small partition to trigger class loading and JIT
    System.gc();
    warmup(codecFactory);

    // Measure serialize memory
    System.gc();
    long serializeBaseline = usedMemory();

    long serializeStart = System.nanoTime();
    byte[] serialized = AvroSerializer.serialize(partition, metadata, codecFactory);
    long serializeEnd = System.nanoTime();

    long serializePeak = usedMemory();
    long serializeMs = (serializeEnd - serializeStart) / 1_000_000;
    long serializedSize = serialized.length;
    long serializeMemory = serializePeak - serializeBaseline;

    // GC between phases to avoid serialize garbage inflating deserialize measurement
    System.gc();

    // Measure deserialize memory
    long deserializeBaseline = usedMemory();

    long deserializeStart = System.nanoTime();
    ObjectStoragePartition deserialized = AvroSerializer.deserialize(serialized, metadata);
    long deserializeEnd = System.nanoTime();

    long deserializeMemory = usedMemory() - deserializeBaseline;
    long deserializeMs = (deserializeEnd - deserializeStart) / 1_000_000;
    long totalMs = serializeMs + deserializeMs;

    // Release serialized bytes for GC
    serialized = null;

    // Correctness check
    assertThat(deserialized.getRecords()).hasSize(RECORD_COUNT);

    // Release deserialized partition for GC
    deserialized = null;

    // Estimate in-memory size for compression ratio
    long estimatedInMemoryBytes = (long) RECORD_COUNT * 1024L;
    double compressionRatio = (double) estimatedInMemoryBytes / serializedSize;

    // Print results
    System.out.printf("--- Benchmark Result: %s ---%n", name);
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

  private void warmup(CodecFactory codecFactory) {
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

    byte[] data = AvroSerializer.serialize(smallPartition, metadata, codecFactory);
    AvroSerializer.deserialize(data, metadata);
  }
}
