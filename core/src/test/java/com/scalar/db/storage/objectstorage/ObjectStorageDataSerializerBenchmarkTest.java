package com.scalar.db.storage.objectstorage;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Tag("benchmark")
public class ObjectStorageDataSerializerBenchmarkTest {

  private static final int RECORD_COUNT = 500_000;

  private static ObjectStoragePartition partition;
  private static ObjectStorageTableMetadata osMetadata;
  private static long estimatedRecordSize;

  @BeforeAll
  static void setUp() {
    System.out.println("=== Benchmark Setup ===");
    System.out.println("Generating " + RECORD_COUNT + " records...");

    // Build table metadata with all column types used in the benchmark
    Map<String, String> columns = getStringStringMap();
    osMetadata =
        ObjectStorageTableMetadata.newBuilder()
            .partitionKeyNames(new LinkedHashSet<>(Collections.singletonList("pk")))
            .clusteringKeyNames(new LinkedHashSet<>(Collections.singletonList("ck")))
            .clusteringOrders(Collections.singletonMap("ck", "ASC"))
            .secondaryIndexNames(Collections.emptySet())
            .columns(columns)
            .build();

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
      values.put("tx_state", 1); // COMMITTED
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

  private static @NonNull Map<String, String> getStringStringMap() {
    Map<String, String> columns = new LinkedHashMap<>();
    columns.put("pk", "text");
    columns.put("ck", "text");
    columns.put("col_text", "text");
    columns.put("col_int", "int");
    columns.put("col_bigint", "bigint");
    columns.put("col_double", "double");
    columns.put("col_boolean", "boolean");
    columns.put("col_blob", "blob");
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
    columns.put("before_col_blob", "blob");
    columns.put("before_tx_id", "text");
    columns.put("before_tx_state", "int");
    columns.put("before_tx_version", "int");
    columns.put("before_tx_prepared_at", "bigint");
    columns.put("before_tx_committed_at", "bigint");
    return columns;
  }

  static Stream<Arguments> serializerProvider() {
    return Stream.of(
        Arguments.of(
            "CBOR",
            ObjectStorageDataSerializerFactory.create(
                ObjectStorageFormat.CBOR, ObjectStorageCompression.NONE)),
        Arguments.of(
            "Fory",
            ObjectStorageDataSerializerFactory.create(
                ObjectStorageFormat.FORY, ObjectStorageCompression.NONE, osMetadata)),
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
                ObjectStorageFormat.FORY, ObjectStorageCompression.GZIP, osMetadata)),
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
                ObjectStorageFormat.FORY, ObjectStorageCompression.ZSTD, osMetadata)),
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
    Stream<Arguments> all =
        Stream.of(serializerProvider(), serializerWithGzipProvider(), serializerWithZstdProvider())
            .flatMap(s -> s);
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

  @ParameterizedTest(name = "{0}")
  @MethodSource("allSerializerProvider")
  void benchmark(String name, ObjectStorageDataSerializer serializer) {
    // Warmup with a small partition to trigger class loading and JIT
    System.gc();
    warmup(serializer);

    // Measure serialize memory
    System.gc();
    long serializeBaseline = usedMemory();

    long serializeStart = System.nanoTime();
    byte[] serialized = serializer.serialize(partition);
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
    ObjectStoragePartition deserialized = serializer.deserialize(serialized);
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
    long estimatedInMemoryBytes = (long) RECORD_COUNT * estimatedRecordSize;
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

  private void warmup(ObjectStorageDataSerializer serializer) {
    Map<String, ObjectStorageRecord> smallRecords = new HashMap<>(1);
    ObjectStorageRecord record =
        ObjectStorageRecord.newBuilder()
            .id("warmup")
            .partitionKey(Collections.singletonMap("pk", (Object) "warmup"))
            .clusteringKey(Collections.singletonMap("ck", (Object) "warmup"))
            .values(Collections.singletonMap("col", (Object) "warmup"))
            .build();
    smallRecords.put("warmup", record);
    ObjectStoragePartition smallPartition = new ObjectStoragePartition(smallRecords);

    byte[] data = serializer.serialize(smallPartition);
    serializer.deserialize(data);
  }
}
