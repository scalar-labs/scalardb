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
import org.apache.orc.CompressionKind;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Tag("benchmark")
public class OrcSerializerBenchmarkTest {

  private static final int RECORD_COUNT = 500_000;
  private static final int WARMUP_ITERATIONS = 2;
  private static final int BENCHMARK_ITERATIONS = 5;
  private static final int SMALL_RECORD_COUNT = 100;

  private static ObjectStoragePartition partition;
  private static ObjectStoragePartition smallPartition;
  private static ObjectStorageTableMetadata osMetadata;
  private static long estimatedRecordSize;

  @BeforeAll
  static void setUp() {
    System.out.println("=== Benchmark Setup ===");
    System.out.println("Generating " + RECORD_COUNT + " records...");

    // Build table metadata with all column types used in the benchmark
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

    // Build small partition from first SMALL_RECORD_COUNT records
    Map<String, ObjectStorageRecord> smallRecords = new HashMap<>(SMALL_RECORD_COUNT);
    int count = 0;
    for (Map.Entry<String, ObjectStorageRecord> entry : records.entrySet()) {
      if (count >= SMALL_RECORD_COUNT) break;
      smallRecords.put(entry.getKey(), entry.getValue());
      count++;
    }
    smallPartition = new ObjectStoragePartition(smallRecords);

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

  static Stream<Arguments> allSerializerProvider() {
    Stream<Arguments> all =
        Stream.of(
            Arguments.of("ORC", CompressionKind.NONE),
            Arguments.of("ORC+ZLIB", CompressionKind.ZLIB),
            Arguments.of("ORC+ZSTD", CompressionKind.ZSTD),
            Arguments.of("ORC+SNAPPY", CompressionKind.SNAPPY));
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
  @MethodSource("allSerializerProvider")
  void benchmark(String name, CompressionKind compressionKind) {
    runBenchmark(name, partition, RECORD_COUNT, compressionKind);
  }

  @ParameterizedTest(name = "{0} [small]")
  @MethodSource("allSerializerProvider")
  void benchmarkSmall(String name, CompressionKind compressionKind) {
    runBenchmark(name + " [small]", smallPartition, SMALL_RECORD_COUNT, compressionKind);
  }

  private void runBenchmark(
      String name,
      ObjectStoragePartition targetPartition,
      int recordCount,
      CompressionKind compressionKind) {
    System.gc();
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      byte[] data = OrcSerializer.serialize(targetPartition, osMetadata, compressionKind);
      OrcSerializer.deserialize(data, osMetadata);
    }

    long[] serializeNanos = new long[BENCHMARK_ITERATIONS];
    long[] deserializeNanos = new long[BENCHMARK_ITERATIONS];
    long[] serializeMemories = new long[BENCHMARK_ITERATIONS];
    long[] deserializeMemories = new long[BENCHMARK_ITERATIONS];
    long serializedSize = 0;

    for (int iter = 0; iter < BENCHMARK_ITERATIONS; iter++) {
      System.gc();
      long serializeBaseline = usedMemory();

      long serializeStart = System.nanoTime();
      byte[] serialized = OrcSerializer.serialize(targetPartition, osMetadata, compressionKind);
      long serializeEnd = System.nanoTime();

      long serializePeak = usedMemory();
      serializeNanos[iter] = serializeEnd - serializeStart;
      serializedSize = serialized.length;
      serializeMemories[iter] = serializePeak - serializeBaseline;

      System.gc();
      long deserializeBaseline = usedMemory();

      long deserializeStart = System.nanoTime();
      ObjectStoragePartition deserialized = OrcSerializer.deserialize(serialized, osMetadata);
      long deserializeEnd = System.nanoTime();

      deserializeMemories[iter] = usedMemory() - deserializeBaseline;
      deserializeNanos[iter] = deserializeEnd - deserializeStart;

      serialized = null;
      assertThat(deserialized.getRecords()).hasSize(recordCount);
      deserialized = null;
    }

    printResults(
        name,
        recordCount,
        serializedSize,
        serializeNanos,
        deserializeNanos,
        serializeMemories,
        deserializeMemories);
  }

  private static long usedMemory() {
    Runtime rt = Runtime.getRuntime();
    return rt.totalMemory() - rt.freeMemory();
  }

  private static long estimateRecordSize(ObjectStorageRecord record) {
    long size = 0;
    size += record.getId().length();
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

  private void printResults(
      String name,
      int recordCount,
      long serializedSize,
      long[] serializeNanos,
      long[] deserializeNanos,
      long[] serializeMemories,
      long[] deserializeMemories) {
    long estimatedInMemoryBytes = (long) recordCount * estimatedRecordSize;
    double compressionRatio = (double) estimatedInMemoryBytes / serializedSize;

    long avgRoundTripNs =
        (Arrays.stream(serializeNanos).sum() + Arrays.stream(deserializeNanos).sum())
            / BENCHMARK_ITERATIONS;
    boolean useMicros = avgRoundTripNs < 10_000_000L;
    String timeUnit = useMicros ? "\u00b5s" : "ms";
    long timeDivisor = useMicros ? 1_000L : 1_000_000L;

    boolean useKiB = serializedSize < 1024L * 1024L;
    String sizeUnit = useKiB ? "KiB" : "MiB";
    double sizeDivisor = useKiB ? 1024.0 : 1024.0 * 1024.0;

    System.out.printf("--- Benchmark Result: %s ---%n", name);
    System.out.printf(
        "  Records: %,d  Iterations: %d (warmup: %d)%n",
        recordCount, BENCHMARK_ITERATIONS, WARMUP_ITERATIONS);
    for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
      long ser = serializeNanos[i] / timeDivisor;
      long des = deserializeNanos[i] / timeDivisor;
      long rt = ser + des;
      System.out.printf(
          "  [%d] Serialize: %,7d %s  Deserialize: %,7d %s  Round-trip: %,7d %s%n",
          i + 1, ser, timeUnit, des, timeUnit, rt, timeUnit);
    }
    long avgSer = Arrays.stream(serializeNanos).sum() / BENCHMARK_ITERATIONS / timeDivisor;
    long avgDes = Arrays.stream(deserializeNanos).sum() / BENCHMARK_ITERATIONS / timeDivisor;
    long avgRt = avgSer + avgDes;
    double avgSerMem = Arrays.stream(serializeMemories).average().orElse(0);
    double avgDesMem = Arrays.stream(deserializeMemories).average().orElse(0);
    System.out.printf("  [Avg] Serialize time:     %,d %s%n", avgSer, timeUnit);
    System.out.printf("  [Avg] Deserialize time:   %,d %s%n", avgDes, timeUnit);
    System.out.printf("  [Avg] Total round-trip:   %,d %s%n", avgRt, timeUnit);
    System.out.printf(
        Locale.US,
        "  Serialized size:    %.2f %s (%,d bytes)%n",
        serializedSize / sizeDivisor,
        sizeUnit,
        serializedSize);
    System.out.printf(Locale.US, "  Compression ratio:  %.2fx%n", compressionRatio);
    System.out.printf(
        Locale.US, "  [Avg] Serialize memory:   %.2f MiB%n", avgSerMem / (1024.0 * 1024.0));
    System.out.printf(
        Locale.US, "  [Avg] Deserialize memory: %.2f MiB%n", avgDesMem / (1024.0 * 1024.0));
    System.out.println();
  }
}
