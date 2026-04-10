package com.scalar.db.storage.objectstorage;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.luben.zstd.Zstd;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Tag("benchmark")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SerializerBenchmarkTest {

  private static final int RECORD_COUNT = 500_000;

  private ObjectStoragePartition partition;

  @BeforeAll
  void setUp() {
    System.out.println("=== Benchmark Setup ===");
    System.out.println("Generating " + RECORD_COUNT + " records...");

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

    // Estimate in-memory size (rough: ~1KB per record with 22 fields)
    long estimatedBytes = (long) RECORD_COUNT * 1024L;
    System.out.printf("Records generated: %,d%n", RECORD_COUNT);
    System.out.printf(
        "Estimated in-memory size: ~%.2f GiB%n", estimatedBytes / (1024.0 * 1024.0 * 1024.0));
    System.out.println();
  }

  enum Compression {
    NONE,
    GZIP,
    ZSTD
  }

  @ParameterizedTest(name = "Kryo+{0}")
  @EnumSource(Compression.class)
  void benchmark(Compression compression) {
    String name = compression == Compression.NONE ? "Kryo" : "Kryo+" + compression;

    // Warmup with a small partition to trigger class loading and JIT
    System.gc();
    warmup(compression);

    // Measure serialize memory
    System.gc();
    long serializeBaseline = usedMemory();

    long serializeStart = System.nanoTime();
    byte[] serialized = serialize(partition, compression);
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
    ObjectStoragePartition deserialized = deserialize(serialized, compression);
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

  private byte[] serialize(ObjectStoragePartition partition, Compression compression) {
    byte[] raw = Serializer.serialize(partition);
    switch (compression) {
      case GZIP:
        return compressGzip(raw);
      case ZSTD:
        return Zstd.compress(raw);
      default:
        return raw;
    }
  }

  private ObjectStoragePartition deserialize(byte[] data, Compression compression) {
    byte[] raw;
    switch (compression) {
      case GZIP:
        raw = decompressGzip(data);
        break;
      case ZSTD:
        raw = Zstd.decompress(data, (int) Zstd.decompressedSize(data));
        break;
      default:
        raw = data;
        break;
    }
    return Serializer.deserialize(raw);
  }

  private static byte[] compressGzip(byte[] data) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(data.length);
      try (GZIPOutputStream gzip = new GZIPOutputStream(baos)) {
        gzip.write(data);
      }
      return baos.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static byte[] decompressGzip(byte[] data) {
    try {
      ByteArrayInputStream bais = new ByteArrayInputStream(data);
      ByteArrayOutputStream baos = new ByteArrayOutputStream(data.length * 2);
      try (GZIPInputStream gzip = new GZIPInputStream(bais)) {
        byte[] buf = new byte[8192];
        int len;
        while ((len = gzip.read(buf)) != -1) {
          baos.write(buf, 0, len);
        }
      }
      return baos.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static long usedMemory() {
    Runtime rt = Runtime.getRuntime();
    return rt.totalMemory() - rt.freeMemory();
  }

  private void warmup(Compression compression) {
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

    byte[] data = serialize(smallPartition, compression);
    deserialize(data, compression);
  }
}
