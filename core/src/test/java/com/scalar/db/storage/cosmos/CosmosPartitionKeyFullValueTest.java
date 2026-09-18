package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Key;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Verifies ScalarDB stores and returns the full partition key value without truncation.
 *
 * <p>In Cosmos DB V1, only the first 101 bytes are used for partition routing (hashing). ScalarDB
 * still persists and reads back the complete partition key in {@code concatenatedPartitionKey} and
 * in each partition-key column. V2 uses the same storage model with a higher routing limit (2048
 * bytes).
 */
public class CosmosPartitionKeyFullValueTest {

  private static final int V1_PARTITION_KEY_ROUTING_UTF8_BYTES = 101;
  private static final int V2_PARTITION_KEY_ROUTING_UTF8_BYTES = 2048;

  private static final String NAMESPACE = "namespace";
  private static final String TABLE = "table";
  private static final String PARTITION_KEY_COLUMN = "pk";

  @Mock private TableMetadata metadata;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    when(metadata.getPartitionKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList(PARTITION_KEY_COLUMN)));
  }

  @ParameterizedTest
  @ValueSource(ints = {101, 102, 200, 2048})
  public void makeRecord_PutGiven_ShouldStoreFullPartitionKeyValue(int byteLength) {
    // Arrange
    String partitionKeyValue = asciiOfLength(byteLength);
    Put put = putWithPartitionKey(partitionKeyValue);
    CosmosMutation cosmosMutation = new CosmosMutation(put, metadata);

    // Act
    Record record = cosmosMutation.makeRecord();

    // Assert
    assertThat(record.getConcatenatedPartitionKey()).isEqualTo(partitionKeyValue);
    assertThat(record.getConcatenatedPartitionKey().getBytes(StandardCharsets.UTF_8).length)
        .isEqualTo(byteLength);
    assertThat(record.getPartitionKey().get(PARTITION_KEY_COLUMN)).isEqualTo(partitionKeyValue);

    printStoredPartitionKeyComparison(
        "makeRecord (write path)",
        byteLength,
        record.getConcatenatedPartitionKey(),
        (String) record.getPartitionKey().get(PARTITION_KEY_COLUMN));
  }

  @Test
  public void interpret_RecordWith102BytePartitionKey_ShouldReturnFullPartitionKeyColumn() {
    // V1 routing uses only the first 101 bytes, but the stored column value must remain intact.
    interpretShouldReturnFullPartitionKey(102, "V1");
  }

  @Test
  public void interpret_RecordWith200BytePartitionKey_ShouldReturnFullPartitionKeyColumn() {
    // Typical V2-length partition key.
    interpretShouldReturnFullPartitionKey(200, "V2");
  }

  @Test
  public void makeRecordAndInterpret_PutGiven_ShouldRoundTripFullPartitionKeyValue() {
    // Arrange
    String partitionKeyValue = asciiOfLength(150);
    Put put = putWithPartitionKey(partitionKeyValue);
    CosmosMutation cosmosMutation = new CosmosMutation(put, metadata);
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(PARTITION_KEY_COLUMN, com.scalar.db.io.DataType.TEXT)
            .addPartitionKey(PARTITION_KEY_COLUMN)
            .build();

    // Act
    Record record = cosmosMutation.makeRecord();
    Result result = new ResultInterpreter(Collections.emptyList(), tableMetadata).interpret(record);
    String retrievedColumnValue = result.getText(PARTITION_KEY_COLUMN);

    // Assert
    assertThat(retrievedColumnValue).isEqualTo(partitionKeyValue);
    assertThat(retrievedColumnValue.getBytes(StandardCharsets.UTF_8).length).isEqualTo(150);

    System.out.println();
    System.out.println("=== Round-trip (write -> read) ===");
    printStoredPartitionKeyComparison(
        "makeRecord (stored document)",
        150,
        record.getConcatenatedPartitionKey(),
        (String) record.getPartitionKey().get(PARTITION_KEY_COLUMN));
    printRetrievedPartitionKeyComparison(
        "ResultInterpreter (read path)", 150, "V1", retrievedColumnValue);
  }

  private void interpretShouldReturnFullPartitionKey(int byteLength, String cosmosVersionLabel) {
    String partitionKeyValue = asciiOfLength(byteLength);
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(PARTITION_KEY_COLUMN, com.scalar.db.io.DataType.TEXT)
            .addPartitionKey(PARTITION_KEY_COLUMN)
            .build();
    Record record =
        new Record(
            "id",
            partitionKeyValue,
            Collections.singletonMap(PARTITION_KEY_COLUMN, partitionKeyValue),
            Collections.emptyMap(),
            Collections.emptyMap());

    Result result = new ResultInterpreter(Collections.emptyList(), tableMetadata).interpret(record);
    String retrievedColumnValue = result.getText(PARTITION_KEY_COLUMN);

    assertThat(retrievedColumnValue).isEqualTo(partitionKeyValue);
    assertThat(retrievedColumnValue.getBytes(StandardCharsets.UTF_8).length).isEqualTo(byteLength);

    printRetrievedPartitionKeyComparison(
        "ResultInterpreter (read path)", byteLength, cosmosVersionLabel, retrievedColumnValue);
  }

  private static void printStoredPartitionKeyComparison(
      String stage, int fullByteLength, String concatenatedPartitionKey, String columnValue) {
    System.out.println();
    System.out.println("=== " + stage + " ===");
    System.out.println("Full concatenatedPartitionKey UTF-8 bytes: " + fullByteLength);
    System.out.println("Full concatenatedPartitionKey value: " + preview(concatenatedPartitionKey));
    System.out.println(
        "Full partition-key column ("
            + PARTITION_KEY_COLUMN
            + ") UTF-8 bytes: "
            + columnValue.getBytes(StandardCharsets.UTF_8).length);
    System.out.println("Full partition-key column value: " + preview(columnValue));
    printRoutingPrefixComparison(fullByteLength, concatenatedPartitionKey, "V1");
    printRoutingPrefixComparison(fullByteLength, concatenatedPartitionKey, "V2");
  }

  private static void printRetrievedPartitionKeyComparison(
      String stage, int fullByteLength, String cosmosVersionLabel, String retrievedColumnValue) {
    System.out.println();
    System.out.println("=== " + stage + " [" + cosmosVersionLabel + " container context] ===");
    System.out.println(
        "Retrieved partition-key column ("
            + PARTITION_KEY_COLUMN
            + ") UTF-8 bytes: "
            + retrievedColumnValue.getBytes(StandardCharsets.UTF_8).length);
    System.out.println("Retrieved partition-key column value: " + preview(retrievedColumnValue));
    printRoutingPrefixComparison(fullByteLength, retrievedColumnValue, cosmosVersionLabel);
    System.out.println(
        "Note: ScalarDB returns the full column value. Cosmos "
            + cosmosVersionLabel
            + " uses only the routing prefix above for partition hashing.");
  }

  private static void printRoutingPrefixComparison(
      int fullByteLength, String fullValue, String cosmosVersionLabel) {
    int routingLimit =
        "V2".equals(cosmosVersionLabel)
            ? V2_PARTITION_KEY_ROUTING_UTF8_BYTES
            : V1_PARTITION_KEY_ROUTING_UTF8_BYTES;
    String routingPrefix = utf8Prefix(fullValue, routingLimit);
    int routingPrefixBytes = routingPrefix.getBytes(StandardCharsets.UTF_8).length;

    System.out.println(
        cosmosVersionLabel
            + " routing prefix (first "
            + routingLimit
            + " UTF-8 bytes used by Cosmos for hashing): "
            + routingPrefixBytes
            + " bytes");
    System.out.println(cosmosVersionLabel + " routing prefix value: " + preview(routingPrefix));
    if (fullByteLength > routingLimit) {
      System.out.println(
          cosmosVersionLabel
              + " routing prefix differs from full key: suffix after byte "
              + routingLimit
              + " is NOT used for routing but IS still stored/returned.");
    } else {
      System.out.println(
          cosmosVersionLabel + " routing prefix equals full key (key fits within routing limit).");
    }
  }

  private static String utf8Prefix(String value, int maxUtf8Bytes) {
    if (maxUtf8Bytes <= 0 || value.isEmpty()) {
      return "";
    }
    byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
    if (utf8.length <= maxUtf8Bytes) {
      return value;
    }
    return new String(utf8, 0, maxUtf8Bytes, StandardCharsets.UTF_8);
  }

  private static String preview(String value) {
    int previewLength = Math.min(value.length(), 120);
    String excerpt = value.substring(0, previewLength);
    if (value.length() > previewLength) {
      return excerpt
          + "... [truncated in log output only, full length="
          + value.length()
          + " chars]";
    }
    return excerpt;
  }

  private static Put putWithPartitionKey(String partitionKeyValue) {
    return Put.newBuilder()
        .namespace(NAMESPACE)
        .table(TABLE)
        .partitionKey(Key.ofText(PARTITION_KEY_COLUMN, partitionKeyValue))
        .textValue("col", "value")
        .build();
  }

  private static String asciiOfLength(int numBytes) {
    if (numBytes <= 0) {
      return "";
    }
    return String.join("", Collections.nCopies(numBytes, "x"));
  }
}
