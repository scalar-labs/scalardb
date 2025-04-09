package com.scalar.db.dataloader.core.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.dataloader.core.ColumnInfo;
import com.scalar.db.dataloader.core.ColumnKeyValue;
import com.scalar.db.dataloader.core.UnitTestUtils;
import com.scalar.db.dataloader.core.exception.KeyParsingException;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit tests for the KeyUtils class which handles parsing and creation of database keys. Tests
 * cover various data types and key creation scenarios including partition and clustering keys.
 */
@ExtendWith(MockitoExtension.class)
class KeyUtilsTest {

  @Mock private TableMetadata tableMetadata;
  private static final Map<String, DataType> dataTypeByColumnName = UnitTestUtils.getColumnData();
  private static final ObjectNode sourceRecord = UnitTestUtils.getOutputDataWithMetadata();

  /** Tests that parsing a null key value returns null. */
  @Test
  void parseKeyValue_nullKeyValue_returnsNull() throws KeyParsingException {
    assertNull(KeyUtils.parseKeyValue(null, null, null, tableMetadata));
  }

  /**
   * Tests that attempting to parse a key value with an invalid column name throws
   * KeyParsingException. The exception should contain appropriate error message with namespace and
   * table details.
   */
  @Test
  void parseKeyValue_invalidColumnName_throwsKeyParsingException() {
    String columnName = "invalidColumn";
    ColumnKeyValue keyValue = new ColumnKeyValue(columnName, "value");
    when(tableMetadata.getColumnDataType(columnName)).thenReturn(null);

    KeyParsingException exception =
        assertThrows(
            KeyParsingException.class,
            () -> KeyUtils.parseKeyValue(keyValue, "namespace", "table", tableMetadata));
    assertEquals(
        CoreError.DATA_LOADER_INVALID_COLUMN_NON_EXISTENT.buildMessage(
            columnName, "table", "namespace"),
        exception.getMessage());
  }

  /** Tests successful parsing of a valid key value with TEXT data type. */
  @Test
  void parseKeyValue_validKeyValue_returnsKey() throws KeyParsingException {
    String columnName = "columnName";
    String value = "value";
    ColumnKeyValue keyValue = new ColumnKeyValue(columnName, value);
    DataType dataType = DataType.TEXT;
    when(tableMetadata.getColumnDataType(columnName)).thenReturn(dataType);

    Key expected = Key.newBuilder().add(TextColumn.of(columnName, value)).build();
    Key actual = KeyUtils.parseKeyValue(keyValue, "namespace", "table", tableMetadata);

    assertEquals(expected, actual);
  }

  /** Tests creation of a key with BOOLEAN data type. */
  @Test
  void createKey_boolean_returnsKey() throws KeyParsingException {
    String columnName = "booleanColumn";
    String value = "true";
    ColumnInfo columnInfo = ColumnInfo.builder().columnName(columnName).build();
    Key expected = Key.newBuilder().add(BooleanColumn.of(columnName, true)).build();
    Key actual = KeyUtils.createKey(DataType.BOOLEAN, columnInfo, value);
    assertEquals(expected, actual);
  }

  /** Tests creation of a key with INT data type. */
  @Test
  void createKey_int_returnsKey() throws KeyParsingException {
    String columnName = "intColumn";
    String value = "42";
    ColumnInfo columnInfo = ColumnInfo.builder().columnName(columnName).build();
    Key expected = Key.newBuilder().add(IntColumn.of(columnName, 42)).build();
    Key actual = KeyUtils.createKey(DataType.INT, columnInfo, value);
    assertEquals(expected, actual);
  }

  /** Tests creation of a key with BIGINT data type. */
  @Test
  void createKey_bigint_returnsKey() throws KeyParsingException {
    String columnName = "bigintColumn";
    String value = "123456789012345";
    ColumnInfo columnInfo = ColumnInfo.builder().columnName(columnName).build();
    Key expected = Key.newBuilder().add(BigIntColumn.of(columnName, 123456789012345L)).build();
    Key actual = KeyUtils.createKey(DataType.BIGINT, columnInfo, value);
    assertEquals(expected, actual);
  }

  /** Tests creation of a key with FLOAT data type. */
  @Test
  void createKey_float_returnsKey() throws KeyParsingException {
    String columnName = "floatColumn";
    String value = "1.23";
    ColumnInfo columnInfo = ColumnInfo.builder().columnName(columnName).build();
    Key expected = Key.newBuilder().add(FloatColumn.of(columnName, 1.23f)).build();
    Key actual = KeyUtils.createKey(DataType.FLOAT, columnInfo, value);
    assertEquals(expected, actual);
  }

  /** Tests creation of a key with DOUBLE data type. */
  @Test
  void createKey_double_returnsKey() throws KeyParsingException {
    String columnName = "doubleColumn";
    String value = "1.23";
    ColumnInfo columnInfo = ColumnInfo.builder().columnName(columnName).build();
    Key expected = Key.newBuilder().add(DoubleColumn.of(columnName, 1.23)).build();
    Key actual = KeyUtils.createKey(DataType.DOUBLE, columnInfo, value);
    assertEquals(expected, actual);
  }

  /** Tests creation of a key with TEXT data type. */
  @Test
  void createKey_text_returnsKey() throws KeyParsingException {
    String columnName = "textColumn";
    String value = "Hello, world!";
    ColumnInfo columnInfo = ColumnInfo.builder().columnName(columnName).build();
    Key expected = Key.newBuilder().add(TextColumn.of(columnName, value)).build();
    Key actual = KeyUtils.createKey(DataType.TEXT, columnInfo, value);
    assertEquals(expected, actual);
  }

  /** Tests creation of a key with BLOB data type using Base64 encoded input. */
  @Test
  void createKey_blob_returnsKey() throws KeyParsingException {
    String columnName = "blobColumn";
    ColumnInfo columnInfo = ColumnInfo.builder().columnName(columnName).build();
    String value =
        Base64.getEncoder().encodeToString("Hello, world!".getBytes(StandardCharsets.UTF_8));
    Key expected =
        Key.newBuilder()
            .add(BlobColumn.of(columnName, "Hello, world!".getBytes(StandardCharsets.UTF_8)))
            .build();
    Key actual = KeyUtils.createKey(DataType.BLOB, columnInfo, value);
    assertEquals(expected, actual);
  }

  /**
   * Tests that attempting to create a BLOB key with invalid Base64 input throws
   * KeyParsingException.
   */
  @Test
  void createKey_invalidBase64_throwsBase64Exception() {
    String columnName = "blobColumn";
    String value = "invalidBase64";
    ColumnInfo columnInfo = ColumnInfo.builder().columnName(columnName).build();
    assertThrows(
        KeyParsingException.class, () -> KeyUtils.createKey(DataType.BLOB, columnInfo, value));
  }

  /** Tests that creating a clustering key from an empty set returns an empty Optional. */
  @Test
  void createClusteringKeyFromSource_withEmptyClusteringKeySet_shouldReturnEmpty() {
    Optional<Key> key = KeyUtils.createClusteringKeyFromSource(Collections.EMPTY_SET, null, null);
    assertEquals(Optional.empty(), key);
  }

  /**
   * Tests creation of a clustering key from a valid set of clustering columns. Verifies that the
   * resulting key contains the expected INT and BOOLEAN values.
   */
  @Test
  void createClusteringKeyFromSource_withValidClusteringKeySet_shouldReturnValidKey() {
    Set<String> clusterKeySet = new HashSet<>();
    clusterKeySet.add(UnitTestUtils.TEST_COLUMN_2_CK);
    clusterKeySet.add(UnitTestUtils.TEST_COLUMN_3_CK);
    Optional<Key> key =
        KeyUtils.createClusteringKeyFromSource(clusterKeySet, dataTypeByColumnName, sourceRecord);
    assertEquals(
        "Optional[Key{IntColumn{name=col2, value=2147483647, hasNullValue=false}, BooleanColumn{name=col3, value=true, hasNullValue=false}}]",
        key.toString());
  }

  /**
   * Tests that attempting to create a partition key with invalid data returns an empty Optional.
   */
  @Test
  void createPartitionKeyFromSource_withInvalidData_shouldReturnEmpty() {
    Set<String> partitionKeySet = new HashSet<>();
    partitionKeySet.add("id1");
    Optional<Key> key =
        KeyUtils.createPartitionKeyFromSource(partitionKeySet, dataTypeByColumnName, sourceRecord);
    assertEquals(Optional.empty(), key);
  }

  /**
   * Tests creation of a partition key from valid data. Verifies that the resulting key contains the
   * expected BIGINT value.
   */
  @Test
  void createPartitionKeyFromSource_withValidData_shouldReturnValidKey() {
    Set<String> partitionKeySet = new HashSet<>();
    partitionKeySet.add(UnitTestUtils.TEST_COLUMN_1_PK);
    Optional<Key> key =
        KeyUtils.createPartitionKeyFromSource(partitionKeySet, dataTypeByColumnName, sourceRecord);
    assertEquals(
        "Optional[Key{BigIntColumn{name=col1, value=9007199254740992, hasNullValue=false}}]",
        key.toString());
  }
}
