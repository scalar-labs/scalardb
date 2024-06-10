package com.scalar.db.dataloader.core.util;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.dataloader.core.ColumnKeyValue;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KeyUtilsTest {

  @Mock private TableMetadata tableMetadata;

  @Test
  void parseKeyValue_nullKeyValue_returnsNull() throws KeyParsingException {
    assertNull(KeyUtils.parseKeyValue(null, null, null, tableMetadata));
  }

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

  @Test
  void createKey_boolean_returnsKey() throws KeyParsingException {
    String columnName = "booleanColumn";
    String value = "true";
    Key expected = Key.newBuilder().add(BooleanColumn.of(columnName, true)).build();
    Key actual = KeyUtils.createKey(DataType.BOOLEAN, columnName, value);
    assertEquals(expected, actual);
  }

  @Test
  void createKey_int_returnsKey() throws KeyParsingException {
    String columnName = "intColumn";
    String value = "42";
    Key expected = Key.newBuilder().add(IntColumn.of(columnName, 42)).build();
    Key actual = KeyUtils.createKey(DataType.INT, columnName, value);
    assertEquals(expected, actual);
  }

  @Test
  void createKey_bigint_returnsKey() throws KeyParsingException {
    String columnName = "bigintColumn";
    String value = "123456789012345";
    Key expected = Key.newBuilder().add(BigIntColumn.of(columnName, 123456789012345L)).build();
    Key actual = KeyUtils.createKey(DataType.BIGINT, columnName, value);
    assertEquals(expected, actual);
  }

  @Test
  void createKey_float_returnsKey() throws KeyParsingException {
    String columnName = "floatColumn";
    String value = "1.23";
    Key expected = Key.newBuilder().add(FloatColumn.of(columnName, 1.23f)).build();
    Key actual = KeyUtils.createKey(DataType.FLOAT, columnName, value);
    assertEquals(expected, actual);
  }

  @Test
  void createKey_double_returnsKey() throws KeyParsingException {
    String columnName = "doubleColumn";
    String value = "1.23";
    Key expected = Key.newBuilder().add(DoubleColumn.of(columnName, 1.23)).build();
    Key actual = KeyUtils.createKey(DataType.DOUBLE, columnName, value);
    assertEquals(expected, actual);
  }

  @Test
  void createKey_text_returnsKey() throws KeyParsingException {
    String columnName = "textColumn";
    String value = "Hello, world!";
    Key expected = Key.newBuilder().add(TextColumn.of(columnName, value)).build();
    Key actual = KeyUtils.createKey(DataType.TEXT, columnName, value);
    assertEquals(expected, actual);
  }

  @Test
  void createKey_blob_returnsKey() throws KeyParsingException {
    String columnName = "blobColumn";
    String value =
        Base64.getEncoder().encodeToString("Hello, world!".getBytes(StandardCharsets.UTF_8));
    Key expected =
        Key.newBuilder()
            .add(BlobColumn.of(columnName, "Hello, world!".getBytes(StandardCharsets.UTF_8)))
            .build();
    Key actual = KeyUtils.createKey(DataType.BLOB, columnName, value);
    assertEquals(expected, actual);
  }

  @Test
  void createKey_invalidBase64_throwsBase64Exception() {
    String columnName = "blobColumn";
    String value = "invalidBase64";
    assertThrows(
        KeyParsingException.class, () -> KeyUtils.createKey(DataType.BLOB, columnName, value));
  }
}
