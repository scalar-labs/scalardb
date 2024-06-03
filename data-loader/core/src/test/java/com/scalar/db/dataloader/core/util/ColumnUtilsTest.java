package com.scalar.db.dataloader.core.util;

import static org.junit.jupiter.api.Assertions.*;

import com.scalar.db.common.error.CoreError;
import com.scalar.db.dataloader.core.exception.Base64Exception;
import com.scalar.db.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ColumnUtilsTest {

  private static final float FLOAT_VALUE = 2.78f;

  private static Stream<Arguments> provideColumnsForCreateColumnFromValue() {
    return Stream.of(
        Arguments.of(DataType.BOOLEAN, "boolColumn", "true", BooleanColumn.of("boolColumn", true)),
        Arguments.of(DataType.BOOLEAN, "boolColumn", null, BooleanColumn.ofNull("boolColumn")),
        Arguments.of(DataType.INT, "intColumn", "42", IntColumn.of("intColumn", 42)),
        Arguments.of(DataType.INT, "intColumn", null, IntColumn.ofNull("intColumn")),
        Arguments.of(
            DataType.BIGINT,
            "bigintColumn",
            "123456789012",
            BigIntColumn.of("bigintColumn", 123456789012L)),
        Arguments.of(DataType.BIGINT, "bigintColumn", null, BigIntColumn.ofNull("bigintColumn")),
        Arguments.of(
            DataType.FLOAT,
            "floatColumn",
            Float.toString(FLOAT_VALUE),
            FloatColumn.of("floatColumn", FLOAT_VALUE)),
        Arguments.of(DataType.FLOAT, "floatColumn", null, FloatColumn.ofNull("floatColumn")),
        Arguments.of(
            DataType.DOUBLE,
            "doubleColumn",
            Double.toString(Math.E),
            DoubleColumn.of("doubleColumn", Math.E)),
        Arguments.of(DataType.DOUBLE, "doubleColumn", null, DoubleColumn.ofNull("doubleColumn")),
        Arguments.of(
            DataType.TEXT,
            "textColumn",
            "Hello, world!",
            TextColumn.of("textColumn", "Hello, world!")),
        Arguments.of(DataType.TEXT, "textColumn", null, TextColumn.ofNull("textColumn")),
        Arguments.of(
            DataType.BLOB,
            "blobColumn",
            Base64.getEncoder().encodeToString("binary".getBytes(StandardCharsets.UTF_8)),
            BlobColumn.of("blobColumn", "binary".getBytes(StandardCharsets.UTF_8))),
        Arguments.of(DataType.BLOB, "blobColumn", null, BlobColumn.ofNull("blobColumn")));
  }

  @ParameterizedTest
  @MethodSource("provideColumnsForCreateColumnFromValue")
  void createColumnFromValue_validInput_returnsColumn(
      DataType dataType, String columnName, String value, Column<?> expectedColumn)
      throws Base64Exception {
    Column<?> actualColumn = ColumnUtils.createColumnFromValue(dataType, columnName, value);
    assertEquals(expectedColumn, actualColumn);
  }

  @Test
  void createColumnFromValue_invalidNumberFormat_throwsNumberFormatException() {
    String columnName = "intColumn";
    String value = "not_a_number";
    NumberFormatException exception =
        assertThrows(
            NumberFormatException.class,
            () -> ColumnUtils.createColumnFromValue(DataType.INT, columnName, value));
    assertEquals(
        CoreError.DATA_LOADER_INVALID_NUMBER_FORMAT_FOR_COLUMN_VALUE.buildMessage(columnName),
        exception.getMessage());
  }

  @Test
  void createColumnFromValue_invalidBase64_throwsBase64Exception() {
    String columnName = "blobColumn";
    String value = "invalid_base64";
    Base64Exception exception =
        assertThrows(
            Base64Exception.class,
            () -> ColumnUtils.createColumnFromValue(DataType.BLOB, columnName, value));
    assertEquals(
        CoreError.DATA_LOADER_INVALID_BASE64_ENCODING_FOR_COLUMN_VALUE.buildMessage(columnName),
        exception.getMessage());
  }
}
