package com.scalar.db.dataloader.core.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.ResultImpl;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.dataloader.core.ColumnInfo;
import com.scalar.db.dataloader.core.UnitTestUtils;
import com.scalar.db.dataloader.core.exception.Base64Exception;
import com.scalar.db.dataloader.core.exception.ColumnParsingException;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Unit tests for the ColumnUtils class which handles column creation and manipulation. Tests
 * various data type conversions and error handling scenarios.
 */
class ColumnUtilsTest {

  private static final float FLOAT_VALUE = 2.78f;
  private static final TableMetadata mockMetadata = UnitTestUtils.createTestTableMetadata();
  private static final ObjectNode sourceRecord = UnitTestUtils.getOutputDataWithMetadata();
  private static final Map<String, Column<?>> values = UnitTestUtils.createTestValues();
  private static final Result scalarDBResult = new ResultImpl(values, mockMetadata);

  /**
   * Provides test cases for column creation with different data types and values. Each test case
   * includes: - The target DataType - Column name - Input value (as string) - Expected Column
   * object
   *
   * @return Stream of Arguments containing test parameters
   */
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
        Arguments.of(DataType.BLOB, "blobColumn", null, BlobColumn.ofNull("blobColumn")),
        Arguments.of(
            DataType.DATE,
            "dateColumn",
            LocalDate.of(2000, 1, 1).toString(),
            DateColumn.of("dateColumn", LocalDate.of(2000, 1, 1))),
        Arguments.of(DataType.DATE, "dateColumn", null, DateColumn.ofNull("dateColumn")),
        Arguments.of(
            DataType.TIME,
            "timeColumn",
            LocalTime.of(1, 1, 1).toString(),
            TimeColumn.of("timeColumn", LocalTime.of(1, 1, 1))),
        Arguments.of(DataType.TIME, "timeColumn", null, TimeColumn.ofNull("timeColumn")),
        Arguments.of(
            DataType.TIMESTAMP,
            "timestampColumn",
            LocalDateTime.of(2000, 1, 1, 1, 1).toString(),
            TimestampColumn.of("timestampColumn", LocalDateTime.of(2000, 1, 1, 1, 1))),
        Arguments.of(
            DataType.TIMESTAMP, "timestampColumn", null, TimestampColumn.ofNull("timestampColumn")),
        Arguments.of(
            DataType.TIMESTAMPTZ,
            "timestampTZColumn",
            Instant.ofEpochMilli(1940041740).toString(),
            TimestampTZColumn.of("timestampTZColumn", Instant.ofEpochMilli(1940041740))),
        Arguments.of(
            DataType.TIMESTAMPTZ,
            "timestampTZColumn",
            null,
            TimestampTZColumn.ofNull("timestampTZColumn")));
  }

  /**
   * Tests column creation from string values for various data types. Verifies that the created
   * column matches the expected column with correct type and value.
   *
   * @param dataType The target ScalarDB data type
   * @param columnName Name of the column
   * @param value String value to convert
   * @param expectedColumn Expected Column object after conversion
   * @throws ColumnParsingException if the value cannot be parsed into the target data type
   */
  @ParameterizedTest
  @MethodSource("provideColumnsForCreateColumnFromValue")
  void createColumnFromValue_validInput_returnsColumn(
      DataType dataType, String columnName, String value, Column<?> expectedColumn)
      throws ColumnParsingException {
    ColumnInfo columnInfo = ColumnInfo.builder().columnName(columnName).build();
    Column<?> actualColumn = ColumnUtils.createColumnFromValue(dataType, columnInfo, value);
    assertEquals(expectedColumn, actualColumn);
  }

  /**
   * Tests that attempting to create a numeric column with an invalid number format throws a
   * ColumnParsingException with appropriate error message.
   */
  @Test
  void createColumnFromValue_invalidNumberFormat_throwsNumberFormatException() {
    String columnName = "intColumn";
    String value = "not_a_number";
    ColumnInfo columnInfo =
        ColumnInfo.builder().namespace("ns").tableName("table").columnName(columnName).build();
    ColumnParsingException exception =
        assertThrows(
            ColumnParsingException.class,
            () -> ColumnUtils.createColumnFromValue(DataType.INT, columnInfo, value));
    assertEquals(
        CoreError.DATA_LOADER_INVALID_NUMBER_FORMAT_FOR_COLUMN_VALUE.buildMessage(
            columnName, "table", "ns"),
        exception.getMessage());
  }

  /**
   * Tests that attempting to create a BLOB column with invalid Base64 encoding throws a
   * ColumnParsingException with appropriate error message.
   */
  @Test
  void createColumnFromValue_invalidBase64_throwsBase64Exception() {
    String columnName = "blobColumn";
    String value = "invalid_base64";
    ColumnInfo columnInfo =
        ColumnInfo.builder().namespace("ns").tableName("table").columnName(columnName).build();
    ColumnParsingException exception =
        assertThrows(
            ColumnParsingException.class,
            () -> ColumnUtils.createColumnFromValue(DataType.BLOB, columnInfo, value));
    assertEquals(
        CoreError.DATA_LOADER_INVALID_BASE64_ENCODING_FOR_COLUMN_VALUE.buildMessage(
            columnName, "table", "ns"),
        exception.getMessage());
  }

  /**
   * Tests the extraction of columns from a ScalarDB Result object. Verifies that all columns are
   * correctly extracted and converted from the source record.
   *
   * @throws Base64Exception if BLOB data contains invalid Base64 encoding
   * @throws ColumnParsingException if any column value cannot be parsed into its target data type
   */
  @Test
  void getColumnsFromResult_withValidData_shouldReturnColumns()
      throws Base64Exception, ColumnParsingException {
    List<Column<?>> columns =
        ColumnUtils.getColumnsFromResult(scalarDBResult, sourceRecord, false, mockMetadata);
    assertEquals(8, columns.size());
  }
}
