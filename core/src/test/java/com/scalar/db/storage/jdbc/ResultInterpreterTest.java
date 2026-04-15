package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ResultInterpreterTest {

  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_COLUMN_NAME_1 = "col1";
  private static final String ANY_COLUMN_NAME_2 = "col2";
  private static final String ANY_COLUMN_NAME_3 = "col3";
  private static final String ANY_COLUMN_NAME_4 = "col4";
  private static final String ANY_COLUMN_NAME_5 = "col5";
  private static final String ANY_COLUMN_NAME_6 = "col6";
  private static final String ANY_COLUMN_NAME_7 = "col7";
  private static final String ANY_COLUMN_NAME_8 = "col8";
  private static final String ANY_COLUMN_NAME_9 = "col9";
  private static final String ANY_COLUMN_NAME_10 = "col10";
  private static final String ANY_COLUMN_NAME_11 = "col11";

  private static final TableMetadata TABLE_METADATA =
      TableMetadata.newBuilder()
          .addColumn(ANY_NAME_1, DataType.TEXT)
          .addColumn(ANY_NAME_2, DataType.TEXT)
          .addColumn(ANY_COLUMN_NAME_1, DataType.BOOLEAN)
          .addColumn(ANY_COLUMN_NAME_2, DataType.INT)
          .addColumn(ANY_COLUMN_NAME_3, DataType.BIGINT)
          .addColumn(ANY_COLUMN_NAME_4, DataType.FLOAT)
          .addColumn(ANY_COLUMN_NAME_5, DataType.DOUBLE)
          .addColumn(ANY_COLUMN_NAME_6, DataType.TEXT)
          .addColumn(ANY_COLUMN_NAME_7, DataType.BLOB)
          .addColumn(ANY_COLUMN_NAME_8, DataType.DATE)
          .addColumn(ANY_COLUMN_NAME_9, DataType.TIME)
          .addColumn(ANY_COLUMN_NAME_10, DataType.TIMESTAMP)
          .addColumn(ANY_COLUMN_NAME_11, DataType.TIMESTAMPTZ)
          .addPartitionKey(ANY_NAME_1)
          .addClusteringKey(ANY_NAME_2)
          .build();

  private static final LocalDate ANY_DATE = DateColumn.MAX_VALUE;
  private static final LocalTime ANY_TIME = TimeColumn.MAX_VALUE;
  private static final LocalDateTime ANY_TIMESTAMP = TimestampColumn.MAX_VALUE;
  private static final Instant ANY_TIMESTAMPTZ = TimestampTZColumn.MAX_VALUE;

  @Mock private ResultSet resultSet;
  @Mock private RdbEngineStrategy rdbEngine;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  @Test
  public void interpret_ShouldReturnWhatsSet() throws SQLException {
    // Arrange
    when(resultSet.getString(ANY_NAME_1)).thenReturn(ANY_TEXT_1);
    when(resultSet.getString(ANY_NAME_2)).thenReturn(ANY_TEXT_2);
    when(resultSet.getBoolean(ANY_COLUMN_NAME_1)).thenReturn(true);
    when(resultSet.getInt(ANY_COLUMN_NAME_2)).thenReturn(Integer.MAX_VALUE);
    when(resultSet.getLong(ANY_COLUMN_NAME_3)).thenReturn(Long.MAX_VALUE);
    when(resultSet.getDouble(ANY_COLUMN_NAME_4)).thenReturn((double) Float.MAX_VALUE);
    when(resultSet.getDouble(ANY_COLUMN_NAME_5)).thenReturn(Double.MAX_VALUE);
    when(resultSet.getString(ANY_COLUMN_NAME_6)).thenReturn("string");
    when(resultSet.getBytes(ANY_COLUMN_NAME_7))
        .thenReturn("bytes".getBytes(StandardCharsets.UTF_8));
    when(rdbEngine.parseDateColumn(resultSet, ANY_COLUMN_NAME_8))
        .thenReturn(DateColumn.of(ANY_COLUMN_NAME_8, ANY_DATE));
    when(rdbEngine.parseTimeColumn(resultSet, ANY_COLUMN_NAME_9))
        .thenReturn(TimeColumn.of(ANY_COLUMN_NAME_9, ANY_TIME));
    when(rdbEngine.parseTimestampColumn(resultSet, ANY_COLUMN_NAME_10))
        .thenReturn(TimestampColumn.of(ANY_COLUMN_NAME_10, ANY_TIMESTAMP));
    when(rdbEngine.parseTimestampTZColumn(resultSet, ANY_COLUMN_NAME_11))
        .thenReturn(TimestampTZColumn.of(ANY_COLUMN_NAME_11, ANY_TIMESTAMPTZ));
    when(resultSet.wasNull()).thenReturn(false);

    List<String> projections = Collections.emptyList();

    ResultInterpreter interpreter = new ResultInterpreter(projections, TABLE_METADATA, rdbEngine);

    // Act
    Result result = interpreter.interpret(resultSet);

    // Assert
    assertThat(result.contains(ANY_NAME_1)).isTrue();
    assertThat(result.isNull(ANY_NAME_1)).isFalse();
    assertThat(result.getText(ANY_NAME_1)).isEqualTo(ANY_TEXT_1);
    assertThat(result.contains(ANY_NAME_2)).isTrue();
    assertThat(result.isNull(ANY_NAME_2)).isFalse();
    assertThat(result.getText(ANY_NAME_2)).isEqualTo(ANY_TEXT_2);
    assertThat(result.contains(ANY_COLUMN_NAME_1)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_1)).isFalse();
    assertThat(result.getBoolean(ANY_COLUMN_NAME_1)).isTrue();
    assertThat(result.contains(ANY_COLUMN_NAME_2)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_2)).isFalse();
    assertThat(result.getInt(ANY_COLUMN_NAME_2)).isEqualTo(Integer.MAX_VALUE);
    assertThat(result.contains(ANY_COLUMN_NAME_3)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_3)).isFalse();
    assertThat(result.getBigInt(ANY_COLUMN_NAME_3)).isEqualTo(Long.MAX_VALUE);
    assertThat(result.contains(ANY_COLUMN_NAME_4)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_4)).isFalse();
    assertThat(result.getFloat(ANY_COLUMN_NAME_4)).isEqualTo(Float.MAX_VALUE);
    assertThat(result.contains(ANY_COLUMN_NAME_5)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_5)).isFalse();
    assertThat(result.getDouble(ANY_COLUMN_NAME_5)).isEqualTo(Double.MAX_VALUE);
    assertThat(result.contains(ANY_COLUMN_NAME_6)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_6)).isFalse();
    assertThat(result.getText(ANY_COLUMN_NAME_6)).isEqualTo("string");
    assertThat(result.contains(ANY_COLUMN_NAME_7)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_7)).isFalse();
    assertThat(result.getBlob(ANY_COLUMN_NAME_7))
        .isEqualTo(ByteBuffer.wrap("bytes".getBytes(StandardCharsets.UTF_8)));
    assertThat(result.getBlobAsBytes(ANY_COLUMN_NAME_7))
        .isEqualTo("bytes".getBytes(StandardCharsets.UTF_8));
    assertThat(result.contains(ANY_COLUMN_NAME_8)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_8)).isFalse();
    assertThat(result.getDate(ANY_COLUMN_NAME_8)).isEqualTo(ANY_DATE);
    assertThat(result.contains(ANY_COLUMN_NAME_9)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_9)).isFalse();
    assertThat(result.getTime(ANY_COLUMN_NAME_9)).isEqualTo(ANY_TIME);
    assertThat(result.contains(ANY_COLUMN_NAME_10)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_10)).isFalse();
    assertThat(result.getTimestamp(ANY_COLUMN_NAME_10)).isEqualTo(ANY_TIMESTAMP);
    assertThat(result.contains(ANY_COLUMN_NAME_11)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_11)).isFalse();
    assertThat(result.getTimestampTZ(ANY_COLUMN_NAME_11)).isEqualTo(ANY_TIMESTAMPTZ);

    Map<String, Column<?>> columns = result.getColumns();
    assertThat(columns.containsKey(ANY_NAME_1)).isTrue();
    assertThat(columns.get(ANY_NAME_1).hasNullValue()).isFalse();
    assertThat(columns.get(ANY_NAME_1).getTextValue()).isEqualTo(ANY_TEXT_1);
    assertThat(columns.containsKey(ANY_NAME_2)).isTrue();
    assertThat(columns.get(ANY_NAME_2).hasNullValue()).isFalse();
    assertThat(columns.get(ANY_NAME_2).getTextValue()).isEqualTo(ANY_TEXT_2);
    assertThat(columns.containsKey(ANY_COLUMN_NAME_1)).isTrue();
    assertThat(columns.get(ANY_COLUMN_NAME_1).hasNullValue()).isFalse();
    assertThat(columns.get(ANY_COLUMN_NAME_1).getBooleanValue()).isTrue();
    assertThat(columns.containsKey(ANY_COLUMN_NAME_2)).isTrue();
    assertThat(columns.get(ANY_COLUMN_NAME_2).hasNullValue()).isFalse();
    assertThat(columns.get(ANY_COLUMN_NAME_2).getIntValue()).isEqualTo(Integer.MAX_VALUE);
    assertThat(columns.containsKey(ANY_COLUMN_NAME_3)).isTrue();
    assertThat(columns.get(ANY_COLUMN_NAME_3).hasNullValue()).isFalse();
    assertThat(columns.get(ANY_COLUMN_NAME_3).getBigIntValue()).isEqualTo(Long.MAX_VALUE);
    assertThat(columns.containsKey(ANY_COLUMN_NAME_4)).isTrue();
    assertThat(columns.get(ANY_COLUMN_NAME_4).hasNullValue()).isFalse();
    assertThat(columns.get(ANY_COLUMN_NAME_4).getFloatValue()).isEqualTo(Float.MAX_VALUE);
    assertThat(columns.containsKey(ANY_COLUMN_NAME_5)).isTrue();
    assertThat(columns.get(ANY_COLUMN_NAME_5).hasNullValue()).isFalse();
    assertThat(columns.get(ANY_COLUMN_NAME_5).getDoubleValue()).isEqualTo(Double.MAX_VALUE);
    assertThat(columns.containsKey(ANY_COLUMN_NAME_6)).isTrue();
    assertThat(columns.get(ANY_COLUMN_NAME_6).hasNullValue()).isFalse();
    assertThat(columns.get(ANY_COLUMN_NAME_6).getTextValue()).isEqualTo("string");
    assertThat(columns.containsKey(ANY_COLUMN_NAME_7)).isTrue();
    assertThat(columns.get(ANY_COLUMN_NAME_7).hasNullValue()).isFalse();
    assertThat(columns.get(ANY_COLUMN_NAME_7).getBlobValueAsBytes())
        .isEqualTo("bytes".getBytes(StandardCharsets.UTF_8));
    assertThat(columns.get(ANY_COLUMN_NAME_7).getBlobValueAsByteBuffer())
        .isEqualTo(ByteBuffer.wrap("bytes".getBytes(StandardCharsets.UTF_8)));
    assertThat(columns.containsKey(ANY_COLUMN_NAME_8)).isTrue();
    assertThat(columns.get(ANY_COLUMN_NAME_8).hasNullValue()).isFalse();
    assertThat(columns.get(ANY_COLUMN_NAME_8).getDateValue()).isEqualTo(ANY_DATE);
    assertThat(columns.containsKey(ANY_COLUMN_NAME_9)).isTrue();
    assertThat(columns.get(ANY_COLUMN_NAME_9).hasNullValue()).isFalse();
    assertThat(columns.get(ANY_COLUMN_NAME_9).getTimeValue()).isEqualTo(ANY_TIME);
    assertThat(columns.containsKey(ANY_COLUMN_NAME_10)).isTrue();
    assertThat(columns.get(ANY_COLUMN_NAME_10).hasNullValue()).isFalse();
    assertThat(columns.get(ANY_COLUMN_NAME_10).getTimestampValue()).isEqualTo(ANY_TIMESTAMP);
    assertThat(columns.containsKey(ANY_COLUMN_NAME_11)).isTrue();
    assertThat(columns.get(ANY_COLUMN_NAME_11).hasNullValue()).isFalse();
    assertThat(columns.get(ANY_COLUMN_NAME_11).getTimestampTZValue()).isEqualTo(ANY_TIMESTAMPTZ);
  }

  @Test
  public void interpret_ShouldReturnWhatsSetWithNullValues() throws SQLException {
    // Arrange
    when(resultSet.getString(ANY_NAME_1)).thenReturn(ANY_TEXT_1);
    when(resultSet.getString(ANY_NAME_2)).thenReturn(ANY_TEXT_2);
    when(resultSet.getBoolean(ANY_COLUMN_NAME_1)).thenReturn(false);
    when(resultSet.getInt(ANY_COLUMN_NAME_2)).thenReturn(0);
    when(resultSet.getLong(ANY_COLUMN_NAME_3)).thenReturn(0L);
    when(resultSet.getDouble(ANY_COLUMN_NAME_4)).thenReturn(0.0D);
    when(resultSet.getDouble(ANY_COLUMN_NAME_5)).thenReturn(0.0D);
    when(resultSet.getString(ANY_COLUMN_NAME_6)).thenReturn(null);
    when(resultSet.getBytes(ANY_COLUMN_NAME_7)).thenReturn(null);
    when(rdbEngine.parseDateColumn(resultSet, ANY_COLUMN_NAME_8))
        .thenReturn(DateColumn.ofNull(ANY_COLUMN_NAME_8));
    when(rdbEngine.parseTimeColumn(resultSet, ANY_COLUMN_NAME_9))
        .thenReturn(TimeColumn.ofNull(ANY_COLUMN_NAME_9));
    when(rdbEngine.parseTimestampColumn(resultSet, ANY_COLUMN_NAME_10))
        .thenReturn(TimestampColumn.ofNull(ANY_COLUMN_NAME_10));
    when(rdbEngine.parseTimestampTZColumn(resultSet, ANY_COLUMN_NAME_11))
        .thenReturn(TimestampTZColumn.ofNull(ANY_COLUMN_NAME_11));
    when(resultSet.wasNull()).thenReturn(false).thenReturn(false).thenReturn(true);

    List<String> projections = Collections.emptyList();

    ResultInterpreter interpreter =
        spy(new ResultInterpreter(projections, TABLE_METADATA, rdbEngine));

    // Act
    Result result = interpreter.interpret(resultSet);

    // Assert
    assertThat(result.contains(ANY_NAME_1)).isTrue();
    assertThat(result.isNull(ANY_NAME_1)).isFalse();
    assertThat(result.getText(ANY_NAME_1)).isEqualTo(ANY_TEXT_1);
    assertThat(result.contains(ANY_NAME_2)).isTrue();
    assertThat(result.isNull(ANY_NAME_2)).isFalse();
    assertThat(result.getText(ANY_NAME_2)).isEqualTo(ANY_TEXT_2);
    assertThat(result.contains(ANY_COLUMN_NAME_1)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_1)).isTrue();
    assertThat(result.getBoolean(ANY_COLUMN_NAME_1)).isFalse();
    assertThat(result.contains(ANY_COLUMN_NAME_2)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_2)).isTrue();
    assertThat(result.getInt(ANY_COLUMN_NAME_2)).isEqualTo(0);
    assertThat(result.contains(ANY_COLUMN_NAME_3)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_3)).isTrue();
    assertThat(result.getBigInt(ANY_COLUMN_NAME_3)).isEqualTo(0L);
    assertThat(result.contains(ANY_COLUMN_NAME_4)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_4)).isTrue();
    assertThat(result.getFloat(ANY_COLUMN_NAME_4)).isEqualTo(0.0F);
    assertThat(result.contains(ANY_COLUMN_NAME_5)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_5)).isTrue();
    assertThat(result.getDouble(ANY_COLUMN_NAME_5)).isEqualTo(0D);
    assertThat(result.contains(ANY_COLUMN_NAME_6)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_6)).isTrue();
    assertThat(result.getText(ANY_COLUMN_NAME_6)).isNull();
    assertThat(result.contains(ANY_COLUMN_NAME_7)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_7)).isTrue();
    assertThat(result.getBlob(ANY_COLUMN_NAME_7)).isNull();
    assertThat(result.contains(ANY_COLUMN_NAME_8)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_8)).isTrue();
    assertThat(result.getDate(ANY_COLUMN_NAME_8)).isNull();
    assertThat(result.contains(ANY_COLUMN_NAME_9)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_9)).isTrue();
    assertThat(result.getTime(ANY_COLUMN_NAME_9)).isNull();
    assertThat(result.contains(ANY_COLUMN_NAME_10)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_10)).isTrue();
    assertThat(result.getTimestamp(ANY_COLUMN_NAME_10)).isNull();
    assertThat(result.contains(ANY_COLUMN_NAME_11)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_11)).isTrue();
    assertThat(result.getTimestampTZ(ANY_COLUMN_NAME_11)).isNull();

    Map<String, Column<?>> columns = result.getColumns();
    assertThat(columns.containsKey(ANY_NAME_1)).isTrue();
    assertThat(columns.get(ANY_NAME_1).hasNullValue()).isFalse();
    assertThat(columns.get(ANY_NAME_1).getTextValue()).isEqualTo(ANY_TEXT_1);
    assertThat(columns.containsKey(ANY_NAME_2)).isTrue();
    assertThat(columns.get(ANY_NAME_2).hasNullValue()).isFalse();
    assertThat(columns.get(ANY_NAME_2).getTextValue()).isEqualTo(ANY_TEXT_2);
    assertThat(columns.containsKey(ANY_COLUMN_NAME_1)).isTrue();
    assertThat(columns.get(ANY_COLUMN_NAME_1).hasNullValue()).isTrue();
    assertThat(columns.get(ANY_COLUMN_NAME_1).getBooleanValue()).isFalse();
    assertThat(columns.containsKey(ANY_COLUMN_NAME_2)).isTrue();
    assertThat(columns.get(ANY_COLUMN_NAME_2).hasNullValue()).isTrue();
    assertThat(columns.get(ANY_COLUMN_NAME_2).getIntValue()).isEqualTo(0);
    assertThat(columns.containsKey(ANY_COLUMN_NAME_3)).isTrue();
    assertThat(columns.get(ANY_COLUMN_NAME_3).hasNullValue()).isTrue();
    assertThat(columns.get(ANY_COLUMN_NAME_3).getBigIntValue()).isEqualTo(0L);
    assertThat(columns.containsKey(ANY_COLUMN_NAME_4)).isTrue();
    assertThat(columns.get(ANY_COLUMN_NAME_4).hasNullValue()).isTrue();
    assertThat(columns.get(ANY_COLUMN_NAME_4).getFloatValue()).isEqualTo(0.0F);
    assertThat(columns.containsKey(ANY_COLUMN_NAME_5)).isTrue();
    assertThat(columns.get(ANY_COLUMN_NAME_5).hasNullValue()).isTrue();
    assertThat(columns.get(ANY_COLUMN_NAME_5).getDoubleValue()).isEqualTo(0D);
    assertThat(columns.containsKey(ANY_COLUMN_NAME_6)).isTrue();
    assertThat(columns.get(ANY_COLUMN_NAME_6).hasNullValue()).isTrue();
    assertThat(columns.get(ANY_COLUMN_NAME_6).getTextValue()).isNull();
    assertThat(columns.containsKey(ANY_COLUMN_NAME_7)).isTrue();
    assertThat(columns.get(ANY_COLUMN_NAME_7).hasNullValue()).isTrue();
    assertThat(columns.get(ANY_COLUMN_NAME_7).getBlobValueAsBytes()).isNull();
    assertThat(columns.get(ANY_COLUMN_NAME_7).getBlobValueAsByteBuffer()).isNull();
    assertThat(columns.get(ANY_COLUMN_NAME_8).hasNullValue()).isTrue();
    assertThat(columns.get(ANY_COLUMN_NAME_8).getDateValue()).isNull();
    assertThat(columns.containsKey(ANY_COLUMN_NAME_9)).isTrue();
    assertThat(columns.get(ANY_COLUMN_NAME_9).hasNullValue()).isTrue();
    assertThat(columns.get(ANY_COLUMN_NAME_9).getTimeValue()).isNull();
    assertThat(columns.containsKey(ANY_COLUMN_NAME_10)).isTrue();
    assertThat(columns.get(ANY_COLUMN_NAME_10).hasNullValue()).isTrue();
    assertThat(columns.get(ANY_COLUMN_NAME_10).getTimestampValue()).isNull();
    assertThat(columns.containsKey(ANY_COLUMN_NAME_11)).isTrue();
    assertThat(columns.get(ANY_COLUMN_NAME_11).hasNullValue()).isTrue();
    assertThat(columns.get(ANY_COLUMN_NAME_11).getTimestampTZValue()).isNull();
  }

  @ParameterizedTest
  @EnumSource(RdbEngine.class)
  public void interpret_TimeColumnWithSubMicrosecondPrecision_ShouldThrowIllegalArgumentException(
      RdbEngine rdbEngine) throws SQLException {
    // Arrange
    RdbEngineStrategy rdbEngineStrategy = RdbEngine.createRdbEngineStrategy(rdbEngine);

    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(ANY_NAME_1, DataType.TEXT)
            .addColumn(ANY_COLUMN_NAME_1, DataType.TIME)
            .addPartitionKey(ANY_NAME_1)
            .build();

    when(resultSet.getString(ANY_NAME_1)).thenReturn(ANY_TEXT_1);
    when(resultSet.wasNull()).thenReturn(false);

    LocalTime subMicrosecondTime = LocalTime.of(12, 30, 45, 123_456_789);

    // Mock all engine-specific read methods for TIME
    // Default engines use getObject(col, LocalTime.class)
    when(resultSet.getObject(ANY_COLUMN_NAME_1, LocalTime.class)).thenReturn(subMicrosecondTime);
    // SQLite uses getLong(col) with nanoOfDay encoding
    when(resultSet.getLong(ANY_COLUMN_NAME_1)).thenReturn(subMicrosecondTime.toNanoOfDay());
    // DB2 uses getTimestamp(col)
    Timestamp timestamp =
        Timestamp.valueOf(LocalDateTime.of(LocalDate.of(2024, 1, 1), subMicrosecondTime));
    when(resultSet.getTimestamp(ANY_COLUMN_NAME_1)).thenReturn(timestamp);

    ResultInterpreter interpreter =
        new ResultInterpreter(Collections.emptyList(), tableMetadata, rdbEngineStrategy);

    // Act Assert
    assertThatThrownBy(() -> interpreter.interpret(resultSet))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // SQLite is excluded because it uses long encoding (decodeTimestamp/decodeTimestampTZ) that
  // can never produce sub-millisecond values.
  // DB2 is excluded because it uses a string formatter that rejects excess fractional digits
  // with DateTimeParseException (tested separately below).
  @ParameterizedTest
  @EnumSource(
      value = RdbEngine.class,
      names = {"SQLITE", "DB2"},
      mode = EnumSource.Mode.EXCLUDE)
  public void
      interpret_TimestampColumnWithSubMillisecondPrecision_ShouldThrowIllegalArgumentException(
          RdbEngine rdbEngine) throws SQLException {
    // Arrange
    RdbEngineStrategy rdbEngineStrategy = RdbEngine.createRdbEngineStrategy(rdbEngine);

    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(ANY_NAME_1, DataType.TEXT)
            .addColumn(ANY_COLUMN_NAME_1, DataType.TIMESTAMP)
            .addPartitionKey(ANY_NAME_1)
            .build();

    when(resultSet.getString(ANY_NAME_1)).thenReturn(ANY_TEXT_1);
    when(resultSet.wasNull()).thenReturn(false);

    LocalDateTime subMillisecondTimestamp = LocalDateTime.of(2024, 1, 15, 12, 30, 45, 123_456_789);
    when(resultSet.getObject(ANY_COLUMN_NAME_1, LocalDateTime.class))
        .thenReturn(subMillisecondTimestamp);

    ResultInterpreter interpreter =
        new ResultInterpreter(Collections.emptyList(), tableMetadata, rdbEngineStrategy);

    // Act Assert
    assertThatThrownBy(() -> interpreter.interpret(resultSet))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // SQLite is excluded because it uses long encoding (decodeTimestampTZ) that can never produce
  // sub-millisecond values.
  // DB2 is excluded because it uses a string formatter that rejects excess fractional digits
  // with DateTimeParseException (tested separately below).
  @ParameterizedTest
  @EnumSource(
      value = RdbEngine.class,
      names = {"SQLITE", "DB2"},
      mode = EnumSource.Mode.EXCLUDE)
  public void
      interpret_TimestampTZColumnWithSubMillisecondPrecision_ShouldThrowIllegalArgumentException(
          RdbEngine rdbEngine) throws SQLException {
    // Arrange
    RdbEngineStrategy rdbEngineStrategy = RdbEngine.createRdbEngineStrategy(rdbEngine);

    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(ANY_NAME_1, DataType.TEXT)
            .addColumn(ANY_COLUMN_NAME_1, DataType.TIMESTAMPTZ)
            .addPartitionKey(ANY_NAME_1)
            .build();

    when(resultSet.getString(ANY_NAME_1)).thenReturn(ANY_TEXT_1);
    when(resultSet.wasNull()).thenReturn(false);

    LocalDateTime subMillisecondLocalDateTime =
        LocalDateTime.of(2024, 1, 15, 12, 30, 45, 123_456_789);
    OffsetDateTime subMillisecondOffsetDateTime =
        subMillisecondLocalDateTime.atOffset(ZoneOffset.UTC);

    // MySQL/MariaDB read LocalDateTime for TIMESTAMPTZ
    when(resultSet.getObject(ANY_COLUMN_NAME_1, LocalDateTime.class))
        .thenReturn(subMillisecondLocalDateTime);
    // PostgreSQL, Oracle, SQL Server, Yugabyte read OffsetDateTime
    when(resultSet.getObject(ANY_COLUMN_NAME_1, OffsetDateTime.class))
        .thenReturn(subMillisecondOffsetDateTime);

    ResultInterpreter interpreter =
        new ResultInterpreter(Collections.emptyList(), tableMetadata, rdbEngineStrategy);

    // Act Assert
    assertThatThrownBy(() -> interpreter.interpret(resultSet))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      interpret_Db2TimestampColumnWithExcessFractionalDigits_ShouldThrowDateTimeParseException()
          throws SQLException {
    // Arrange
    RdbEngineStrategy rdbEngineStrategy = RdbEngine.createRdbEngineStrategy(RdbEngine.DB2);

    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(ANY_NAME_1, DataType.TEXT)
            .addColumn(ANY_COLUMN_NAME_1, DataType.TIMESTAMP)
            .addPartitionKey(ANY_NAME_1)
            .build();

    when(resultSet.getString(ANY_NAME_1)).thenReturn(ANY_TEXT_1);
    when(resultSet.wasNull()).thenReturn(false);

    // DB2 TIMESTAMP_FORMATTER parses at most 3 fractional digits (milliseconds).
    // A string with microsecond precision (6 digits) causes a DateTimeParseException.
    when(resultSet.getString(ANY_COLUMN_NAME_1)).thenReturn("2024-01-15 12:30:45.123456");

    ResultInterpreter interpreter =
        new ResultInterpreter(Collections.emptyList(), tableMetadata, rdbEngineStrategy);

    // Act Assert
    assertThatThrownBy(() -> interpreter.interpret(resultSet))
        .isInstanceOf(DateTimeParseException.class);
  }

  @Test
  public void
      interpret_Db2TimestampTZColumnWithExcessFractionalDigits_ShouldThrowDateTimeParseException()
          throws SQLException {
    // Arrange
    RdbEngineStrategy rdbEngineStrategy = RdbEngine.createRdbEngineStrategy(RdbEngine.DB2);

    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(ANY_NAME_1, DataType.TEXT)
            .addColumn(ANY_COLUMN_NAME_1, DataType.TIMESTAMPTZ)
            .addPartitionKey(ANY_NAME_1)
            .build();

    when(resultSet.getString(ANY_NAME_1)).thenReturn(ANY_TEXT_1);
    when(resultSet.wasNull()).thenReturn(false);

    // DB2 TIMESTAMP_FORMATTER parses at most 3 fractional digits (milliseconds).
    // A string with microsecond precision (6 digits) causes a DateTimeParseException.
    when(resultSet.getString(ANY_COLUMN_NAME_1)).thenReturn("2024-01-15 12:30:45.123456");

    ResultInterpreter interpreter =
        new ResultInterpreter(Collections.emptyList(), tableMetadata, rdbEngineStrategy);

    // Act Assert
    assertThatThrownBy(() -> interpreter.interpret(resultSet))
        .isInstanceOf(DateTimeParseException.class);
  }
}
