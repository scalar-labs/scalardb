package com.scalar.db.storage.cassandra;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Row;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampTZColumn;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
          .addColumn(ANY_COLUMN_NAME_10, DataType.TIMESTAMPTZ)
          .addPartitionKey(ANY_NAME_1)
          .addClusteringKey(ANY_NAME_2)
          .build();

  private static final LocalDate ANY_DATE = DateColumn.MAX_VALUE;
  private static final LocalTime ANY_TIME = TimeColumn.MAX_VALUE;
  private static final Instant ANY_TIMESTAMPTZ = TimestampTZColumn.MAX_VALUE;

  @Mock private Row row;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  @Test
  public void interpret_ShouldReturnWhatsSet() {
    // Arrange
    when(row.getString(ANY_NAME_1)).thenReturn(ANY_TEXT_1);
    when(row.getString(ANY_NAME_2)).thenReturn(ANY_TEXT_2);
    when(row.getBool(ANY_COLUMN_NAME_1)).thenReturn(true);
    when(row.getInt(ANY_COLUMN_NAME_2)).thenReturn(Integer.MAX_VALUE);
    when(row.getLong(ANY_COLUMN_NAME_3)).thenReturn(BigIntValue.MAX_VALUE);
    when(row.getFloat(ANY_COLUMN_NAME_4)).thenReturn(Float.MAX_VALUE);
    when(row.getDouble(ANY_COLUMN_NAME_5)).thenReturn(Double.MAX_VALUE);
    when(row.getString(ANY_COLUMN_NAME_6)).thenReturn("string");
    byte[] bytesValue = "bytes".getBytes(StandardCharsets.UTF_8);
    when(row.getBytes(ANY_COLUMN_NAME_7))
        .thenReturn((ByteBuffer) ByteBuffer.allocate(bytesValue.length).put(bytesValue).flip());
    when(row.getDate(ANY_COLUMN_NAME_8))
        .thenReturn(
            com.datastax.driver.core.LocalDate.fromDaysSinceEpoch((int) ANY_DATE.toEpochDay()));
    when(row.getTime(ANY_COLUMN_NAME_9)).thenReturn(ANY_TIME.toNanoOfDay());
    when(row.getTimestamp(ANY_COLUMN_NAME_10)).thenReturn(java.util.Date.from(ANY_TIMESTAMPTZ));

    List<String> projections = Collections.emptyList();

    ResultInterpreter interpreter = new ResultInterpreter(projections, TABLE_METADATA);

    // Act
    Result result = interpreter.interpret(row);

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
    assertThat(result.getBigInt(ANY_COLUMN_NAME_3)).isEqualTo(BigIntColumn.MAX_VALUE);
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
    assertThat(result.getTimestampTZ(ANY_COLUMN_NAME_10)).isEqualTo(ANY_TIMESTAMPTZ);

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
    assertThat(columns.get(ANY_COLUMN_NAME_3).getBigIntValue()).isEqualTo(BigIntColumn.MAX_VALUE);
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
    assertThat(columns.get(ANY_COLUMN_NAME_10).getTimestampTZValue()).isEqualTo(ANY_TIMESTAMPTZ);
  }

  @Test
  public void interpret_ShouldReturnWhatsSetWithNullValues() {
    // Arrange
    when(row.getString(ANY_NAME_1)).thenReturn(ANY_TEXT_1);
    when(row.getString(ANY_NAME_2)).thenReturn(ANY_TEXT_2);
    when(row.isNull(ANY_COLUMN_NAME_1)).thenReturn(true);
    when(row.getBool(ANY_COLUMN_NAME_1)).thenReturn(false);
    when(row.isNull(ANY_COLUMN_NAME_2)).thenReturn(true);
    when(row.getInt(ANY_COLUMN_NAME_2)).thenReturn(0);
    when(row.isNull(ANY_COLUMN_NAME_3)).thenReturn(true);
    when(row.getLong(ANY_COLUMN_NAME_3)).thenReturn(0L);
    when(row.isNull(ANY_COLUMN_NAME_4)).thenReturn(true);
    when(row.getFloat(ANY_COLUMN_NAME_4)).thenReturn(0.0F);
    when(row.isNull(ANY_COLUMN_NAME_5)).thenReturn(true);
    when(row.getDouble(ANY_COLUMN_NAME_5)).thenReturn(0.0D);
    when(row.isNull(ANY_COLUMN_NAME_6)).thenReturn(true);
    when(row.getString(ANY_COLUMN_NAME_6)).thenReturn(null);
    when(row.isNull(ANY_COLUMN_NAME_7)).thenReturn(true);
    when(row.getBytes(ANY_COLUMN_NAME_7)).thenReturn(null);
    when(row.isNull(ANY_COLUMN_NAME_8)).thenReturn(true);
    when(row.getDate(ANY_COLUMN_NAME_8)).thenReturn(null);
    when(row.isNull(ANY_COLUMN_NAME_9)).thenReturn(true);
    when(row.getTime(ANY_COLUMN_NAME_9)).thenReturn(0L);
    when(row.isNull(ANY_COLUMN_NAME_10)).thenReturn(true);
    when(row.getTimestamp(ANY_COLUMN_NAME_10)).thenReturn(null);
    List<String> projections = Collections.emptyList();

    ResultInterpreter interpreter = new ResultInterpreter(projections, TABLE_METADATA);

    // Act
    Result result = interpreter.interpret(row);

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
    assertThat(result.getDouble(ANY_COLUMN_NAME_5)).isEqualTo(0.0D);
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
    assertThat(result.getTimestampTZ(ANY_COLUMN_NAME_10)).isNull();

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
    assertThat(columns.get(ANY_COLUMN_NAME_5).getDoubleValue()).isEqualTo(0.0D);
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
    assertThat(columns.get(ANY_COLUMN_NAME_10).getTimestampTZValue()).isNull();
  }

  @Test
  public void interpret_TimestampType_ShouldThrowUnsupportedOperationException() {
    // Arrange
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(ANY_NAME_1, DataType.TEXT)
            .addColumn(ANY_NAME_2, DataType.TEXT)
            .addColumn(ANY_COLUMN_NAME_1, DataType.TIMESTAMP)
            .addPartitionKey(ANY_NAME_1)
            .addClusteringKey(ANY_NAME_2)
            .build();

    when(row.getString(ANY_NAME_1)).thenReturn(ANY_TEXT_1);
    when(row.getString(ANY_NAME_2)).thenReturn(ANY_TEXT_2);
    when(row.getTimestamp(ANY_COLUMN_NAME_1)).thenReturn(Date.from(TimestampTZColumn.MAX_VALUE));

    ResultInterpreter interpreter = new ResultInterpreter(Collections.emptyList(), tableMetadata);

    // Act
    assertThatThrownBy(() -> interpreter.interpret(row))
        .isInstanceOf(UnsupportedOperationException.class);
  }
}
