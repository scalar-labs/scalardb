package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ResultImplTest {

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
  private static final LocalDate ANY_DATE = DateColumn.MAX_VALUE;
  private static final LocalTime ANY_TIME = TimeColumn.MAX_VALUE;
  private static final LocalDateTime ANY_TIMESTAMP = TimestampColumn.MAX_VALUE;
  private static final Instant ANY_TIMESTAMPTZ = TimestampTZColumn.MAX_VALUE;
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

  private Map<String, Column<?>> columns;

  @BeforeEach
  public void setUp() {
    columns =
        ImmutableMap.<String, Column<?>>builder()
            .put(ANY_NAME_1, TextColumn.of(ANY_NAME_1, ANY_TEXT_1))
            .put(ANY_NAME_2, TextColumn.of(ANY_NAME_2, ANY_TEXT_2))
            .put(ANY_COLUMN_NAME_1, BooleanColumn.of(ANY_COLUMN_NAME_1, true))
            .put(ANY_COLUMN_NAME_2, IntColumn.of(ANY_COLUMN_NAME_2, Integer.MAX_VALUE))
            .put(ANY_COLUMN_NAME_3, BigIntColumn.of(ANY_COLUMN_NAME_3, BigIntColumn.MAX_VALUE))
            .put(ANY_COLUMN_NAME_4, FloatColumn.of(ANY_COLUMN_NAME_4, Float.MAX_VALUE))
            .put(ANY_COLUMN_NAME_5, DoubleColumn.of(ANY_COLUMN_NAME_5, Double.MAX_VALUE))
            .put(ANY_COLUMN_NAME_6, TextColumn.of(ANY_COLUMN_NAME_6, "string"))
            .put(
                ANY_COLUMN_NAME_7,
                BlobColumn.of(ANY_COLUMN_NAME_7, "bytes".getBytes(StandardCharsets.UTF_8)))
            .put(ANY_COLUMN_NAME_8, DateColumn.of(ANY_COLUMN_NAME_8, ANY_DATE))
            .put(ANY_COLUMN_NAME_9, TimeColumn.of(ANY_COLUMN_NAME_9, ANY_TIME))
            .put(ANY_COLUMN_NAME_10, TimestampColumn.of(ANY_COLUMN_NAME_10, ANY_TIMESTAMP))
            .put(ANY_COLUMN_NAME_11, TimestampTZColumn.of(ANY_COLUMN_NAME_11, ANY_TIMESTAMPTZ))
            .build();
  }

  @Test
  public void getValue_ProperValuesGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    ResultImpl result = new ResultImpl(columns, TABLE_METADATA);

    // Act Assert
    assertThat(result.getValue(ANY_NAME_1))
        .isEqualTo(Optional.of(new TextValue(ANY_NAME_1, ANY_TEXT_1)));
    assertThat(result.getValue(ANY_NAME_2))
        .isEqualTo(Optional.of(new TextValue(ANY_NAME_2, ANY_TEXT_2)));
    assertThat(result.getValue(ANY_COLUMN_NAME_1))
        .isEqualTo(Optional.of(new BooleanValue(ANY_COLUMN_NAME_1, true)));
    assertThat(result.getValue(ANY_COLUMN_NAME_2))
        .isEqualTo(Optional.of(new IntValue(ANY_COLUMN_NAME_2, Integer.MAX_VALUE)));
    assertThat(result.getValue(ANY_COLUMN_NAME_3))
        .isEqualTo(Optional.of(new BigIntValue(ANY_COLUMN_NAME_3, BigIntValue.MAX_VALUE)));
    assertThat(result.getValue(ANY_COLUMN_NAME_4))
        .isEqualTo(Optional.of(new FloatValue(ANY_COLUMN_NAME_4, Float.MAX_VALUE)));
    assertThat(result.getValue(ANY_COLUMN_NAME_5))
        .isEqualTo(Optional.of(new DoubleValue(ANY_COLUMN_NAME_5, Double.MAX_VALUE)));
    assertThat(result.getValue(ANY_COLUMN_NAME_6))
        .isEqualTo(Optional.of(new TextValue(ANY_COLUMN_NAME_6, "string")));
    assertThat(result.getValue(ANY_COLUMN_NAME_7))
        .isEqualTo(
            Optional.of(
                new BlobValue(ANY_COLUMN_NAME_7, "bytes".getBytes(StandardCharsets.UTF_8))));

    assertThat(result.getContainedColumnNames())
        .isEqualTo(
            new HashSet<>(
                Arrays.asList(
                    ANY_NAME_1,
                    ANY_NAME_2,
                    ANY_COLUMN_NAME_1,
                    ANY_COLUMN_NAME_2,
                    ANY_COLUMN_NAME_3,
                    ANY_COLUMN_NAME_4,
                    ANY_COLUMN_NAME_5,
                    ANY_COLUMN_NAME_6,
                    ANY_COLUMN_NAME_7,
                    ANY_COLUMN_NAME_8,
                    ANY_COLUMN_NAME_9,
                    ANY_COLUMN_NAME_10,
                    ANY_COLUMN_NAME_11)));

    assertThat(result.contains(ANY_NAME_1)).isTrue();
    assertThat(result.isNull(ANY_NAME_1)).isFalse();
    assertThat(result.getText(ANY_NAME_1)).isEqualTo(ANY_TEXT_1);
    assertThat(result.getAsObject(ANY_NAME_1)).isEqualTo(ANY_TEXT_1);

    assertThat(result.contains(ANY_NAME_2)).isTrue();
    assertThat(result.isNull(ANY_NAME_2)).isFalse();
    assertThat(result.getText(ANY_NAME_2)).isEqualTo(ANY_TEXT_2);
    assertThat(result.getAsObject(ANY_NAME_2)).isEqualTo(ANY_TEXT_2);

    assertThat(result.contains(ANY_COLUMN_NAME_1)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_1)).isFalse();
    assertThat(result.getBoolean(ANY_COLUMN_NAME_1)).isEqualTo(true);
    assertThat(result.getAsObject(ANY_COLUMN_NAME_1)).isEqualTo(true);

    assertThat(result.contains(ANY_COLUMN_NAME_2)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_2)).isFalse();
    assertThat(result.getInt(ANY_COLUMN_NAME_2)).isEqualTo(Integer.MAX_VALUE);
    assertThat(result.getAsObject(ANY_COLUMN_NAME_2)).isEqualTo(Integer.MAX_VALUE);

    assertThat(result.contains(ANY_COLUMN_NAME_3)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_3)).isFalse();
    assertThat(result.getBigInt(ANY_COLUMN_NAME_3)).isEqualTo(BigIntValue.MAX_VALUE);
    assertThat(result.getAsObject(ANY_COLUMN_NAME_3)).isEqualTo(BigIntValue.MAX_VALUE);

    assertThat(result.contains(ANY_COLUMN_NAME_4)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_4)).isFalse();
    assertThat(result.getFloat(ANY_COLUMN_NAME_4)).isEqualTo(Float.MAX_VALUE);
    assertThat(result.getAsObject(ANY_COLUMN_NAME_4)).isEqualTo(Float.MAX_VALUE);

    assertThat(result.contains(ANY_COLUMN_NAME_5)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_5)).isFalse();
    assertThat(result.getDouble(ANY_COLUMN_NAME_5)).isEqualTo(Double.MAX_VALUE);
    assertThat(result.getAsObject(ANY_COLUMN_NAME_5)).isEqualTo(Double.MAX_VALUE);

    assertThat(result.contains(ANY_COLUMN_NAME_6)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_6)).isFalse();
    assertThat(result.getText(ANY_COLUMN_NAME_6)).isEqualTo("string");
    assertThat(result.getAsObject(ANY_COLUMN_NAME_6)).isEqualTo("string");

    assertThat(result.contains(ANY_COLUMN_NAME_7)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_7)).isFalse();
    assertThat(result.getBlob(ANY_COLUMN_NAME_7))
        .isEqualTo(ByteBuffer.wrap("bytes".getBytes(StandardCharsets.UTF_8)));
    assertThat(result.getBlobAsByteBuffer(ANY_COLUMN_NAME_7))
        .isEqualTo(ByteBuffer.wrap("bytes".getBytes(StandardCharsets.UTF_8)));
    assertThat(result.getBlobAsBytes(ANY_COLUMN_NAME_7))
        .isEqualTo("bytes".getBytes(StandardCharsets.UTF_8));
    assertThat(result.getAsObject(ANY_COLUMN_NAME_7))
        .isEqualTo(ByteBuffer.wrap("bytes".getBytes(StandardCharsets.UTF_8)));

    assertThat(result.contains(ANY_COLUMN_NAME_8)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_8)).isFalse();
    assertThat(result.getDate(ANY_COLUMN_NAME_8)).isEqualTo(ANY_DATE);
    assertThat(result.getAsObject(ANY_COLUMN_NAME_8)).isEqualTo(ANY_DATE);

    assertThat(result.contains(ANY_COLUMN_NAME_9)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_9)).isFalse();
    assertThat(result.getTime(ANY_COLUMN_NAME_9)).isEqualTo(ANY_TIME);
    assertThat(result.getAsObject(ANY_COLUMN_NAME_9)).isEqualTo(ANY_TIME);

    assertThat(result.contains(ANY_COLUMN_NAME_10)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_10)).isFalse();
    assertThat(result.getTimestamp(ANY_COLUMN_NAME_10)).isEqualTo(ANY_TIMESTAMP);
    assertThat(result.getAsObject(ANY_COLUMN_NAME_10)).isEqualTo(ANY_TIMESTAMP);

    assertThat(result.contains(ANY_COLUMN_NAME_11)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_11)).isFalse();
    assertThat(result.getTimestampTZ(ANY_COLUMN_NAME_11)).isEqualTo(ANY_TIMESTAMPTZ);
    assertThat(result.getAsObject(ANY_COLUMN_NAME_11)).isEqualTo(ANY_TIMESTAMPTZ);
  }

  @Test
  public void getValue_ProperNullValuesGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    ResultImpl result =
        new ResultImpl(
            ImmutableMap.<String, Column<?>>builder()
                .put(ANY_NAME_1, TextColumn.of(ANY_NAME_1, ANY_TEXT_1))
                .put(ANY_NAME_2, TextColumn.of(ANY_NAME_2, ANY_TEXT_2))
                .put(ANY_COLUMN_NAME_1, BooleanColumn.ofNull(ANY_COLUMN_NAME_1))
                .put(ANY_COLUMN_NAME_2, IntColumn.ofNull(ANY_COLUMN_NAME_2))
                .put(ANY_COLUMN_NAME_3, BigIntColumn.ofNull(ANY_COLUMN_NAME_3))
                .put(ANY_COLUMN_NAME_4, FloatColumn.ofNull(ANY_COLUMN_NAME_4))
                .put(ANY_COLUMN_NAME_5, DoubleColumn.ofNull(ANY_COLUMN_NAME_5))
                .put(ANY_COLUMN_NAME_6, TextColumn.ofNull(ANY_COLUMN_NAME_6))
                .put(ANY_COLUMN_NAME_7, BlobColumn.ofNull(ANY_COLUMN_NAME_7))
                .build(),
            TABLE_METADATA);

    // Act Assert
    assertThat(result.getValue(ANY_NAME_1))
        .isEqualTo(Optional.of(new TextValue(ANY_NAME_1, ANY_TEXT_1)));
    assertThat(result.getValue(ANY_NAME_2))
        .isEqualTo(Optional.of(new TextValue(ANY_NAME_2, ANY_TEXT_2)));
    assertThat(result.getValue(ANY_COLUMN_NAME_1))
        .isEqualTo(Optional.of(new BooleanValue(ANY_COLUMN_NAME_1, false)));
    assertThat(result.getValue(ANY_COLUMN_NAME_2))
        .isEqualTo(Optional.of(new IntValue(ANY_COLUMN_NAME_2, 0)));
    assertThat(result.getValue(ANY_COLUMN_NAME_3))
        .isEqualTo(Optional.of(new BigIntValue(ANY_COLUMN_NAME_3, 0L)));
    assertThat(result.getValue(ANY_COLUMN_NAME_4))
        .isEqualTo(Optional.of(new FloatValue(ANY_COLUMN_NAME_4, 0.0F)));
    assertThat(result.getValue(ANY_COLUMN_NAME_5))
        .isEqualTo(Optional.of(new DoubleValue(ANY_COLUMN_NAME_5, 0.0D)));
    assertThat(result.getValue(ANY_COLUMN_NAME_6))
        .isEqualTo(Optional.of(new TextValue(ANY_COLUMN_NAME_6, (String) null)));
    assertThat(result.getValue(ANY_COLUMN_NAME_7))
        .isEqualTo(Optional.of(new BlobValue(ANY_COLUMN_NAME_7, (byte[]) null)));

    assertThat(result.getContainedColumnNames())
        .isEqualTo(
            new HashSet<>(
                Arrays.asList(
                    ANY_NAME_1,
                    ANY_NAME_2,
                    ANY_COLUMN_NAME_1,
                    ANY_COLUMN_NAME_2,
                    ANY_COLUMN_NAME_3,
                    ANY_COLUMN_NAME_4,
                    ANY_COLUMN_NAME_5,
                    ANY_COLUMN_NAME_6,
                    ANY_COLUMN_NAME_7)));

    assertThat(result.contains(ANY_NAME_1)).isTrue();
    assertThat(result.isNull(ANY_NAME_1)).isFalse();
    assertThat(result.getText(ANY_NAME_1)).isEqualTo(ANY_TEXT_1);
    assertThat(result.getAsObject(ANY_NAME_1)).isEqualTo(ANY_TEXT_1);

    assertThat(result.contains(ANY_NAME_2)).isTrue();
    assertThat(result.isNull(ANY_NAME_2)).isFalse();
    assertThat(result.getText(ANY_NAME_2)).isEqualTo(ANY_TEXT_2);
    assertThat(result.getAsObject(ANY_NAME_2)).isEqualTo(ANY_TEXT_2);

    assertThat(result.contains(ANY_COLUMN_NAME_1)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_1)).isTrue();
    assertThat(result.getBoolean(ANY_COLUMN_NAME_1)).isEqualTo(false);
    assertThat(result.getAsObject(ANY_COLUMN_NAME_1)).isNull();

    assertThat(result.contains(ANY_COLUMN_NAME_2)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_2)).isTrue();
    assertThat(result.getInt(ANY_COLUMN_NAME_2)).isEqualTo(0);
    assertThat(result.getAsObject(ANY_COLUMN_NAME_2)).isNull();

    assertThat(result.contains(ANY_COLUMN_NAME_3)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_3)).isTrue();
    assertThat(result.getBigInt(ANY_COLUMN_NAME_3)).isEqualTo(0L);
    assertThat(result.getAsObject(ANY_COLUMN_NAME_3)).isNull();

    assertThat(result.contains(ANY_COLUMN_NAME_4)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_4)).isTrue();
    assertThat(result.getFloat(ANY_COLUMN_NAME_4)).isEqualTo(0.0F);
    assertThat(result.getAsObject(ANY_COLUMN_NAME_4)).isNull();

    assertThat(result.contains(ANY_COLUMN_NAME_5)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_5)).isTrue();
    assertThat(result.getDouble(ANY_COLUMN_NAME_5)).isEqualTo(0.0D);
    assertThat(result.getAsObject(ANY_COLUMN_NAME_5)).isNull();

    assertThat(result.contains(ANY_COLUMN_NAME_6)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_6)).isTrue();
    assertThat(result.getText(ANY_COLUMN_NAME_6)).isNull();
    assertThat(result.getAsObject(ANY_COLUMN_NAME_6)).isNull();

    assertThat(result.contains(ANY_COLUMN_NAME_7)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_7)).isTrue();
    assertThat(result.getBlob(ANY_COLUMN_NAME_7)).isNull();
    assertThat(result.getBlobAsByteBuffer(ANY_COLUMN_NAME_7)).isNull();
    assertThat(result.getBlobAsBytes(ANY_COLUMN_NAME_7)).isNull();
    assertThat(result.getAsObject(ANY_COLUMN_NAME_7)).isNull();
  }

  @Test
  public void
      getValue_ProperValuesWithNullTextValueAndNullBlobValueGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    ResultImpl result =
        new ResultImpl(
            ImmutableMap.<String, Column<?>>builder()
                .put(ANY_NAME_1, TextColumn.of(ANY_NAME_1, ANY_TEXT_1))
                .put(ANY_NAME_2, TextColumn.of(ANY_NAME_2, ANY_TEXT_2))
                .put(ANY_COLUMN_NAME_1, BooleanColumn.of(ANY_COLUMN_NAME_1, true))
                .put(ANY_COLUMN_NAME_2, IntColumn.of(ANY_COLUMN_NAME_2, Integer.MAX_VALUE))
                .put(ANY_COLUMN_NAME_3, BigIntColumn.of(ANY_COLUMN_NAME_3, BigIntColumn.MAX_VALUE))
                .put(ANY_COLUMN_NAME_4, FloatColumn.of(ANY_COLUMN_NAME_4, Float.MAX_VALUE))
                .put(ANY_COLUMN_NAME_5, DoubleColumn.of(ANY_COLUMN_NAME_5, Double.MAX_VALUE))
                .put(ANY_COLUMN_NAME_6, TextColumn.of(ANY_COLUMN_NAME_6, null))
                .put(ANY_COLUMN_NAME_7, BlobColumn.of(ANY_COLUMN_NAME_7, (byte[]) null))
                .build(),
            TABLE_METADATA);

    // Act Assert
    assertThat(result.getValue(ANY_NAME_1))
        .isEqualTo(Optional.of(new TextValue(ANY_NAME_1, ANY_TEXT_1)));
    assertThat(result.getValue(ANY_NAME_2))
        .isEqualTo(Optional.of(new TextValue(ANY_NAME_2, ANY_TEXT_2)));
    assertThat(result.getValue(ANY_COLUMN_NAME_1))
        .isEqualTo(Optional.of(new BooleanValue(ANY_COLUMN_NAME_1, true)));
    assertThat(result.getValue(ANY_COLUMN_NAME_2))
        .isEqualTo(Optional.of(new IntValue(ANY_COLUMN_NAME_2, Integer.MAX_VALUE)));
    assertThat(result.getValue(ANY_COLUMN_NAME_3))
        .isEqualTo(Optional.of(new BigIntValue(ANY_COLUMN_NAME_3, BigIntValue.MAX_VALUE)));
    assertThat(result.getValue(ANY_COLUMN_NAME_4))
        .isEqualTo(Optional.of(new FloatValue(ANY_COLUMN_NAME_4, Float.MAX_VALUE)));
    assertThat(result.getValue(ANY_COLUMN_NAME_5))
        .isEqualTo(Optional.of(new DoubleValue(ANY_COLUMN_NAME_5, Double.MAX_VALUE)));
    assertThat(result.getValue(ANY_COLUMN_NAME_6))
        .isEqualTo(Optional.of(new TextValue(ANY_COLUMN_NAME_6, (String) null)));
    assertThat(result.getValue(ANY_COLUMN_NAME_7))
        .isEqualTo(Optional.of(new BlobValue(ANY_COLUMN_NAME_7, (byte[]) null)));

    assertThat(result.getContainedColumnNames())
        .isEqualTo(
            new HashSet<>(
                Arrays.asList(
                    ANY_NAME_1,
                    ANY_NAME_2,
                    ANY_COLUMN_NAME_1,
                    ANY_COLUMN_NAME_2,
                    ANY_COLUMN_NAME_3,
                    ANY_COLUMN_NAME_4,
                    ANY_COLUMN_NAME_5,
                    ANY_COLUMN_NAME_6,
                    ANY_COLUMN_NAME_7)));

    assertThat(result.contains(ANY_NAME_1)).isTrue();
    assertThat(result.isNull(ANY_NAME_1)).isFalse();
    assertThat(result.getText(ANY_NAME_1)).isEqualTo(ANY_TEXT_1);
    assertThat(result.getAsObject(ANY_NAME_1)).isEqualTo(ANY_TEXT_1);

    assertThat(result.contains(ANY_NAME_2)).isTrue();
    assertThat(result.isNull(ANY_NAME_2)).isFalse();
    assertThat(result.getText(ANY_NAME_2)).isEqualTo(ANY_TEXT_2);
    assertThat(result.getAsObject(ANY_NAME_2)).isEqualTo(ANY_TEXT_2);

    assertThat(result.contains(ANY_COLUMN_NAME_1)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_1)).isFalse();
    assertThat(result.getBoolean(ANY_COLUMN_NAME_1)).isEqualTo(true);
    assertThat(result.getAsObject(ANY_COLUMN_NAME_1)).isEqualTo(true);

    assertThat(result.contains(ANY_COLUMN_NAME_2)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_2)).isFalse();
    assertThat(result.getInt(ANY_COLUMN_NAME_2)).isEqualTo(Integer.MAX_VALUE);
    assertThat(result.getAsObject(ANY_COLUMN_NAME_2)).isEqualTo(Integer.MAX_VALUE);

    assertThat(result.contains(ANY_COLUMN_NAME_3)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_3)).isFalse();
    assertThat(result.getBigInt(ANY_COLUMN_NAME_3)).isEqualTo(BigIntValue.MAX_VALUE);
    assertThat(result.getAsObject(ANY_COLUMN_NAME_3)).isEqualTo(BigIntValue.MAX_VALUE);

    assertThat(result.contains(ANY_COLUMN_NAME_4)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_4)).isFalse();
    assertThat(result.getFloat(ANY_COLUMN_NAME_4)).isEqualTo(Float.MAX_VALUE);
    assertThat(result.getAsObject(ANY_COLUMN_NAME_4)).isEqualTo(Float.MAX_VALUE);

    assertThat(result.contains(ANY_COLUMN_NAME_5)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_5)).isFalse();
    assertThat(result.getDouble(ANY_COLUMN_NAME_5)).isEqualTo(Double.MAX_VALUE);
    assertThat(result.getAsObject(ANY_COLUMN_NAME_5)).isEqualTo(Double.MAX_VALUE);

    assertThat(result.contains(ANY_COLUMN_NAME_6)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_6)).isTrue();
    assertThat(result.getText(ANY_COLUMN_NAME_6)).isNull();
    assertThat(result.getAsObject(ANY_COLUMN_NAME_6)).isNull();

    assertThat(result.contains(ANY_COLUMN_NAME_7)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_7)).isTrue();
    assertThat(result.getBlob(ANY_COLUMN_NAME_7)).isNull();
    assertThat(result.getBlobAsByteBuffer(ANY_COLUMN_NAME_7)).isNull();
    assertThat(result.getBlobAsBytes(ANY_COLUMN_NAME_7)).isNull();
    assertThat(result.getAsObject(ANY_COLUMN_NAME_7)).isNull();
  }

  @Test
  public void getValues_ProperValuesGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    ResultImpl result = new ResultImpl(columns, TABLE_METADATA);

    // Act
    Map<String, Column<?>> actual = result.getColumns();

    // Assert
    assertThat(actual.get(ANY_NAME_1)).isEqualTo(TextColumn.of(ANY_NAME_1, ANY_TEXT_1));
    assertThat(actual.get(ANY_NAME_2)).isEqualTo(TextColumn.of(ANY_NAME_2, ANY_TEXT_2));
    assertThat(actual.get(ANY_COLUMN_NAME_1)).isEqualTo(BooleanColumn.of(ANY_COLUMN_NAME_1, true));
    assertThat(actual.get(ANY_COLUMN_NAME_7))
        .isEqualTo(BlobColumn.of(ANY_COLUMN_NAME_7, "bytes".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  public void getValues_NoValuesGivenInConstructor_ShouldReturnEmptyValues() {
    // Arrange
    Map<String, Column<?>> emptyValues = Collections.emptyMap();
    ResultImpl result = new ResultImpl(emptyValues, TABLE_METADATA);

    // Act
    Map<String, Column<?>> actual = result.getColumns();

    // Assert
    assertThat(actual.isEmpty()).isTrue();
  }

  @Test
  public void getValues_TryToModifyReturned_ShouldThrowException() {
    // Arrange
    ResultImpl result = new ResultImpl(columns, TABLE_METADATA);
    Map<String, Column<?>> values = result.getColumns();

    // Act Assert
    assertThatThrownBy(() -> values.put("new", TextColumn.of(ANY_NAME_1, ANY_TEXT_1)))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void getColumns_ProperValuesGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    ResultImpl result = new ResultImpl(columns, TABLE_METADATA);

    // Act
    Map<String, Column<?>> columns = result.getColumns();

    // Assert
    assertThat(columns.size()).isEqualTo(13);
    assertThat(columns.get(ANY_NAME_1).hasNullValue()).isFalse();
    assertThat(columns.get(ANY_NAME_1).getTextValue()).isEqualTo(ANY_TEXT_1);
    assertThat(columns.get(ANY_NAME_2).hasNullValue()).isFalse();
    assertThat(columns.get(ANY_NAME_2).getTextValue()).isEqualTo(ANY_TEXT_2);
    assertThat(columns.get(ANY_COLUMN_NAME_1).hasNullValue()).isFalse();
    assertThat(columns.get(ANY_COLUMN_NAME_1).getBooleanValue()).isEqualTo(true);
    assertThat(columns.get(ANY_COLUMN_NAME_2).hasNullValue()).isFalse();
    assertThat(columns.get(ANY_COLUMN_NAME_2).getIntValue()).isEqualTo(Integer.MAX_VALUE);
    assertThat(columns.get(ANY_COLUMN_NAME_3).hasNullValue()).isFalse();
    assertThat(columns.get(ANY_COLUMN_NAME_3).getBigIntValue()).isEqualTo(BigIntColumn.MAX_VALUE);
    assertThat(columns.get(ANY_COLUMN_NAME_4).hasNullValue()).isFalse();
    assertThat(columns.get(ANY_COLUMN_NAME_4).getFloatValue()).isEqualTo(Float.MAX_VALUE);
    assertThat(columns.get(ANY_COLUMN_NAME_5).hasNullValue()).isFalse();
    assertThat(columns.get(ANY_COLUMN_NAME_5).getDoubleValue()).isEqualTo(Double.MAX_VALUE);
    assertThat(columns.get(ANY_COLUMN_NAME_6).hasNullValue()).isFalse();
    assertThat(columns.get(ANY_COLUMN_NAME_6).getTextValue()).isEqualTo("string");
    assertThat(columns.get(ANY_COLUMN_NAME_7).hasNullValue()).isFalse();
    assertThat(columns.get(ANY_COLUMN_NAME_7).getBlobValue())
        .isEqualTo(ByteBuffer.wrap("bytes".getBytes(StandardCharsets.UTF_8)));
    assertThat(columns.get(ANY_COLUMN_NAME_8).hasNullValue()).isFalse();
    assertThat(columns.get(ANY_COLUMN_NAME_8).getDateValue()).isEqualTo(ANY_DATE);
    assertThat(columns.get(ANY_COLUMN_NAME_9).hasNullValue()).isFalse();
    assertThat(columns.get(ANY_COLUMN_NAME_9).getTimeValue()).isEqualTo(ANY_TIME);
    assertThat(columns.get(ANY_COLUMN_NAME_10).hasNullValue()).isFalse();
    assertThat(columns.get(ANY_COLUMN_NAME_10).getTimestampValue()).isEqualTo(ANY_TIMESTAMP);
    assertThat(columns.get(ANY_COLUMN_NAME_11).hasNullValue()).isFalse();
    assertThat(columns.get(ANY_COLUMN_NAME_11).getTimestampTZValue()).isEqualTo(ANY_TIMESTAMPTZ);
  }

  @Test
  public void equals_DifferentObjectsSameValuesGiven_ShouldReturnTrue() {
    // Arrange
    ResultImpl r1 = new ResultImpl(columns, TABLE_METADATA);
    ResultImpl r2 = new ResultImpl(columns, TABLE_METADATA);

    // Act Assert
    assertThat(r1.equals(r2)).isTrue();
  }

  @Test
  public void equals_DifferentObjectsDifferentValuesGiven_ShouldReturnFalse() {
    // Arrange
    ResultImpl r1 = new ResultImpl(columns, TABLE_METADATA);
    Map<String, Column<?>> emptyValues = Collections.emptyMap();
    ResultImpl r2 = new ResultImpl(emptyValues, TABLE_METADATA);

    // Act Assert
    assertThat(r1.equals(r2)).isFalse();
  }

  @Test
  public void constructor_NullGiven_ShouldThrowNullPointerException() {
    // Act Assert
    assertThatThrownBy(() -> new ResultImpl(null, null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void getPartitionKey_RequiredValuesGiven_ShouldReturnPartitionKey() {
    // Arrange
    ResultImpl result = new ResultImpl(columns, TABLE_METADATA);

    // Act
    Optional<Key> key = result.getPartitionKey();

    // Assert
    assertThat(key.isPresent()).isTrue();
    assertThat(key.get().get().size()).isEqualTo(1);
    assertThat(key.get().get().get(0)).isEqualTo(new TextValue(ANY_NAME_1, ANY_TEXT_1));
  }

  @Test
  public void getPartitionKey_NotRequiredValuesGiven_ShouldThrowIllegalStateException() {
    // Arrange
    ResultImpl result =
        new ResultImpl(
            ImmutableMap.<String, Column<?>>builder()
                .put(ANY_NAME_2, TextColumn.of(ANY_NAME_2, ANY_TEXT_2))
                .build(),
            TABLE_METADATA);

    // Act
    Throwable thrown = catchThrowable(result::getPartitionKey);

    // Assert
    assertThat(thrown).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void getClusteringKey_RequiredValuesGiven_ShouldReturnClusteringKey() {
    // Arrange
    ResultImpl result = new ResultImpl(columns, TABLE_METADATA);

    // Act
    Optional<Key> key = result.getClusteringKey();

    // Assert
    assertThat(key.isPresent()).isTrue();
    assertThat(key.get().get().size()).isEqualTo(1);
    assertThat(key.get().get().get(0)).isEqualTo(new TextValue(ANY_NAME_2, ANY_TEXT_2));
  }

  @Test
  public void getClusteringKey_NotRequiredValuesGiven_ShouldThrowIllegalStateException() {
    // Arrange
    ResultImpl result =
        new ResultImpl(
            ImmutableMap.<String, Column<?>>builder()
                .put(ANY_NAME_1, TextColumn.of(ANY_NAME_1, ANY_TEXT_1))
                .build(),
            TABLE_METADATA);

    // Act
    Throwable thrown = catchThrowable(result::getClusteringKey);

    // Assert
    assertThat(thrown).isInstanceOf(IllegalStateException.class);
  }
}
