package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.Value;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

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
  private static final String ANY_ID_1 = "id";

  private static final TableMetadata TABLE_METADATA =
      TableMetadata.newBuilder()
          .addColumn(ANY_NAME_1, com.scalar.db.io.DataType.TEXT)
          .addColumn(ANY_NAME_2, com.scalar.db.io.DataType.TEXT)
          .addColumn(ANY_COLUMN_NAME_1, com.scalar.db.io.DataType.BOOLEAN)
          .addColumn(ANY_COLUMN_NAME_2, com.scalar.db.io.DataType.INT)
          .addColumn(ANY_COLUMN_NAME_3, com.scalar.db.io.DataType.BIGINT)
          .addColumn(ANY_COLUMN_NAME_4, com.scalar.db.io.DataType.FLOAT)
          .addColumn(ANY_COLUMN_NAME_5, com.scalar.db.io.DataType.DOUBLE)
          .addColumn(ANY_COLUMN_NAME_6, com.scalar.db.io.DataType.TEXT)
          .addColumn(ANY_COLUMN_NAME_7, com.scalar.db.io.DataType.BLOB)
          .addPartitionKey(ANY_NAME_1)
          .addClusteringKey(ANY_NAME_2)
          .build();

  @Test
  public void interpret_ShouldReturnWhatsSet() {
    // Arrange
    Map<String, Object> recordValues =
        ImmutableMap.<String, Object>builder()
            .put(ANY_COLUMN_NAME_1, true)
            .put(ANY_COLUMN_NAME_2, Integer.MAX_VALUE)
            .put(ANY_COLUMN_NAME_3, BigIntValue.MAX_VALUE)
            .put(ANY_COLUMN_NAME_4, Float.MAX_VALUE)
            .put(ANY_COLUMN_NAME_5, Double.MAX_VALUE)
            .put(ANY_COLUMN_NAME_6, "string")
            .put(
                ANY_COLUMN_NAME_7,
                Base64.getEncoder().encodeToString("bytes".getBytes(StandardCharsets.UTF_8)))
            .build();
    Record record =
        new Record(
            ANY_ID_1,
            ANY_TEXT_1,
            ImmutableMap.of(ANY_NAME_1, ANY_TEXT_1),
            ImmutableMap.of(ANY_NAME_2, ANY_TEXT_2),
            recordValues);
    List<String> projections = Collections.emptyList();
    ResultInterpreter spy = spy(new ResultInterpreter(projections, TABLE_METADATA));

    // Act
    Result result = spy.interpret(record);

    // Assert
    assertThat(result.getValue(ANY_NAME_1).isPresent()).isTrue();
    assertThat(result.getValue(ANY_NAME_1).get().getAsString().isPresent()).isTrue();
    assertThat(result.getValue(ANY_NAME_1).get().getAsString().get()).isEqualTo(ANY_TEXT_1);
    assertThat(result.getValue(ANY_NAME_2).isPresent()).isTrue();
    assertThat(result.getValue(ANY_NAME_2).get().getAsString().isPresent()).isTrue();
    assertThat(result.getValue(ANY_NAME_2).get().getAsString().get()).isEqualTo(ANY_TEXT_2);
    assertThat(result.getValue(ANY_COLUMN_NAME_1).isPresent()).isTrue();
    assertThat(result.getValue(ANY_COLUMN_NAME_1).get().getAsBoolean()).isTrue();
    assertThat(result.getValue(ANY_COLUMN_NAME_2).isPresent()).isTrue();
    assertThat(result.getValue(ANY_COLUMN_NAME_2).get().getAsInt()).isEqualTo(Integer.MAX_VALUE);
    assertThat(result.getValue(ANY_COLUMN_NAME_3).isPresent()).isTrue();
    assertThat(result.getValue(ANY_COLUMN_NAME_3).get().getAsLong())
        .isEqualTo(BigIntValue.MAX_VALUE);
    assertThat(result.getValue(ANY_COLUMN_NAME_4).isPresent()).isTrue();
    assertThat(result.getValue(ANY_COLUMN_NAME_4).get().getAsFloat()).isEqualTo(Float.MAX_VALUE);
    assertThat(result.getValue(ANY_COLUMN_NAME_5).isPresent()).isTrue();
    assertThat(result.getValue(ANY_COLUMN_NAME_5).get().getAsDouble()).isEqualTo(Double.MAX_VALUE);
    assertThat(result.getValue(ANY_COLUMN_NAME_6).isPresent()).isTrue();
    assertThat(result.getValue(ANY_COLUMN_NAME_6).get().getAsString().isPresent()).isTrue();
    assertThat(result.getValue(ANY_COLUMN_NAME_6).get().getAsString().get()).isEqualTo("string");
    assertThat(result.getValue(ANY_COLUMN_NAME_7).isPresent()).isTrue();
    assertThat(result.getValue(ANY_COLUMN_NAME_7).get().getAsBytes().isPresent()).isTrue();
    assertThat(result.getValue(ANY_COLUMN_NAME_7).get().getAsBytes().get())
        .isEqualTo("bytes".getBytes(StandardCharsets.UTF_8));

    Map<String, Value<?>> values = result.getValues();
    assertThat(values.containsKey(ANY_NAME_1)).isTrue();
    assertThat(values.get(ANY_NAME_1).getAsString().isPresent()).isTrue();
    assertThat(values.get(ANY_NAME_1).getAsString().get()).isEqualTo(ANY_TEXT_1);
    assertThat(values.containsKey(ANY_NAME_2)).isTrue();
    assertThat(values.get(ANY_NAME_2).getAsString().isPresent()).isTrue();
    assertThat(values.get(ANY_NAME_2).getAsString().get()).isEqualTo(ANY_TEXT_2);
    assertThat(values.containsKey(ANY_COLUMN_NAME_1)).isTrue();
    assertThat(values.get(ANY_COLUMN_NAME_1).getAsBoolean()).isTrue();
    assertThat(values.containsKey(ANY_COLUMN_NAME_2)).isTrue();
    assertThat(values.get(ANY_COLUMN_NAME_2).getAsInt()).isEqualTo(Integer.MAX_VALUE);
    assertThat(values.containsKey(ANY_COLUMN_NAME_3)).isTrue();
    assertThat(values.get(ANY_COLUMN_NAME_3).getAsLong()).isEqualTo(BigIntValue.MAX_VALUE);
    assertThat(values.containsKey(ANY_COLUMN_NAME_4)).isTrue();
    assertThat(values.get(ANY_COLUMN_NAME_4).getAsFloat()).isEqualTo(Float.MAX_VALUE);
    assertThat(values.containsKey(ANY_COLUMN_NAME_5)).isTrue();
    assertThat(values.get(ANY_COLUMN_NAME_5).getAsDouble()).isEqualTo(Double.MAX_VALUE);
    assertThat(values.containsKey(ANY_COLUMN_NAME_6)).isTrue();
    assertThat(values.get(ANY_COLUMN_NAME_6).getAsString().isPresent()).isTrue();
    assertThat(values.get(ANY_COLUMN_NAME_6).getAsString().get()).isEqualTo("string");
    assertThat(values.containsKey(ANY_COLUMN_NAME_7)).isTrue();
    assertThat(values.get(ANY_COLUMN_NAME_7).getAsBytes().isPresent()).isTrue();
    assertThat(values.get(ANY_COLUMN_NAME_7).getAsBytes().get())
        .isEqualTo("bytes".getBytes(StandardCharsets.UTF_8));

    assertThat(result.isNull(ANY_NAME_1)).isFalse();
    assertThat(result.getText(ANY_NAME_1)).isEqualTo(ANY_TEXT_1);
    assertThat(result.isNull(ANY_NAME_2)).isFalse();
    assertThat(result.getText(ANY_NAME_2)).isEqualTo(ANY_TEXT_2);
    assertThat(result.isNull(ANY_COLUMN_NAME_1)).isFalse();
    assertThat(result.getBoolean(ANY_COLUMN_NAME_1)).isTrue();
    assertThat(result.isNull(ANY_COLUMN_NAME_2)).isFalse();
    assertThat(result.getInt(ANY_COLUMN_NAME_2)).isEqualTo(Integer.MAX_VALUE);
    assertThat(result.isNull(ANY_COLUMN_NAME_3)).isFalse();
    assertThat(result.getBigInt(ANY_COLUMN_NAME_3)).isEqualTo(BigIntValue.MAX_VALUE);
    assertThat(result.isNull(ANY_COLUMN_NAME_4)).isFalse();
    assertThat(result.getFloat(ANY_COLUMN_NAME_4)).isEqualTo(Float.MAX_VALUE);
    assertThat(result.isNull(ANY_COLUMN_NAME_5)).isFalse();
    assertThat(result.getDouble(ANY_COLUMN_NAME_5)).isEqualTo(Double.MAX_VALUE);
    assertThat(result.isNull(ANY_COLUMN_NAME_6)).isFalse();
    assertThat(result.getText(ANY_COLUMN_NAME_6)).isEqualTo("string");
    assertThat(result.isNull(ANY_COLUMN_NAME_7)).isFalse();
    assertThat(result.getBlob(ANY_COLUMN_NAME_7))
        .isEqualTo(ByteBuffer.wrap("bytes".getBytes(StandardCharsets.UTF_8)));
    assertThat(result.getBlobAsBytes(ANY_COLUMN_NAME_7))
        .isEqualTo("bytes".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void interpret_ShouldReturnWhatsSetWithNullValues() {
    // Arrange
    Map<String, Object> recordValues = new HashMap<>();
    recordValues.put(ANY_COLUMN_NAME_1, null);
    recordValues.put(ANY_COLUMN_NAME_2, null);
    recordValues.put(ANY_COLUMN_NAME_3, null);
    recordValues.put(ANY_COLUMN_NAME_4, null);
    recordValues.put(ANY_COLUMN_NAME_5, null);
    recordValues.put(ANY_COLUMN_NAME_6, null);
    recordValues.put(ANY_COLUMN_NAME_7, null);
    Record record =
        new Record(
            ANY_ID_1,
            ANY_TEXT_1,
            ImmutableMap.of(ANY_NAME_1, ANY_TEXT_1),
            ImmutableMap.of(ANY_NAME_2, ANY_TEXT_2),
            recordValues);

    List<String> projections = Collections.emptyList();

    ResultInterpreter spy = spy(new ResultInterpreter(projections, TABLE_METADATA));

    // Act
    Result result = spy.interpret(record);

    // Assert
    assertThat(result.getValue(ANY_NAME_1).isPresent()).isTrue();
    assertThat(result.getValue(ANY_NAME_1).get().getAsString().isPresent()).isTrue();
    assertThat(result.getValue(ANY_NAME_1).get().getAsString().get()).isEqualTo(ANY_TEXT_1);
    assertThat(result.getValue(ANY_NAME_2).isPresent()).isTrue();
    assertThat(result.getValue(ANY_NAME_2).get().getAsString().isPresent()).isTrue();
    assertThat(result.getValue(ANY_NAME_2).get().getAsString().get()).isEqualTo(ANY_TEXT_2);

    assertThat(result.getValue(ANY_COLUMN_NAME_1).isPresent()).isTrue();
    assertThat(result.getValue(ANY_COLUMN_NAME_1).get().getAsBoolean()).isFalse();
    assertThat(result.getValue(ANY_COLUMN_NAME_2).isPresent()).isTrue();
    assertThat(result.getValue(ANY_COLUMN_NAME_2).get().getAsInt()).isEqualTo(0);
    assertThat(result.getValue(ANY_COLUMN_NAME_3).isPresent()).isTrue();
    assertThat(result.getValue(ANY_COLUMN_NAME_3).get().getAsLong()).isEqualTo(0L);
    assertThat(result.getValue(ANY_COLUMN_NAME_4).isPresent()).isTrue();
    assertThat(result.getValue(ANY_COLUMN_NAME_4).get().getAsFloat()).isEqualTo(0.0F);
    assertThat(result.getValue(ANY_COLUMN_NAME_5).isPresent()).isTrue();
    assertThat(result.getValue(ANY_COLUMN_NAME_5).get().getAsDouble()).isEqualTo(0.0D);
    assertThat(result.getValue(ANY_COLUMN_NAME_6).isPresent()).isTrue();
    assertThat(result.getValue(ANY_COLUMN_NAME_6).get().getAsString()).isNotPresent();
    assertThat(result.getValue(ANY_COLUMN_NAME_7).isPresent()).isTrue();
    assertThat(result.getValue(ANY_COLUMN_NAME_7).get().getAsBytes()).isNotPresent();

    Map<String, Value<?>> values = result.getValues();
    assertThat(values.containsKey(ANY_NAME_1)).isTrue();
    assertThat(values.get(ANY_NAME_1).getAsString().isPresent()).isTrue();
    assertThat(values.get(ANY_NAME_1).getAsString().get()).isEqualTo(ANY_TEXT_1);
    assertThat(values.containsKey(ANY_NAME_2)).isTrue();
    assertThat(values.get(ANY_NAME_2).getAsString().isPresent()).isTrue();
    assertThat(values.get(ANY_NAME_2).getAsString().get()).isEqualTo(ANY_TEXT_2);

    assertThat(values.containsKey(ANY_COLUMN_NAME_1)).isTrue();
    assertThat(values.get(ANY_COLUMN_NAME_1).getAsBoolean()).isFalse();
    assertThat(values.containsKey(ANY_COLUMN_NAME_2)).isTrue();
    assertThat(values.get(ANY_COLUMN_NAME_2).getAsInt()).isEqualTo(0);
    assertThat(values.containsKey(ANY_COLUMN_NAME_3)).isTrue();
    assertThat(values.get(ANY_COLUMN_NAME_3).getAsLong()).isEqualTo(0L);
    assertThat(values.containsKey(ANY_COLUMN_NAME_4)).isTrue();
    assertThat(values.get(ANY_COLUMN_NAME_4).getAsFloat()).isEqualTo(0.0F);
    assertThat(values.containsKey(ANY_COLUMN_NAME_5)).isTrue();
    assertThat(values.get(ANY_COLUMN_NAME_5).getAsDouble()).isEqualTo(0.0D);
    assertThat(values.containsKey(ANY_COLUMN_NAME_6)).isTrue();
    assertThat(values.get(ANY_COLUMN_NAME_6).getAsString()).isNotPresent();
    assertThat(values.containsKey(ANY_COLUMN_NAME_7)).isTrue();
    assertThat(values.get(ANY_COLUMN_NAME_7).getAsBytes()).isNotPresent();

    assertThat(result.isNull(ANY_NAME_1)).isFalse();
    assertThat(result.getText(ANY_NAME_1)).isEqualTo(ANY_TEXT_1);
    assertThat(result.isNull(ANY_NAME_2)).isFalse();
    assertThat(result.getText(ANY_NAME_2)).isEqualTo(ANY_TEXT_2);
    assertThat(result.isNull(ANY_COLUMN_NAME_1)).isTrue();
    assertThat(result.getBoolean(ANY_COLUMN_NAME_1)).isFalse();
    assertThat(result.isNull(ANY_COLUMN_NAME_2)).isTrue();
    assertThat(result.getInt(ANY_COLUMN_NAME_2)).isEqualTo(0);
    assertThat(result.isNull(ANY_COLUMN_NAME_3)).isTrue();
    assertThat(result.getBigInt(ANY_COLUMN_NAME_3)).isEqualTo(0L);
    assertThat(result.isNull(ANY_COLUMN_NAME_4)).isTrue();
    assertThat(result.getFloat(ANY_COLUMN_NAME_4)).isEqualTo(0.0F);
    assertThat(result.isNull(ANY_COLUMN_NAME_5)).isTrue();
    assertThat(result.getDouble(ANY_COLUMN_NAME_5)).isEqualTo(0.0D);
    assertThat(result.isNull(ANY_COLUMN_NAME_6)).isTrue();
    assertThat(result.getText(ANY_COLUMN_NAME_6)).isNull();
    assertThat(result.isNull(ANY_COLUMN_NAME_7)).isTrue();
    assertThat(result.getBlob(ANY_COLUMN_NAME_7)).isNull();
    assertThat(result.getBlobAsBytes(ANY_COLUMN_NAME_7)).isNull();
  }
}
