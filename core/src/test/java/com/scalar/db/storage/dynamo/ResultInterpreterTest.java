package com.scalar.db.storage.dynamo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;

import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Value;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

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

  @Test
  public void interpret_ShouldReturnWhatsSet() {
    // Arrange
    TableMetadata metadata =
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
            .addPartitionKey(ANY_NAME_1)
            .addClusteringKey(ANY_NAME_2)
            .build();

    Map<String, AttributeValue> item = new HashMap<>();
    item.put(DynamoOperation.PARTITION_KEY, AttributeValue.builder().s(ANY_TEXT_1).build());
    item.put(ANY_NAME_1, AttributeValue.builder().s(ANY_TEXT_1).build());
    item.put(ANY_NAME_2, AttributeValue.builder().s(ANY_TEXT_2).build());
    item.put(ANY_COLUMN_NAME_1, AttributeValue.builder().bool(true).build());
    item.put(
        ANY_COLUMN_NAME_2, AttributeValue.builder().n(String.valueOf(Integer.MAX_VALUE)).build());
    item.put(
        ANY_COLUMN_NAME_3,
        AttributeValue.builder().n(String.valueOf(BigIntValue.MAX_VALUE)).build());
    item.put(
        ANY_COLUMN_NAME_4, AttributeValue.builder().n(String.valueOf(Float.MAX_VALUE)).build());
    item.put(
        ANY_COLUMN_NAME_5, AttributeValue.builder().n(String.valueOf(Double.MAX_VALUE)).build());
    item.put(ANY_COLUMN_NAME_6, AttributeValue.builder().s("string").build());
    item.put(
        ANY_COLUMN_NAME_7,
        AttributeValue.builder()
            .b(SdkBytes.fromByteArray("bytes".getBytes(StandardCharsets.UTF_8)))
            .build());

    List<String> projections = Collections.emptyList();
    ResultInterpreter spy = spy(new ResultInterpreter(projections, metadata));

    // Act
    Result result = spy.interpret(item);

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
  }
}
