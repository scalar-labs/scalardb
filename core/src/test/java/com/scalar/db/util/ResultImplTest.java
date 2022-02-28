package com.scalar.db.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

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
          .addPartitionKey(ANY_NAME_1)
          .addClusteringKey(ANY_NAME_2)
          .build();

  private Map<String, Optional<Value<?>>> values;

  @Before
  public void setUp() {
    values =
        ImmutableMap.<String, Optional<Value<?>>>builder()
            .put(ANY_NAME_1, Optional.of(new TextValue(ANY_NAME_1, ANY_TEXT_1)))
            .put(ANY_NAME_2, Optional.of(new TextValue(ANY_NAME_2, ANY_TEXT_2)))
            .put(ANY_COLUMN_NAME_1, Optional.of(new BooleanValue(ANY_COLUMN_NAME_1, true)))
            .put(ANY_COLUMN_NAME_2, Optional.of(new IntValue(ANY_COLUMN_NAME_2, Integer.MAX_VALUE)))
            .put(
                ANY_COLUMN_NAME_3,
                Optional.of(new BigIntValue(ANY_COLUMN_NAME_3, BigIntValue.MAX_VALUE)))
            .put(ANY_COLUMN_NAME_4, Optional.of(new FloatValue(ANY_COLUMN_NAME_4, Float.MAX_VALUE)))
            .put(
                ANY_COLUMN_NAME_5,
                Optional.of(new DoubleValue(ANY_COLUMN_NAME_5, Double.MAX_VALUE)))
            .put(ANY_COLUMN_NAME_6, Optional.of(new TextValue(ANY_COLUMN_NAME_6, "string")))
            .put(
                ANY_COLUMN_NAME_7,
                Optional.of(
                    new BlobValue(ANY_COLUMN_NAME_7, "bytes".getBytes(StandardCharsets.UTF_8))))
            .build();
  }

  @Test
  public void getValue_ProperValuesGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    ResultImpl result = new ResultImpl(values, TABLE_METADATA);

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
  }

  @Test
  public void getValue_ProperNullValuesGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    ResultImpl result =
        new ResultImpl(
            ImmutableMap.<String, Optional<Value<?>>>builder()
                .put(ANY_NAME_1, Optional.of(new TextValue(ANY_NAME_1, ANY_TEXT_1)))
                .put(ANY_NAME_2, Optional.of(new TextValue(ANY_NAME_2, ANY_TEXT_2)))
                .put(ANY_COLUMN_NAME_1, Optional.empty())
                .put(ANY_COLUMN_NAME_2, Optional.empty())
                .put(ANY_COLUMN_NAME_3, Optional.empty())
                .put(ANY_COLUMN_NAME_4, Optional.empty())
                .put(ANY_COLUMN_NAME_5, Optional.empty())
                .put(ANY_COLUMN_NAME_6, Optional.empty())
                .put(ANY_COLUMN_NAME_7, Optional.empty())
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
            ImmutableMap.<String, Optional<Value<?>>>builder()
                .put(ANY_NAME_1, Optional.of(new TextValue(ANY_NAME_1, ANY_TEXT_1)))
                .put(ANY_NAME_2, Optional.of(new TextValue(ANY_NAME_2, ANY_TEXT_2)))
                .put(ANY_COLUMN_NAME_1, Optional.of(new BooleanValue(ANY_COLUMN_NAME_1, true)))
                .put(
                    ANY_COLUMN_NAME_2,
                    Optional.of(new IntValue(ANY_COLUMN_NAME_2, Integer.MAX_VALUE)))
                .put(
                    ANY_COLUMN_NAME_3,
                    Optional.of(new BigIntValue(ANY_COLUMN_NAME_3, BigIntValue.MAX_VALUE)))
                .put(
                    ANY_COLUMN_NAME_4,
                    Optional.of(new FloatValue(ANY_COLUMN_NAME_4, Float.MAX_VALUE)))
                .put(
                    ANY_COLUMN_NAME_5,
                    Optional.of(new DoubleValue(ANY_COLUMN_NAME_5, Double.MAX_VALUE)))
                .put(
                    ANY_COLUMN_NAME_6, Optional.of(new TextValue(ANY_COLUMN_NAME_6, (String) null)))
                .put(
                    ANY_COLUMN_NAME_7, Optional.of(new BlobValue(ANY_COLUMN_NAME_7, (byte[]) null)))
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
    ResultImpl result = new ResultImpl(values, TABLE_METADATA);

    // Act
    Map<String, Value<?>> actual = result.getValues();

    // Assert
    assertThat(actual.get(ANY_NAME_1)).isEqualTo(new TextValue(ANY_NAME_1, ANY_TEXT_1));
    assertThat(actual.get(ANY_NAME_2)).isEqualTo(new TextValue(ANY_NAME_2, ANY_TEXT_2));
    assertThat(actual.get(ANY_COLUMN_NAME_1)).isEqualTo(new BooleanValue(ANY_COLUMN_NAME_1, true));
    assertThat(actual.get(ANY_COLUMN_NAME_7))
        .isEqualTo(new BlobValue(ANY_COLUMN_NAME_7, "bytes".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  public void getValues_NoValuesGivenInConstructor_ShouldReturnEmptyValues() {
    // Arrange
    Map<String, Optional<Value<?>>> emptyValues = Collections.emptyMap();
    ResultImpl result = new ResultImpl(emptyValues, TABLE_METADATA);

    // Act
    Map<String, Value<?>> actual = result.getValues();

    // Assert
    assertThat(actual.isEmpty()).isTrue();
  }

  @Test
  public void getValues_TryToModifyReturned_ShouldThrowException() {
    // Arrange
    ResultImpl result = new ResultImpl(values, TABLE_METADATA);
    Map<String, Value<?>> values = result.getValues();

    // Act Assert
    assertThatThrownBy(() -> values.put("new", new TextValue(ANY_NAME_1, ANY_TEXT_1)))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void equals_DifferentObjectsSameValuesGiven_ShouldReturnTrue() {
    // Arrange
    ResultImpl r1 = new ResultImpl(values, TABLE_METADATA);
    ResultImpl r2 = new ResultImpl(values, TABLE_METADATA);

    // Act Assert
    assertThat(r1.equals(r2)).isTrue();
  }

  @Test
  public void equals_DifferentObjectsDifferentValuesGiven_ShouldReturnFalse() {
    // Arrange
    ResultImpl r1 = new ResultImpl(values, TABLE_METADATA);
    Map<String, Optional<Value<?>>> emptyValues = Collections.emptyMap();
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
    ResultImpl result = new ResultImpl(values, TABLE_METADATA);

    // Act
    Optional<Key> key = result.getPartitionKey();

    // Assert
    assertThat(key.isPresent()).isTrue();
    assertThat(key.get().get().size()).isEqualTo(1);
    assertThat(key.get().get().get(0)).isEqualTo(new TextValue(ANY_NAME_1, ANY_TEXT_1));
  }

  @Test
  public void getPartitionKey_NotRequiredValuesGiven_ShouldReturnPartitionKey() {
    // Arrange
    ResultImpl result1 =
        new ResultImpl(
            ImmutableMap.<String, Optional<Value<?>>>builder()
                .put(ANY_NAME_2, Optional.of(new TextValue(ANY_NAME_2, ANY_TEXT_2)))
                .build(),
            TABLE_METADATA);

    ResultImpl result2 =
        new ResultImpl(
            ImmutableMap.<String, Optional<Value<?>>>builder()
                .put(ANY_NAME_1, Optional.empty())
                .put(ANY_NAME_2, Optional.of(new TextValue(ANY_NAME_2, ANY_TEXT_2)))
                .build(),
            TABLE_METADATA);

    // Act
    Optional<Key> key1 = result1.getPartitionKey();
    Optional<Key> key2 = result2.getPartitionKey();

    // Assert
    assertThat(key1).isNotPresent();
    assertThat(key2).isNotPresent();
  }

  @Test
  public void getClusteringKey_RequiredValuesGiven_ShouldReturnClusteringKey() {
    // Arrange
    ResultImpl result = new ResultImpl(values, TABLE_METADATA);

    // Act
    Optional<Key> key = result.getClusteringKey();

    // Assert
    assertThat(key.isPresent()).isTrue();
    assertThat(key.get().get().size()).isEqualTo(1);
    assertThat(key.get().get().get(0)).isEqualTo(new TextValue(ANY_NAME_2, ANY_TEXT_2));
  }

  @Test
  public void getClusteringKey_NotRequiredValuesGiven_ShouldReturnClusteringKey() {
    // Arrange
    ResultImpl result1 =
        new ResultImpl(
            ImmutableMap.<String, Optional<Value<?>>>builder()
                .put(ANY_NAME_1, Optional.of(new TextValue(ANY_NAME_1, ANY_TEXT_1)))
                .build(),
            TABLE_METADATA);

    ResultImpl result2 =
        new ResultImpl(
            ImmutableMap.<String, Optional<Value<?>>>builder()
                .put(ANY_NAME_1, Optional.of(new TextValue(ANY_NAME_1, ANY_TEXT_1)))
                .put(ANY_NAME_2, Optional.empty())
                .build(),
            TABLE_METADATA);

    // Act
    Optional<Key> key1 = result1.getClusteringKey();
    Optional<Key> key2 = result2.getClusteringKey();

    // Assert
    assertThat(key1).isNotPresent();
    assertThat(key2).isNotPresent();
  }
}
