package com.scalar.db.storage.common;

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
import com.scalar.db.util.ResultImpl;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
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

  private TableMetadata metadata;
  private Map<String, Value<?>> values;

  @Before
  public void setUp() {
    metadata =
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

    values =
        ImmutableMap.<String, Value<?>>builder()
            .put(ANY_NAME_1, new TextValue(ANY_NAME_1, ANY_TEXT_1))
            .put(ANY_NAME_2, new TextValue(ANY_NAME_2, ANY_TEXT_2))
            .put(ANY_COLUMN_NAME_1, new BooleanValue(ANY_COLUMN_NAME_1, true))
            .put(ANY_COLUMN_NAME_2, new IntValue(ANY_COLUMN_NAME_2, Integer.MAX_VALUE))
            .put(ANY_COLUMN_NAME_3, new BigIntValue(ANY_COLUMN_NAME_3, BigIntValue.MAX_VALUE))
            .put(ANY_COLUMN_NAME_4, new FloatValue(ANY_COLUMN_NAME_4, Float.MAX_VALUE))
            .put(ANY_COLUMN_NAME_5, new DoubleValue(ANY_COLUMN_NAME_5, Double.MAX_VALUE))
            .put(ANY_COLUMN_NAME_6, new TextValue(ANY_COLUMN_NAME_6, "string"))
            .put(
                ANY_COLUMN_NAME_7,
                new BlobValue(ANY_COLUMN_NAME_7, "bytes".getBytes(StandardCharsets.UTF_8)))
            .build();
  }

  @Test
  public void getValue_ProperValuesGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    ResultImpl result = new ResultImpl(values, metadata);

    // Act
    Optional<Value<?>> actual = result.getValue(ANY_NAME_1);

    // Assert
    assertThat(actual).isEqualTo(Optional.of(new TextValue(ANY_NAME_1, ANY_TEXT_1)));
  }

  @Test
  public void getValues_ProperValuesGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    ResultImpl result = new ResultImpl(values, metadata);

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
    Map<String, Value<?>> emptyValues = Collections.emptyMap();
    ResultImpl result = new ResultImpl(emptyValues, metadata);

    // Act
    Map<String, Value<?>> actual = result.getValues();

    // Assert
    assertThat(actual.isEmpty()).isTrue();
  }

  @Test
  public void getValues_TryToModifyReturned_ShouldThrowException() {
    // Arrange
    ResultImpl result = new ResultImpl(values, metadata);
    Map<String, Value<?>> values = result.getValues();

    // Act Assert
    assertThatThrownBy(() -> values.put("new", new TextValue(ANY_NAME_1, ANY_TEXT_1)))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void equals_DifferentObjectsSameValuesGiven_ShouldReturnTrue() {
    // Arrange
    ResultImpl r1 = new ResultImpl(values, metadata);
    ResultImpl r2 = new ResultImpl(values, metadata);

    // Act Assert
    assertThat(r1.equals(r2)).isTrue();
  }

  @Test
  public void equals_DifferentObjectsDifferentValuesGiven_ShouldReturnFalse() {
    // Arrange
    ResultImpl r1 = new ResultImpl(values, metadata);
    Map<String, Value<?>> emptyValues = Collections.emptyMap();
    ResultImpl r2 = new ResultImpl(emptyValues, metadata);

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
    ResultImpl result = new ResultImpl(values, metadata);

    // Act
    Optional<Key> key = result.getPartitionKey();

    // Assert
    assertThat(key.isPresent()).isTrue();
    assertThat(key.get().get().size()).isEqualTo(1);
    assertThat(key.get().get().get(0)).isEqualTo(new TextValue(ANY_NAME_1, ANY_TEXT_1));
  }

  @Test
  public void getClusteringKey_RequiredValuesGiven_ShouldReturnClusteringKey() {
    // Arrange
    ResultImpl result = new ResultImpl(values, metadata);

    // Act
    Optional<Key> key = result.getClusteringKey();

    // Assert
    assertThat(key.isPresent()).isTrue();
    assertThat(key.get().get().size()).isEqualTo(1);
    assertThat(key.get().get().get(0)).isEqualTo(new TextValue(ANY_NAME_2, ANY_TEXT_2));
  }
}
