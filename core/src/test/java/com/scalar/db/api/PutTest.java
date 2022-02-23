package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

public class PutTest {
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_TEXT_3 = "text3";
  private static final String ANY_TEXT_4 = "text4";

  private Put preparePut() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Put(partitionKey, clusteringKey);
  }

  @Test
  public void getPartitionKey_ProperKeyGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    Key expected = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    Put put = new Put(expected, clusteringKey);

    // Act
    Key actual = put.getPartitionKey();

    // Assert
    assertThat((Iterable<? extends Value<?>>) expected).isEqualTo(actual);
  }

  @Test
  public void getClusteringKey_ProperKeyGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key expected = new Key(ANY_NAME_1, ANY_TEXT_2);
    Put put = new Put(partitionKey, expected);

    // Act
    Optional<Key> actual = put.getClusteringKey();

    // Assert
    assertThat(actual).isEqualTo(Optional.of(expected));
  }

  @Test
  public void getClusteringKey_ClusteringKeyNotGivenInConstructor_ShouldReturnNull() {
    // Arrange
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Put put = new Put(partitionKey);

    // Act
    Optional<Key> actual = put.getClusteringKey();

    // Assert
    assertThat(actual.isPresent()).isFalse();
  }

  @Test
  public void withValue_ProperValueGiven_ShouldReturnWhatsSet() {
    // Arrange
    Put put = preparePut();
    TextValue value1 = new TextValue(ANY_NAME_1, ANY_TEXT_1);
    TextValue value2 = new TextValue(ANY_NAME_2, ANY_TEXT_2);

    // Act
    put.withValue(value1).withValue(value2);

    // Assert
    assertThat(put.getValues())
        .isEqualTo(ImmutableMap.of(value1.getName(), value1, value2.getName(), value2));

    assertThat(put.getNullableValues())
        .isEqualTo(
            ImmutableMap.of(
                value1.getName(), Optional.of(value1), value2.getName(), Optional.of(value2)));

    assertThat(put.getContainedColumnNames()).isEqualTo(ImmutableSet.of(ANY_NAME_1, ANY_NAME_2));

    assertThat(put.containsColumn(ANY_NAME_1)).isTrue();
    assertThat(put.isNullValue(ANY_NAME_1)).isFalse();
    assertThat(put.getTextValue(ANY_NAME_1)).isEqualTo(ANY_TEXT_1);
    assertThat(put.getValueAsObject(ANY_NAME_1)).isEqualTo(ANY_TEXT_1);

    assertThat(put.containsColumn(ANY_NAME_2)).isTrue();
    assertThat(put.isNullValue(ANY_NAME_2)).isFalse();
    assertThat(put.getTextValue(ANY_NAME_2)).isEqualTo(ANY_TEXT_2);
    assertThat(put.getValueAsObject(ANY_NAME_2)).isEqualTo(ANY_TEXT_2);
  }

  @Test
  public void withValue_ProperValuesGiven_ShouldReturnWhatsSet() {
    // Arrange
    Put put = preparePut();

    // Act
    put.withValue("val1", true)
        .withValue("val2", 5678)
        .withValue("val3", 1234L)
        .withValue("val4", 4.56f)
        .withValue("val5", 1.23)
        .withValue("val6", "string_value")
        .withValue("val7", "blob_value".getBytes(StandardCharsets.UTF_8))
        .withValue("val8", ByteBuffer.wrap("blob_value2".getBytes(StandardCharsets.UTF_8)))
        .withNullValue("val9");

    // Assert
    Map<String, Value<?>> values = put.getValues();
    assertThat(values.size()).isEqualTo(9);
    assertThat(values.get("val1")).isEqualTo(new BooleanValue("val1", true));
    assertThat(values.get("val2")).isEqualTo(new IntValue("val2", 5678));
    assertThat(values.get("val3")).isEqualTo(new BigIntValue("val3", 1234L));
    assertThat(values.get("val4")).isEqualTo(new FloatValue("val4", 4.56f));
    assertThat(values.get("val5")).isEqualTo(new DoubleValue("val5", 1.23));
    assertThat(values.get("val6")).isEqualTo(new TextValue("val6", "string_value"));
    assertThat(values.get("val7"))
        .isEqualTo(new BlobValue("val7", "blob_value".getBytes(StandardCharsets.UTF_8)));
    assertThat(values.get("val8"))
        .isEqualTo(new BlobValue("val8", "blob_value2".getBytes(StandardCharsets.UTF_8)));
    assertThat(values).containsKey("val9");
    assertThat(values.get("val9")).isNull();

    Map<String, Optional<Value<?>>> nullableValues = put.getNullableValues();
    assertThat(nullableValues.size()).isEqualTo(9);
    assertThat(nullableValues.get("val1")).isEqualTo(Optional.of(new BooleanValue("val1", true)));
    assertThat(nullableValues.get("val2")).isEqualTo(Optional.of(new IntValue("val2", 5678)));
    assertThat(nullableValues.get("val3")).isEqualTo(Optional.of(new BigIntValue("val3", 1234L)));
    assertThat(nullableValues.get("val4")).isEqualTo(Optional.of(new FloatValue("val4", 4.56f)));
    assertThat(nullableValues.get("val5")).isEqualTo(Optional.of(new DoubleValue("val5", 1.23)));
    assertThat(nullableValues.get("val6"))
        .isEqualTo(Optional.of(new TextValue("val6", "string_value")));
    assertThat(nullableValues.get("val7"))
        .isEqualTo(
            Optional.of(new BlobValue("val7", "blob_value".getBytes(StandardCharsets.UTF_8))));
    assertThat(nullableValues.get("val8"))
        .isEqualTo(
            Optional.of(new BlobValue("val8", "blob_value2".getBytes(StandardCharsets.UTF_8))));
    assertThat(nullableValues.get("val9")).isEqualTo(Optional.empty());

    assertThat(put.getContainedColumnNames())
        .isEqualTo(
            ImmutableSet.of(
                "val1", "val2", "val3", "val4", "val5", "val6", "val7", "val8", "val9"));

    assertThat(put.containsColumn("val1")).isTrue();
    assertThat(put.isNullValue("val1")).isFalse();
    assertThat(put.getBooleanValue("val1")).isTrue();
    assertThat(put.getValueAsObject("val1")).isEqualTo(true);

    assertThat(put.containsColumn("val2")).isTrue();
    assertThat(put.isNullValue("val2")).isFalse();
    assertThat(put.getIntValue("val2")).isEqualTo(5678);
    assertThat(put.getValueAsObject("val2")).isEqualTo(5678);

    assertThat(put.containsColumn("val3")).isTrue();
    assertThat(put.isNullValue("val3")).isFalse();
    assertThat(put.getBigIntValue("val3")).isEqualTo(1234L);
    assertThat(put.getValueAsObject("val3")).isEqualTo(1234L);

    assertThat(put.containsColumn("val4")).isTrue();
    assertThat(put.isNullValue("val4")).isFalse();
    assertThat(put.getFloatValue("val4")).isEqualTo(4.56f);
    assertThat(put.getValueAsObject("val4")).isEqualTo(4.56f);

    assertThat(put.containsColumn("val5")).isTrue();
    assertThat(put.isNullValue("val5")).isFalse();
    assertThat(put.getDoubleValue("val5")).isEqualTo(1.23);
    assertThat(put.getValueAsObject("val5")).isEqualTo(1.23);

    assertThat(put.containsColumn("val6")).isTrue();
    assertThat(put.isNullValue("val6")).isFalse();
    assertThat(put.getTextValue("val6")).isEqualTo("string_value");
    assertThat(put.getValueAsObject("val6")).isEqualTo("string_value");

    assertThat(put.containsColumn("val7")).isTrue();
    assertThat(put.isNullValue("val7")).isFalse();
    assertThat(put.getBlobValue("val7"))
        .isEqualTo(ByteBuffer.wrap("blob_value".getBytes(StandardCharsets.UTF_8)));
    assertThat(put.getBlobValueAsByteBuffer("val7"))
        .isEqualTo(ByteBuffer.wrap("blob_value".getBytes(StandardCharsets.UTF_8)));
    assertThat(put.getBlobValueAsBytes("val7"))
        .isEqualTo("blob_value".getBytes(StandardCharsets.UTF_8));
    assertThat(put.getValueAsObject("val7"))
        .isEqualTo(ByteBuffer.wrap("blob_value".getBytes(StandardCharsets.UTF_8)));

    assertThat(put.containsColumn("val8")).isTrue();
    assertThat(put.isNullValue("val8")).isFalse();
    assertThat(put.getBlobValue("val8"))
        .isEqualTo(ByteBuffer.wrap("blob_value2".getBytes(StandardCharsets.UTF_8)));
    assertThat(put.getBlobValueAsByteBuffer("val8"))
        .isEqualTo(ByteBuffer.wrap("blob_value2".getBytes(StandardCharsets.UTF_8)));
    assertThat(put.getBlobValueAsBytes("val8"))
        .isEqualTo("blob_value2".getBytes(StandardCharsets.UTF_8));
    assertThat(put.getValueAsObject("val8"))
        .isEqualTo(ByteBuffer.wrap("blob_value2".getBytes(StandardCharsets.UTF_8)));

    assertThat(put.containsColumn("val9")).isTrue();
    assertThat(put.isNullValue("val9")).isTrue();
    assertThat(put.getValueAsObject("val9")).isNull();
  }

  @Test
  public void withValues_ProperValueGiven_ShouldReturnWhatsSet() {
    // Arrange
    Put put = preparePut();
    TextValue value1 = new TextValue(ANY_NAME_1, ANY_TEXT_1);
    TextValue value2 = new TextValue(ANY_NAME_2, ANY_TEXT_2);

    // Act
    put.withValues(Arrays.asList(value1, value2));

    // Assert
    assertThat(put.getValues())
        .isEqualTo(ImmutableMap.of(value1.getName(), value1, value2.getName(), value2));

    assertThat(put.getNullableValues())
        .isEqualTo(
            ImmutableMap.of(
                value1.getName(), Optional.of(value1), value2.getName(), Optional.of(value2)));

    assertThat(put.getContainedColumnNames()).isEqualTo(ImmutableSet.of(ANY_NAME_1, ANY_NAME_2));

    assertThat(put.containsColumn(ANY_NAME_1)).isTrue();
    assertThat(put.isNullValue(ANY_NAME_1)).isFalse();
    assertThat(put.getTextValue(ANY_NAME_1)).isEqualTo(ANY_TEXT_1);
    assertThat(put.getValueAsObject(ANY_NAME_1)).isEqualTo(ANY_TEXT_1);

    assertThat(put.containsColumn(ANY_NAME_2)).isTrue();
    assertThat(put.isNullValue(ANY_NAME_2)).isFalse();
    assertThat(put.getTextValue(ANY_NAME_2)).isEqualTo(ANY_TEXT_2);
    assertThat(put.getValueAsObject(ANY_NAME_2)).isEqualTo(ANY_TEXT_2);
  }

  @Test
  public void getNullableValues_TryToModifyReturned_ShouldThrowException() {
    // Arrange
    Put put = preparePut();
    TextValue value1 = new TextValue(ANY_NAME_1, ANY_TEXT_1);
    TextValue value2 = new TextValue(ANY_NAME_2, ANY_TEXT_2);
    put.withValue(value1).withValue(value2);

    // Act Assert
    Map<String, Optional<Value<?>>> values = put.getNullableValues();
    assertThatThrownBy(
            () -> values.put(ANY_NAME_3, Optional.of(new TextValue(ANY_NAME_3, ANY_TEXT_3))))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void setConsistency_ProperValueGiven_ShouldReturnWhatsSet() {
    // Arrange
    Put put = preparePut();
    Consistency expected = Consistency.EVENTUAL;

    // Act
    put.withConsistency(expected);

    // Assert
    assertThat(expected).isEqualTo(put.getConsistency());
  }

  @Test
  public void constructor_NullGiven_ShouldThrowNullPointerException() {
    // Act Assert
    assertThatThrownBy(() -> new Put((Key) null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void constructor_PutGiven_ShouldCopyProperly() {
    // Arrange
    Put put =
        preparePut()
            .withValue("c1", 1)
            .withCondition(new PutIfExists())
            .withConsistency(Consistency.EVENTUAL)
            .forNamespace("n1")
            .forTable("t1");

    // Act
    Put actual = new Put(put);

    // Assert
    assertThat(actual).isEqualTo(put);
  }

  @Test
  public void equals_SameInstanceGiven_ShouldReturnTrue() {
    // Arrange
    Put put = preparePut();

    // Act
    @SuppressWarnings("SelfEquals")
    boolean ret = put.equals(put);

    // Assert
    assertThat(ret).isTrue();
  }

  @Test
  public void equals_SamePutGiven_ShouldReturnTrue() {
    // Arrange
    Put put = preparePut();
    Put another = preparePut();

    // Act
    boolean ret = put.equals(another);

    // Assert
    assertThat(ret).isTrue();
    assertThat(put.hashCode()).isEqualTo(another.hashCode());
  }

  @Test
  public void equals_SamePutWithPutIfExistsGiven_ShouldReturnTrue() {
    // Arrange
    Put put = preparePut().withCondition(new PutIfExists());
    Put another = preparePut().withCondition(new PutIfExists());

    // Act
    boolean ret = put.equals(another);

    // Assert
    assertThat(ret).isTrue();
    assertThat(put.hashCode()).isEqualTo(another.hashCode());
  }

  @Test
  public void equals_SamePutWithPutIfNotExistsGiven_ShouldReturnTrue() {
    // Arrange
    Put put = preparePut().withCondition(new PutIfNotExists());
    Put another = preparePut().withCondition(new PutIfNotExists());

    // Act
    boolean ret = put.equals(another);

    // Assert
    assertThat(ret).isTrue();
    assertThat(put.hashCode()).isEqualTo(another.hashCode());
  }

  @Test
  public void equals_PutWithDifferentValuesGiven_ShouldReturnFalse() {
    // Arrange
    Put put = preparePut();
    put.withValue(ANY_NAME_3, ANY_TEXT_3);
    Put another = preparePut();
    another.withValue(ANY_NAME_3, ANY_TEXT_4);

    // Act
    boolean ret = put.equals(another);

    // Assert
    assertThat(ret).isFalse();
  }

  @Test
  public void equals_PutWithDifferentConditionsGiven_ShouldReturnFalse() {
    // Arrange
    Put put = preparePut();
    put.withCondition(new PutIfExists());
    Put another = preparePut();
    another.withCondition(new PutIfNotExists());

    // Act
    boolean ret = put.equals(another);

    // Assert
    assertThat(ret).isFalse();
  }
}
