package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

/** */
public class PutTest {
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_TEXT_3 = "text3";
  private static final String ANY_TEXT_4 = "text4";

  private Put preparePut() {
    Key partitionKey = new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1));
    Key clusteringKey = new Key(new TextValue(ANY_NAME_2, ANY_TEXT_2));
    return new Put(partitionKey, clusteringKey);
  }

  @Test
  public void getPartitionKey_ProperKeyGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    Key expected = new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1));
    Key clusteringKey = new Key(new TextValue(ANY_NAME_2, ANY_TEXT_2));
    Put put = new Put(expected, clusteringKey);

    // Act
    Key actual = put.getPartitionKey();

    // Assert
    assertThat((Iterable<? extends Value<?>>) expected).isEqualTo(actual);
  }

  @Test
  public void getClusteringKey_ProperKeyGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    Key partitionKey = new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1));
    Key expected = new Key(new TextValue(ANY_NAME_1, ANY_TEXT_2));
    Put put = new Put(partitionKey, expected);

    // Act
    Optional<Key> actual = put.getClusteringKey();

    // Assert
    assertThat(actual).isEqualTo(Optional.of(expected));
  }

  @Test
  public void getClusteringKey_ClusteringKeyNotGivenInConstructor_ShouldReturnNull() {
    // Arrange
    Key partitionKey = new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1));
    Put put = new Put(partitionKey);

    // Act
    Optional<Key> actual = put.getClusteringKey();

    // Assert
    assertThat(actual.isPresent()).isFalse();
  }

  @Test
  public void addValue_ProperValueGiven_ShouldReturnWhatsSet() {
    // Arrange
    Put put = preparePut();
    TextValue value1 = new TextValue(ANY_NAME_1, ANY_TEXT_1);
    TextValue value2 = new TextValue(ANY_NAME_2, ANY_TEXT_2);

    // Act
    put.withValue(value1).withValue(value2);

    // Assert
    assertThat(put.getValues())
        .isEqualTo(ImmutableMap.of(value1.getName(), value1, value2.getName(), value2));
  }

  @Test
  public void addValue_ProperValuesGiven_ShouldReturnWhatsSet() {
    // Arrange
    Put put = preparePut();

    // Act
    put.withValue("val3", true)
        .withValue("val6", 5678)
        .withValue("val1", 1234L)
        .withValue("val5", 4.56f)
        .withValue("val4", 1.23)
        .withValue("val7", "string_value")
        .withValue("val2", "blob_value".getBytes());

    // Assert
    Map<String, Value<?>> values = put.getValues();
    assertThat(values.get("val3")).isEqualTo(new BooleanValue("val3", true));
    assertThat(values.get("val6")).isEqualTo(new IntValue("val6", 5678));
    assertThat(values.get("val1")).isEqualTo(new BigIntValue("val1", 1234L));
    assertThat(values.get("val5")).isEqualTo(new FloatValue("val5", 4.56f));
    assertThat(values.get("val4")).isEqualTo(new DoubleValue("val4", 1.23));
    assertThat(values.get("val7")).isEqualTo(new TextValue("val7", "string_value"));
    assertThat(values.get("val2")).isEqualTo(new BlobValue("val2", "blob_value".getBytes()));
  }

  @Test
  public void addValues_ProperValueGiven_ShouldReturnWhatsSet() {
    // Arrange
    Put put = preparePut();
    TextValue value1 = new TextValue(ANY_NAME_1, ANY_TEXT_1);
    TextValue value2 = new TextValue(ANY_NAME_2, ANY_TEXT_2);

    // Act
    put.withValues(Arrays.asList(value1, value2));

    // Assert
    assertThat(put.getValues())
        .isEqualTo(ImmutableMap.of(value1.getName(), value1, value2.getName(), value2));
  }

  @Test
  public void getValues_TryToModifyReturned_ShouldThrowException() {
    // Arrange
    Put put = preparePut();
    TextValue value1 = new TextValue(ANY_NAME_1, ANY_TEXT_1);
    TextValue value2 = new TextValue(ANY_NAME_2, ANY_TEXT_2);
    put.withValue(value1).withValue(value2);

    // Act Assert
    Map<String, Value<?>> values = put.getValues();
    assertThatThrownBy(
            () -> {
              values.put(ANY_NAME_3, new TextValue(ANY_NAME_3, ANY_TEXT_3));
            })
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
    assertThatThrownBy(
            () -> {
              new Put(null);
            })
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void equals_SameInstanceGiven_ShouldReturnTrue() {
    // Arrange
    Put put = preparePut();

    // Act
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
    put.withValue(new TextValue(ANY_NAME_3, ANY_TEXT_3));
    Put another = preparePut();
    another.withValue(new TextValue(ANY_NAME_3, ANY_TEXT_4));

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
