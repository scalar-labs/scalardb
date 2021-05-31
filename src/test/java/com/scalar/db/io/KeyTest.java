package com.scalar.db.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;
import org.junit.Test;

/** */
public class KeyTest {
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_NAME_4 = "name4";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_TEXT_3 = "text3";
  private static final String ANY_TEXT_4 = "text4";
  private static final int ANY_INT_1 = 10;
  private static final int ANY_INT_2 = 20;

  @Test
  public void get_ProperKeysGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    TextValue key1 = new TextValue(ANY_NAME_1, ANY_TEXT_1);
    TextValue key2 = new TextValue(ANY_NAME_2, ANY_TEXT_2);
    Key key = new Key(key1, key2);

    // Act
    List<Value<?>> values = key.get();

    // Assert
    assertThat(values).isEqualTo(Arrays.asList(key1, key2));
  }

  @Test
  public void get_ProperKeysGivenInBuilder_ShouldReturnWhatsSet() {
    // Arrange
    Key key =
        Key.newBuilder()
            .addBoolean("key1", true)
            .addInt("key2", 5678)
            .addBigInt("key3", 1234L)
            .addFloat("key4", 4.56f)
            .addDouble("key5", 1.23)
            .addText("key6", "string_key")
            .addBlob("key7", "blob_key".getBytes())
            .add(new IntValue("key8", 1357))
            .addAll(Arrays.asList(new IntValue("key9", 2468), new BigIntValue("key10", 1111L)))
            .build();

    // Act
    List<Value<?>> values = key.get();

    // Assert
    assertThat(values.size()).isEqualTo(10);
    assertThat(values.get(0)).isEqualTo(new BooleanValue("key1", true));
    assertThat(values.get(1)).isEqualTo(new IntValue("key2", 5678));
    assertThat(values.get(2)).isEqualTo(new BigIntValue("key3", 1234L));
    assertThat(values.get(3)).isEqualTo(new FloatValue("key4", 4.56f));
    assertThat(values.get(4)).isEqualTo(new DoubleValue("key5", 1.23));
    assertThat(values.get(5)).isEqualTo(new TextValue("key6", "string_key"));
    assertThat(values.get(6)).isEqualTo(new BlobValue("key7", "blob_key".getBytes()));
    assertThat(values.get(7)).isEqualTo(new IntValue("key8", 1357));
    assertThat(values.get(8)).isEqualTo(new IntValue("key9", 2468));
    assertThat(values.get(9)).isEqualTo(new BigIntValue("key10", 1111L));
  }

  @Test
  public void get_TryToModifyReturned_ShouldThrowException() {
    // Arrange
    TextValue key1 = new TextValue(ANY_NAME_1, ANY_TEXT_1);
    TextValue key2 = new TextValue(ANY_NAME_2, ANY_TEXT_2);
    Key key = new Key(key1, key2);

    // Act Assert
    List<Value<?>> values = key.get();
    assertThatThrownBy(
            () -> {
              values.add(new TextValue(ANY_NAME_3, ANY_TEXT_3));
            })
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void equals_DifferentObjectsSameValuesGiven_ShouldReturnTrue() {
    // Arrange
    TextValue oneKey1 = new TextValue(ANY_NAME_1, ANY_TEXT_1);
    TextValue oneKey2 = new TextValue(ANY_NAME_2, ANY_TEXT_2);
    Key oneKey = new Key(oneKey1, oneKey2);
    TextValue anotherKey1 = new TextValue(ANY_NAME_1, ANY_TEXT_1);
    TextValue anotherKey2 = new TextValue(ANY_NAME_2, ANY_TEXT_2);
    Key anotherKey = new Key(anotherKey1, anotherKey2);

    // Act
    boolean result = oneKey.equals(anotherKey);

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void equals_SameObjectsGiven_ShouldReturnTrue() {
    // Arrange
    TextValue oneKey1 = new TextValue(ANY_NAME_1, ANY_TEXT_1);
    TextValue oneKey2 = new TextValue(ANY_NAME_2, ANY_TEXT_2);
    Key oneKey = new Key(oneKey1, oneKey2);

    // Act
    boolean result = oneKey.equals(oneKey);

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void equals_DifferentObjectsDifferentValuesGiven_ShouldReturnFalse() {
    // Arrange
    TextValue oneKey1 = new TextValue(ANY_NAME_1, ANY_TEXT_1);
    TextValue oneKey2 = new TextValue(ANY_NAME_2, ANY_TEXT_2);
    Key oneKey = new Key(oneKey1, oneKey2);
    TextValue anotherKey1 = new TextValue(ANY_NAME_3, ANY_TEXT_3);
    TextValue anotherKey2 = new TextValue(ANY_NAME_4, ANY_TEXT_4);
    Key anotherKey = new Key(anotherKey1, anotherKey2);

    // Act
    boolean result = oneKey.equals(anotherKey);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void equals_DifferentTypesSameValuesGiven_ShouldReturnFalse() {
    // Arrange
    TextValue oneKey1 = new TextValue(ANY_NAME_1, ANY_TEXT_1);
    TextValue oneKey2 = new TextValue(ANY_NAME_2, ANY_TEXT_2);
    Key oneKey = new Key(oneKey1, oneKey2);
    BlobValue anotherKey1 = new BlobValue(ANY_NAME_1, ANY_TEXT_1.getBytes());
    BlobValue anotherKey2 = new BlobValue(ANY_NAME_2, ANY_TEXT_2.getBytes());
    Key anotherKey = new Key(anotherKey1, anotherKey2);

    // Act
    boolean result = oneKey.equals(anotherKey);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void compareTo_ThisTextBiggerThanGiven_ShouldReturnPositive() {
    // Arrange
    TextValue oneKey1 = new TextValue(ANY_NAME_3, ANY_TEXT_3);
    TextValue oneKey2 = new TextValue(ANY_NAME_4, ANY_TEXT_4);
    Key oneKey = new Key(oneKey1, oneKey2);
    TextValue anotherKey1 = new TextValue(ANY_NAME_1, ANY_TEXT_1);
    TextValue anotherKey2 = new TextValue(ANY_NAME_2, ANY_TEXT_2);
    Key anotherKey = new Key(anotherKey1, anotherKey2);

    // Act
    int actual = oneKey.compareTo(anotherKey);

    // Assert
    assertThat(actual > 0).isTrue();
  }

  @Test
  public void compareTo_ThisNumberBiggerThanGiven_ShouldReturnPositive() {
    // Arrange
    IntValue oneKey1 = new IntValue(ANY_NAME_1, ANY_INT_1);
    IntValue oneKey2 = new IntValue(ANY_NAME_2, ANY_INT_2);
    Key oneKey = new Key(oneKey1, oneKey2);
    IntValue anotherKey1 = new IntValue(ANY_NAME_1, ANY_INT_1);
    IntValue anotherKey2 = new IntValue(ANY_NAME_2, ANY_INT_1);
    Key anotherKey = new Key(anotherKey1, anotherKey2);

    // Act
    int actual = oneKey.compareTo(anotherKey);

    // Assert
    assertThat(actual > 0).isTrue();
  }

  @Test
  public void compareTo_ThisEqualsToGiven_ShouldReturnZero() {
    // Arrange
    TextValue oneKey1 = new TextValue(ANY_NAME_1, ANY_TEXT_1);
    IntValue oneKey2 = new IntValue(ANY_NAME_2, ANY_INT_2);
    Key oneKey = new Key(oneKey1, oneKey2);
    TextValue anotherKey1 = new TextValue(ANY_NAME_1, ANY_TEXT_1);
    IntValue anotherKey2 = new IntValue(ANY_NAME_2, ANY_INT_2);
    Key anotherKey = new Key(anotherKey1, anotherKey2);

    // Act
    int actual = oneKey.compareTo(anotherKey);

    // Assert
    assertThat(actual == 0).isTrue();
  }

  @Test
  public void compareTo_ThisTextSmallerThanGiven_ShouldReturnNegative() {
    // Arrange
    TextValue oneKey1 = new TextValue(ANY_NAME_1, ANY_TEXT_1);
    TextValue oneKey2 = new TextValue(ANY_NAME_2, ANY_TEXT_2);
    Key oneKey = new Key(oneKey1, oneKey2);
    TextValue anotherKey1 = new TextValue(ANY_NAME_3, ANY_TEXT_3);
    TextValue anotherKey2 = new TextValue(ANY_NAME_4, ANY_TEXT_4);
    Key anotherKey = new Key(anotherKey1, anotherKey2);

    // Act
    int actual = oneKey.compareTo(anotherKey);

    // Assert
    assertThat(actual < 0).isTrue();
  }

  @Test
  public void compareTo_ThisNumberSmallerThanGiven_ShouldReturnNegative() {
    // Arrange
    IntValue oneKey1 = new IntValue(ANY_NAME_1, ANY_INT_1);
    IntValue oneKey2 = new IntValue(ANY_NAME_2, ANY_INT_1);
    Key oneKey = new Key(oneKey1, oneKey2);
    IntValue anotherKey1 = new IntValue(ANY_NAME_1, ANY_INT_1);
    IntValue anotherKey2 = new IntValue(ANY_NAME_2, ANY_INT_2);
    Key anotherKey = new Key(anotherKey1, anotherKey2);

    // Act
    int actual = oneKey.compareTo(anotherKey);

    // Assert
    assertThat(actual < 0).isTrue();
  }

  @Test
  public void constructor_NullGiven_ShouldThrowNullPointerException() {
    // Act Assert
    assertThatThrownBy(
            () -> {
              new Key((List<Value<?>>) null);
            })
        .isInstanceOf(NullPointerException.class);
  }
}
