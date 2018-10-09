package com.scalar.database.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Optional;
import org.junit.Test;

/** */
public class BlobValueTest {
  private static final String ANY_NAME = "name";
  private static final String ANOTHER_NAME = "another_name";

  @Test
  public void get_ProperValueGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    byte[] expected = "some_text".getBytes();
    BlobValue value = new BlobValue(ANY_NAME, expected);

    // Act
    Optional<byte[]> actual = value.get();

    // Assert
    assertThat(Arrays.equals(expected, actual.get())).isTrue();
    assertThat(expected == actual.get()).isFalse();
  }

  @Test
  public void equals_DifferentObjectsSameValuesGiven_ShouldReturnTrue() {
    // Arrange
    BlobValue oneValue = new BlobValue(ANY_NAME, "some_text".getBytes());
    BlobValue anotherValue = new BlobValue(ANY_NAME, "some_text".getBytes());

    // Act
    boolean result = oneValue.equals(anotherValue);

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void equals_DifferentObjectsSameValuesDifferentNamesGiven_ShouldReturnFalse() {
    // Arrange
    BlobValue oneValue = new BlobValue(ANY_NAME, "some_text".getBytes());
    BlobValue anotherValue = new BlobValue(ANOTHER_NAME, "some_text".getBytes());

    // Act
    boolean result = oneValue.equals(anotherValue);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void equals_SameObjectsGiven_ShouldReturnTrue() {
    // Arrange
    BlobValue value = new BlobValue(ANY_NAME, "some_text".getBytes());

    // Act
    boolean result = value.equals(value);

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void equals_DifferentObjectsDifferentValuesGiven_ShouldReturnFalse() {
    // Arrange
    BlobValue oneValue = new BlobValue(ANY_NAME, "some_text".getBytes());
    BlobValue anotherValue = new BlobValue(ANY_NAME, "another_text".getBytes());

    // Act
    boolean result = oneValue.equals(anotherValue);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void equals_DifferentTypesSameValuesGiven_ShouldReturnFalse() {
    // Arrange
    BlobValue oneValue = new BlobValue(ANY_NAME, "some_text".getBytes());
    TextValue anotherValue = new TextValue(ANY_NAME, "some_text");

    // Act
    boolean result = oneValue.equals(anotherValue);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void compareTo_ThisBiggerThanGiven_ShouldReturnPositive() {
    // Arrange
    BlobValue oneValue = new BlobValue(ANY_NAME, "some_value2".getBytes());
    BlobValue anotherValue = new BlobValue(ANY_NAME, "some_value1".getBytes());

    // Act
    int actual = oneValue.compareTo(anotherValue);

    // Assert
    assertThat(actual > 0).isTrue();
  }

  @Test
  public void compareTo_ThisEqualsToGiven_ShouldReturnZero() {
    // Arrange
    BlobValue oneValue = new BlobValue(ANY_NAME, "some_value".getBytes());
    BlobValue anotherValue = new BlobValue(ANY_NAME, "some_value".getBytes());

    // Act
    int actual = oneValue.compareTo(anotherValue);

    // Assert
    assertThat(actual == 0).isTrue();
  }

  @Test
  public void compareTo_ThisSmallerThanGiven_ShouldReturnNegative() {
    // Arrange
    BlobValue oneValue = new BlobValue(ANY_NAME, "some_value1".getBytes());
    BlobValue anotherValue = new BlobValue(ANY_NAME, "some_value2".getBytes());

    // Act
    int actual = oneValue.compareTo(anotherValue);

    // Assert
    assertThat(actual < 0).isTrue();
  }

  @Test
  public void constructor_NullGiven_ShouldThrowNullPointerException() {
    // Act Assert
    assertThatThrownBy(
            () -> {
              new BlobValue(null, null);
            })
        .isInstanceOf(NullPointerException.class);
  }
}
