package com.scalar.database.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import org.junit.Test;

/** */
public class TextValueTest {
  private static final String ANY_NAME = "name";
  private static final String ANOTHER_NAME = "another_name";

  @Test
  public void getBytes_ProperValueGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    byte[] expected = new String("some_text").getBytes(StandardCharsets.UTF_8);
    TextValue value = new TextValue(ANY_NAME, expected);

    // Act
    Optional<byte[]> actual = value.getBytes();

    // Assert
    assertThat(Arrays.equals(expected, actual.get())).isTrue();
    assertThat(expected == actual.get()).isFalse();
  }

  @Test
  public void getString_ProperValueGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    String expected = new String("some_text");
    TextValue value = new TextValue(ANY_NAME, expected);

    // Act
    Optional<String> actual = value.getString();

    // Assert
    assertThat(expected.equals(actual.get())).isTrue();
    assertThat(expected == actual.get()).isFalse();
  }

  @Test
  public void getStringWithTwoBytesCharacter_ProperValueGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    String expected = new String("あいうえお");
    TextValue value = new TextValue(ANY_NAME, expected);

    // Act
    Optional<String> actual = value.getString();

    // Assert
    assertThat(expected.equals(actual.get())).isTrue();
    assertThat(expected == actual.get()).isFalse();
  }

  @Test
  public void equals_DifferentObjectsSameValuesGiven_ShouldReturnTrue() {
    // Arrange
    TextValue oneValue = new TextValue(ANY_NAME, "some_text");
    TextValue anotherValue = new TextValue(ANY_NAME, "some_text");

    // Act
    boolean result = oneValue.equals(anotherValue);

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void equals_DifferentObjectsSameValuesDifferentNamesGiven_ShouldReturnFalse() {
    // Arrange
    TextValue oneValue = new TextValue(ANY_NAME, "some_text");
    TextValue anotherValue = new TextValue(ANOTHER_NAME, "some_text");

    // Act
    boolean result = oneValue.equals(anotherValue);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void equals_SameObjectsGiven_ShouldReturnTrue() {
    // Arrange
    TextValue value = new TextValue(ANY_NAME, "some_text");

    // Act
    boolean result = value.equals(value);

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void equals_DifferentObjectsDifferentValuesGiven_ShouldReturnFalse() {
    // Arrange
    TextValue oneValue = new TextValue(ANY_NAME, "some_text");
    TextValue anotherValue = new TextValue(ANY_NAME, "another_text");

    // Act
    boolean result = oneValue.equals(anotherValue);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void equals_DifferentTypesSameValuesGiven_ShouldReturnFalse() {
    // Arrange
    TextValue oneValue = new TextValue(ANY_NAME, "some_text");
    BlobValue anotherValue = new BlobValue(ANY_NAME, "some_text".getBytes(StandardCharsets.UTF_8));

    // Act
    boolean result = oneValue.equals(anotherValue);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void compareTo_ThisBiggerThanGiven_ShouldReturnPositive() {
    // Arrange
    TextValue oneValue = new TextValue(ANY_NAME, "some_value2");
    TextValue anotherValue = new TextValue(ANY_NAME, "some_value1");

    // Act
    int actual = oneValue.compareTo(anotherValue);

    // Assert
    assertThat(actual > 0).isTrue();
  }

  @Test
  public void compareTo_ThisEqualsToGiven_ShouldReturnZero() {
    // Arrange
    TextValue oneValue = new TextValue(ANY_NAME, "some_value");
    TextValue anotherValue = new TextValue(ANY_NAME, "some_value");

    // Act
    int actual = oneValue.compareTo(anotherValue);

    // Assert
    assertThat(actual == 0).isTrue();
  }

  @Test
  public void compareTo_ThisSmallerThanGiven_ShouldReturnNegative() {
    // Arrange
    TextValue oneValue = new TextValue(ANY_NAME, "some_value1");
    TextValue anotherValue = new TextValue(ANY_NAME, "some_value2");

    // Act
    int actual = oneValue.compareTo(anotherValue);

    // Assert
    assertThat(actual < 0).isTrue();
  }

  @Test
  public void compareTo_ThisNonNullAndGivenNull_ShouldReturnPositive() {
    // Arrange
    TextValue oneValue = new TextValue(ANY_NAME, "some_value");
    TextValue anotherValue = new TextValue(ANY_NAME, (byte[]) null);

    // Act
    int actual = oneValue.compareTo(anotherValue);

    // Assert
    assertThat(actual > 0).isTrue();
  }

  @Test
  public void compareTo_ThisNullAndGivenNonNull_ShouldReturnNegative() {
    // Arrange
    TextValue oneValue = new TextValue(ANY_NAME, (byte[]) null);
    TextValue anotherValue = new TextValue(ANY_NAME, "some_value");

    // Act
    int actual = oneValue.compareTo(anotherValue);

    // Assert
    assertThat(actual < 0).isTrue();
  }

  @Test
  public void compareTo_ThisAndGivenAreNull_ShouldReturnZero() {
    // Arrange
    TextValue oneValue = new TextValue(ANY_NAME, (byte[]) null);
    TextValue anotherValue = new TextValue(ANY_NAME, (byte[]) null);

    // Act
    int actual = oneValue.compareTo(anotherValue);

    // Assert
    assertThat(actual == 0).isTrue();
  }

  @Test
  public void constructor_NullGiven_ShouldThrowNullPointerException() {
    // Act Assert
    assertThatThrownBy(
            () -> {
              new TextValue(null, (String) null);
            })
        .isInstanceOf(NullPointerException.class);
  }
}
