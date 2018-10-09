package com.scalar.database.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

/** */
public class IntValueTest {
  private static final String ANY_NAME = "name";
  private static final String ANOTHER_NAME = "another_name";

  @Test
  public void get_ProperValueGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    int expected = Integer.MAX_VALUE;
    IntValue value = new IntValue(ANY_NAME, expected);

    // Act
    int actual = value.get();

    // Assert
    assertThat(expected).isEqualTo(actual);
  }

  @Test
  public void equals_DifferentObjectsSameValuesGiven_ShouldReturnTrue() {
    // Arrange
    int some = Integer.MAX_VALUE;
    IntValue one = new IntValue(ANY_NAME, some);
    IntValue another = new IntValue(ANY_NAME, some);

    // Act
    boolean result = one.equals(another);

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void equals_DifferentObjectsSameValuesDifferentNamesGiven_ShouldReturnFalse() {
    // Arrange
    int some = Integer.MAX_VALUE;
    IntValue one = new IntValue(ANY_NAME, some);
    IntValue another = new IntValue(ANOTHER_NAME, some);

    // Act
    boolean result = one.equals(another);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void equals_SameObjectsGiven_ShouldReturnTrue() {
    // Arrange
    int some = Integer.MAX_VALUE;
    IntValue value = new IntValue(ANY_NAME, some);

    // Act
    boolean result = value.equals(value);

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void equals_DifferentObjectsDifferentValuesGiven_ShouldReturnFalse() {
    // Arrange
    int one = Integer.MAX_VALUE;
    int another = Integer.MAX_VALUE - 1;
    IntValue oneValue = new IntValue(ANY_NAME, one);
    IntValue anotherValue = new IntValue(ANY_NAME, another);

    // Act
    boolean result = oneValue.equals(anotherValue);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void equals_DifferentTypesSameValuesGiven_ShouldReturnFalse() {
    // Arrange
    int some = Integer.MAX_VALUE;
    IntValue one = new IntValue(ANY_NAME, some);
    BigIntValue another = new BigIntValue(ANY_NAME, some);

    // Act
    boolean result = one.equals(another);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void compareTo_ThisBiggerThanGiven_ShouldReturnPositive() {
    // Arrange
    int one = Integer.MAX_VALUE;
    int another = Integer.MAX_VALUE - 1;
    IntValue oneValue = new IntValue(ANY_NAME, one);
    IntValue anotherValue = new IntValue(ANY_NAME, another);

    // Act
    int actual = oneValue.compareTo(anotherValue);

    // Assert
    assertThat(actual > 0).isTrue();
  }

  @Test
  public void compareTo_ThisEqualsToGiven_ShouldReturnZero() {
    // Arrange
    int one = Integer.MAX_VALUE;
    int another = Integer.MAX_VALUE;
    IntValue oneValue = new IntValue(ANY_NAME, one);
    IntValue anotherValue = new IntValue(ANY_NAME, another);

    // Act
    int actual = oneValue.compareTo(anotherValue);

    // Assert
    assertThat(actual == 0).isTrue();
  }

  @Test
  public void compareTo_ThisSmallerThanGiven_ShouldReturnNegative() {
    // Arrange
    int one = Integer.MAX_VALUE - 1;
    int another = Integer.MAX_VALUE;
    IntValue oneValue = new IntValue(ANY_NAME, one);
    IntValue anotherValue = new IntValue(ANY_NAME, another);

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
              new IntValue(null, 1);
            })
        .isInstanceOf(NullPointerException.class);
  }
}
