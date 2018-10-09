package com.scalar.database.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

/** */
public class BigIntValueTest {
  private static final String ANY_NAME = "name";
  private static final String ANOTHER_NAME = "another_name";

  @Test
  public void get_ProperValueGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    long expected = Long.MAX_VALUE;
    BigIntValue value = new BigIntValue(ANY_NAME, expected);

    // Act
    long actual = value.get();

    // Assert
    assertThat(expected).isEqualTo(actual);
  }

  @Test
  public void equals_DifferentObjectsSameValuesGiven_ShouldReturnTrue() {
    // Arrange
    long some = Long.MAX_VALUE;
    BigIntValue one = new BigIntValue(ANY_NAME, some);
    BigIntValue another = new BigIntValue(ANY_NAME, some);

    // Act
    boolean result = one.equals(another);

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void equals_DifferentObjectsSameValuesDifferentNamesGiven_ShouldReturnFalse() {
    // Arrange
    long some = Long.MAX_VALUE;
    BigIntValue one = new BigIntValue(ANY_NAME, some);
    BigIntValue another = new BigIntValue(ANOTHER_NAME, some);

    // Act
    boolean result = one.equals(another);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void equals_SameObjectsGiven_ShouldReturnTrue() {
    // Arrange
    long some = Long.MAX_VALUE;
    BigIntValue value = new BigIntValue(ANY_NAME, some);

    // Act
    boolean result = value.equals(value);

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void equals_DifferentObjectsDifferentValuesGiven_ShouldReturnFalse() {
    // Arrange
    long one = Long.MAX_VALUE;
    long another = Long.MAX_VALUE - 1;
    BigIntValue oneValue = new BigIntValue(ANY_NAME, one);
    BigIntValue anotherValue = new BigIntValue(ANY_NAME, another);

    // Act
    boolean result = oneValue.equals(anotherValue);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void equals_DifferentTypesSameValuesGiven_ShouldReturnFalse() {
    // Arrange
    int some = Integer.MAX_VALUE;
    BigIntValue one = new BigIntValue(ANY_NAME, some);
    IntValue another = new IntValue(ANY_NAME, some);

    // Act
    boolean result = one.equals(another);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void compareTo_ThisBiggerThanGiven_ShouldReturnPositive() {
    // Arrange
    long one = Long.MAX_VALUE;
    long another = Long.MAX_VALUE - 1;
    BigIntValue oneValue = new BigIntValue(ANY_NAME, one);
    BigIntValue anotherValue = new BigIntValue(ANY_NAME, another);

    // Act
    int actual = oneValue.compareTo(anotherValue);

    // Assert
    assertThat(actual > 0).isTrue();
  }

  @Test
  public void compareTo_ThisEqualsToGiven_ShouldReturnZero() {
    // Arrange
    long one = Long.MAX_VALUE;
    long another = Long.MAX_VALUE;
    BigIntValue oneValue = new BigIntValue(ANY_NAME, one);
    BigIntValue anotherValue = new BigIntValue(ANY_NAME, another);

    // Act
    int actual = oneValue.compareTo(anotherValue);

    // Assert
    assertThat(actual == 0).isTrue();
  }

  @Test
  public void compareTo_ThisSmallerThanGiven_ShouldReturnNegative() {
    // Arrange
    long one = Long.MAX_VALUE - 1;
    long another = Long.MAX_VALUE;
    BigIntValue oneValue = new BigIntValue(ANY_NAME, one);
    BigIntValue anotherValue = new BigIntValue(ANY_NAME, another);

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
              new BigIntValue(null, 0);
            })
        .isInstanceOf(NullPointerException.class);
  }
}
