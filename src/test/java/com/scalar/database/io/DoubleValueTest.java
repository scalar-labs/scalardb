package com.scalar.database.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

/** */
public class DoubleValueTest {
  private static final String ANY_NAME = "name";
  private static final String ANOTHER_NAME = "another_name";

  @Test
  public void get_ProperValueGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    double expected = Double.MAX_VALUE;
    DoubleValue value = new DoubleValue(ANY_NAME, expected);

    // Act
    double actual = value.get();

    // Assert
    assertThat(expected).isEqualTo(actual);
  }

  @Test
  public void equals_DifferentObjectsSameValuesGiven_ShouldReturnTrue() {
    // Arrange
    double some = Double.MAX_VALUE;
    DoubleValue one = new DoubleValue(ANY_NAME, some);
    DoubleValue another = new DoubleValue(ANY_NAME, some);

    // Act
    boolean result = one.equals(another);

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void equals_DifferentObjectsSameValuesDifferentNamesGiven_ShouldReturnFalse() {
    // Arrange
    double some = Double.MAX_VALUE;
    DoubleValue one = new DoubleValue(ANY_NAME, some);
    DoubleValue another = new DoubleValue(ANOTHER_NAME, some);

    // Act
    boolean result = one.equals(another);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void equals_SameObjectsGiven_ShouldReturnTrue() {
    // Arrange
    double some = Double.MAX_VALUE;
    DoubleValue value = new DoubleValue(ANY_NAME, some);

    // Act
    boolean result = value.equals(value);

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void equals_DifferentObjectsDifferentValuesGiven_ShouldReturnFalse() {
    // Arrange
    double one = Double.MAX_VALUE;
    double another = Double.MAX_VALUE / 2.0;
    DoubleValue oneValue = new DoubleValue(ANY_NAME, one);
    DoubleValue anotherValue = new DoubleValue(ANY_NAME, another);

    // Act
    boolean result = oneValue.equals(anotherValue);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void equals_DifferentTypesSameValuesGiven_ShouldReturnFalse() {
    // Arrange
    double some = 1.0;
    DoubleValue oneValue = new DoubleValue(ANY_NAME, some);
    FloatValue anotherValue = new FloatValue(ANY_NAME, (float) some);

    // Act
    boolean result = oneValue.equals(anotherValue);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void compareTo_ThisBiggerThanGiven_ShouldReturnPositive() {
    // Arrange
    double one = Double.MAX_VALUE;
    double another = Double.MAX_VALUE / 2.0;
    DoubleValue oneValue = new DoubleValue(ANY_NAME, one);
    DoubleValue anotherValue = new DoubleValue(ANY_NAME, another);

    // Act
    int actual = oneValue.compareTo(anotherValue);

    // Assert
    assertThat(actual > 0).isTrue();
  }

  @Test
  public void compareTo_ThisEqualsToGiven_ShouldReturnZero() {
    // Arrange
    double one = Double.MAX_VALUE;
    double another = Double.MAX_VALUE;
    DoubleValue oneValue = new DoubleValue(ANY_NAME, one);
    DoubleValue anotherValue = new DoubleValue(ANY_NAME, another);

    // Act
    int actual = oneValue.compareTo(anotherValue);

    // Assert
    assertThat(actual == 0).isTrue();
  }

  @Test
  public void compareTo_ThisSmallerThanGiven_ShouldReturnNegative() {
    // Arrange
    double one = Double.MAX_VALUE / 2.0;
    double another = Double.MAX_VALUE;
    DoubleValue oneValue = new DoubleValue(ANY_NAME, one);
    DoubleValue anotherValue = new DoubleValue(ANY_NAME, another);

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
              new DoubleValue(null, 1.0);
            })
        .isInstanceOf(NullPointerException.class);
  }
}
