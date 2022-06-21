package com.scalar.db.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

public class FloatValueTest {
  private static final String ANY_NAME = "name";
  private static final String ANOTHER_NAME = "another_name";

  @Test
  public void get_ProperValueGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    float expected = Float.MAX_VALUE;
    FloatValue value = new FloatValue(ANY_NAME, expected);

    // Act
    float actual = value.get();

    // Assert
    assertThat(expected).isEqualTo(actual);
  }

  @Test
  public void getAsFloat_ProperValueGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    float expected = Float.MAX_VALUE;
    Value<?> value = new FloatValue(ANY_NAME, expected);

    // Act
    float actual = value.getAsFloat();

    // Assert
    assertThat(expected).isEqualTo(actual);
  }

  @Test
  public void getAsDouble_ProperValueGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    float expected = Float.MAX_VALUE;
    Value<?> value = new FloatValue(ANY_NAME, expected);

    // Act
    double actual = value.getAsDouble();

    // Assert
    assertThat((double) expected).isEqualTo(actual);
  }

  @Test
  public void
      getAsBoolean_ProperValueGivenInConstructor_ShouldThrowUnsupportedOperationException() {
    // Arrange
    float expected = Float.MAX_VALUE;
    Value<?> value = new FloatValue(ANY_NAME, expected);

    // Act Assert
    assertThatThrownBy(value::getAsBoolean).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void getAsInt_ProperValueGivenInConstructor_ShouldThrowUnsupportedOperationException() {
    // Arrange
    float expected = Float.MAX_VALUE;
    Value<?> value = new FloatValue(ANY_NAME, expected);

    // Act Assert
    assertThatThrownBy(value::getAsInt).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void getAsLong_ProperValueGivenInConstructor_ShouldThrowUnsupportedOperationException() {
    // Arrange
    float expected = Float.MAX_VALUE;
    Value<?> value = new FloatValue(ANY_NAME, expected);

    // Act Assert
    assertThatThrownBy(value::getAsLong).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void getAsString_ProperValueGivenInConstructor_ShouldThrowUnsupportedOperationException() {
    // Arrange
    float expected = Float.MAX_VALUE;
    Value<?> value = new FloatValue(ANY_NAME, expected);

    // Act Assert
    assertThatThrownBy(value::getAsString).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void getAsBytes_ProperValueGivenInConstructor_ShouldThrowUnsupportedOperationException() {
    // Arrange
    float expected = Float.MAX_VALUE;
    Value<?> value = new FloatValue(ANY_NAME, expected);

    // Act Assert
    assertThatThrownBy(value::getAsBytes).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void
      getAsByteBuffer_ProperValueGivenInConstructor_ShouldThrowUnsupportedOperationException() {
    // Arrange
    float expected = Float.MAX_VALUE;
    Value<?> value = new FloatValue(ANY_NAME, expected);

    // Act Assert
    assertThatThrownBy(value::getAsByteBuffer).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void equals_DifferentObjectsSameValuesGiven_ShouldReturnTrue() {
    // Arrange
    float some = Float.MAX_VALUE;
    FloatValue one = new FloatValue(ANY_NAME, some);
    FloatValue another = new FloatValue(ANY_NAME, some);

    // Act
    boolean result = one.equals(another);

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void equals_DifferentObjectsSameValuesDifferentNamesGiven_ShouldReturnFalse() {
    // Arrange
    float some = Float.MAX_VALUE;
    FloatValue one = new FloatValue(ANY_NAME, some);
    FloatValue another = new FloatValue(ANOTHER_NAME, some);

    // Act
    boolean result = one.equals(another);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void equals_SameObjectsGiven_ShouldReturnTrue() {
    // Arrange
    float some = Float.MAX_VALUE;
    FloatValue value = new FloatValue(ANY_NAME, some);

    // Act
    @SuppressWarnings("SelfEquals")
    boolean result = value.equals(value);

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void equals_DifferentObjectsDifferentValuesGiven_ShouldReturnFalse() {
    // Arrange
    float one = Float.MAX_VALUE;
    float another = Float.MAX_VALUE / 2.0f;
    FloatValue oneValue = new FloatValue(ANY_NAME, one);
    FloatValue anotherValue = new FloatValue(ANY_NAME, another);

    // Act
    boolean result = oneValue.equals(anotherValue);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void equals_DifferentTypesSameValuesGiven_ShouldReturnFalse() {
    // Arrange
    float some = Float.MAX_VALUE;
    FloatValue oneValue = new FloatValue(ANY_NAME, some);
    DoubleValue anotherValue = new DoubleValue(ANY_NAME, some);

    // Act
    @SuppressWarnings("EqualsIncompatibleType")
    boolean result = oneValue.equals(anotherValue);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void compareTo_ThisBiggerThanGiven_ShouldReturnPositive() {
    // Arrange
    float one = Float.MAX_VALUE;
    float another = Float.MAX_VALUE / 2.0f;
    FloatValue oneValue = new FloatValue(ANY_NAME, one);
    FloatValue anotherValue = new FloatValue(ANY_NAME, another);

    // Act
    int actual = oneValue.compareTo(anotherValue);

    // Assert
    assertThat(actual > 0).isTrue();
  }

  @Test
  public void compareTo_ThisEqualsToGiven_ShouldReturnZero() {
    // Arrange
    float one = Float.MAX_VALUE;
    float another = Float.MAX_VALUE;
    FloatValue oneValue = new FloatValue(ANY_NAME, one);
    FloatValue anotherValue = new FloatValue(ANY_NAME, another);

    // Act
    int actual = oneValue.compareTo(anotherValue);

    // Assert
    assertThat(actual == 0).isTrue();
  }

  @Test
  public void compareTo_ThisSmallerThanGiven_ShouldReturnNegative() {
    // Arrange
    float one = Float.MAX_VALUE / 2.0f;
    float another = Float.MAX_VALUE;
    FloatValue oneValue = new FloatValue(ANY_NAME, one);
    FloatValue anotherValue = new FloatValue(ANY_NAME, another);

    // Act
    int actual = oneValue.compareTo(anotherValue);

    // Assert
    assertThat(actual < 0).isTrue();
  }

  @Test
  public void constructor_NullGiven_ShouldThrowNullPointerException() {
    // Act Assert
    assertThatThrownBy(() -> new FloatValue(null, 1.0f)).isInstanceOf(NullPointerException.class);
  }
}
