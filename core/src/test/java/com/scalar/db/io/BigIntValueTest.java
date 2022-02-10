package com.scalar.db.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.Test;

public class BigIntValueTest {
  private static final String ANY_NAME = "name";
  private static final String ANOTHER_NAME = "another_name";

  @Test
  public void get_ProperValueGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    long expected = BigIntValue.MAX_VALUE;
    BigIntValue value = new BigIntValue(ANY_NAME, expected);

    // Act
    long actual = value.get();

    // Assert
    assertThat(expected).isEqualTo(actual);
  }

  @Test
  public void getAsLong_ProperValueGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    long expected = BigIntValue.MAX_VALUE;
    Value<?> value = new BigIntValue(ANY_NAME, expected);

    // Act
    long actual = value.getAsLong();

    // Assert
    assertThat(expected).isEqualTo(actual);
  }

  @Test
  public void getAsFloat_ProperValueGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    long expected = BigIntValue.MAX_VALUE;
    Value<?> value = new BigIntValue(ANY_NAME, expected);

    // Act
    float actual = value.getAsFloat();

    // Assert
    assertThat((float) expected).isEqualTo(actual);
  }

  @Test
  public void getAsDouble_ProperValueGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    long expected = BigIntValue.MAX_VALUE;
    Value<?> value = new BigIntValue(ANY_NAME, expected);

    // Act
    double actual = value.getAsDouble();

    // Assert
    assertThat((double) expected).isEqualTo(actual);
  }

  @Test
  public void
      getAsBoolean_ProperValueGivenInConstructor_ShouldThrowUnsupportedOperationException() {
    // Arrange
    long expected = BigIntValue.MAX_VALUE;
    Value<?> value = new BigIntValue(ANY_NAME, expected);

    // Act Assert
    assertThatThrownBy(value::getAsBoolean).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void getAsInt_ProperValueGivenInConstructor_ShouldThrowUnsupportedOperationException() {
    // Arrange
    long expected = BigIntValue.MAX_VALUE;
    Value<?> value = new BigIntValue(ANY_NAME, expected);

    // Act Assert
    assertThatThrownBy(value::getAsInt).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void getAsString_ProperValueGivenInConstructor_ShouldThrowUnsupportedOperationException() {
    // Arrange
    long expected = BigIntValue.MAX_VALUE;
    Value<?> value = new BigIntValue(ANY_NAME, expected);

    // Act Assert
    assertThatThrownBy(value::getAsString).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void getAsBytes_ProperValueGivenInConstructor_ShouldThrowUnsupportedOperationException() {
    // Arrange
    long expected = BigIntValue.MAX_VALUE;
    Value<?> value = new BigIntValue(ANY_NAME, expected);

    // Act Assert
    assertThatThrownBy(value::getAsBytes).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void
      getAsByteBuffer_ProperValueGivenInConstructor_ShouldThrowUnsupportedOperationException() {
    // Arrange
    long expected = BigIntValue.MAX_VALUE;
    Value<?> value = new BigIntValue(ANY_NAME, expected);

    // Act Assert
    assertThatThrownBy(value::getAsByteBuffer).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void equals_DifferentObjectsSameValuesGiven_ShouldReturnTrue() {
    // Arrange
    long some = BigIntValue.MAX_VALUE;
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
    long some = BigIntValue.MAX_VALUE;
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
    long some = BigIntValue.MAX_VALUE;
    BigIntValue value = new BigIntValue(ANY_NAME, some);

    // Act
    @SuppressWarnings("SelfEquals")
    boolean result = value.equals(value);

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void equals_DifferentObjectsDifferentValuesGiven_ShouldReturnFalse() {
    // Arrange
    long one = BigIntValue.MAX_VALUE;
    long another = BigIntValue.MAX_VALUE - 1;
    BigIntValue oneValue = new BigIntValue(ANY_NAME, one);
    BigIntValue anotherValue = new BigIntValue(ANY_NAME, another);

    // Act
    boolean result = oneValue.equals(anotherValue);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  @SuppressFBWarnings("EC_UNRELATED_TYPES")
  public void equals_DifferentTypesSameValuesGiven_ShouldReturnFalse() {
    // Arrange
    int some = Integer.MAX_VALUE;
    BigIntValue one = new BigIntValue(ANY_NAME, some);
    IntValue another = new IntValue(ANY_NAME, some);

    // Act
    @SuppressWarnings("EqualsIncompatibleType")
    boolean result = one.equals(another);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void compareTo_ThisBiggerThanGiven_ShouldReturnPositive() {
    // Arrange
    long one = BigIntValue.MAX_VALUE;
    long another = BigIntValue.MAX_VALUE - 1;
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
    long one = BigIntValue.MAX_VALUE;
    long another = BigIntValue.MAX_VALUE;
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
    long one = BigIntValue.MAX_VALUE - 1;
    long another = BigIntValue.MAX_VALUE;
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
    assertThatThrownBy(() -> new BigIntValue(null, 0)).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void constructor_NumberOverMaxValueGiven_ShouldThrowIllegalArgumentException() {
    // Act Assert
    assertThatThrownBy(() -> new BigIntValue(ANY_NAME, BigIntValue.MAX_VALUE + 1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void constructor_NumberUnderMinValueGiven_ShouldThrowIllegalArgumentException() {
    // Act Assert
    assertThatThrownBy(() -> new BigIntValue(ANY_NAME, BigIntValue.MIN_VALUE - 1))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
