package com.scalar.db.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

public class BooleanValueTest {
  private static final String ANY_NAME = "name";
  private static final String ANOTHER_NAME = "another_name";

  @Test
  public void get_ProperValueGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    boolean expected = Boolean.TRUE;
    BooleanValue value = new BooleanValue(ANY_NAME, expected);

    // Act
    boolean actual = value.get();

    // Assert
    assertThat(expected).isEqualTo(actual);
  }

  @Test
  public void getAsBoolean_ProperValueGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    boolean expected = Boolean.TRUE;
    Value<?> value = new BooleanValue(ANY_NAME, expected);

    // Act
    boolean actual = value.getAsBoolean();

    // Assert
    assertThat(expected).isEqualTo(actual);
  }

  @Test
  public void getAsInt_ProperValueGivenInConstructor_ShouldThrowUnsupportedOperationException() {
    // Arrange
    boolean expected = Boolean.TRUE;
    Value<?> value = new BooleanValue(ANY_NAME, expected);

    // Act Assert
    assertThatThrownBy(value::getAsInt).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void getAsLong_ProperValueGivenInConstructor_ShouldThrowUnsupportedOperationException() {
    // Arrange
    boolean expected = Boolean.TRUE;
    Value<?> value = new BooleanValue(ANY_NAME, expected);

    // Act Assert
    assertThatThrownBy(value::getAsLong).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void getAsFloat_ProperValueGivenInConstructor_ShouldThrowUnsupportedOperationException() {
    // Arrange
    boolean expected = Boolean.TRUE;
    Value<?> value = new BooleanValue(ANY_NAME, expected);

    // Act Assert
    assertThatThrownBy(value::getAsFloat).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void getAsDouble_ProperValueGivenInConstructor_ShouldThrowUnsupportedOperationException() {
    // Arrange
    boolean expected = Boolean.TRUE;
    Value<?> value = new BooleanValue(ANY_NAME, expected);

    // Act Assert
    assertThatThrownBy(value::getAsDouble).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void getAsString_ProperValueGivenInConstructor_ShouldThrowUnsupportedOperationException() {
    // Arrange
    boolean expected = Boolean.TRUE;
    Value<?> value = new BooleanValue(ANY_NAME, expected);

    // Act Assert
    assertThatThrownBy(value::getAsString).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void getAsBytes_ProperValueGivenInConstructor_ShouldThrowUnsupportedOperationException() {
    // Arrange
    boolean expected = Boolean.TRUE;
    Value<?> value = new BooleanValue(ANY_NAME, expected);

    // Act Assert
    assertThatThrownBy(value::getAsBytes).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void
      getAsByteBuffer_ProperValueGivenInConstructor_ShouldThrowUnsupportedOperationException() {
    // Arrange
    boolean expected = Boolean.TRUE;
    Value<?> value = new BooleanValue(ANY_NAME, expected);

    // Act Assert
    assertThatThrownBy(value::getAsByteBuffer).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void equals_DifferentObjectsSameValuesGiven_ShouldReturnTrue() {
    // Arrange
    boolean some = Boolean.TRUE;
    BooleanValue one = new BooleanValue(ANY_NAME, some);
    BooleanValue another = new BooleanValue(ANY_NAME, some);

    // Act
    boolean result = one.equals(another);

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void equals_DifferentObjectsSameValuesDifferentNamesGiven_ShouldReturnFalse() {
    // Arrange
    boolean some = Boolean.TRUE;
    BooleanValue one = new BooleanValue(ANY_NAME, some);
    BooleanValue another = new BooleanValue(ANOTHER_NAME, some);

    // Act
    boolean result = one.equals(another);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void equals_SameObjectsGiven_ShouldReturnTrue() {
    // Arrange
    boolean some = Boolean.TRUE;
    BooleanValue value = new BooleanValue(ANY_NAME, some);

    // Act
    @SuppressWarnings("SelfEquals")
    boolean result = value.equals(value);

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void equals_DifferentObjectsDifferentValuesGiven_ShouldReturnFalse() {
    // Arrange
    boolean one = Boolean.TRUE;
    boolean another = Boolean.FALSE;
    BooleanValue oneValue = new BooleanValue(ANY_NAME, one);
    BooleanValue anotherValue = new BooleanValue(ANY_NAME, another);

    // Act
    boolean result = oneValue.equals(anotherValue);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void equals_DifferentTypesSameValuesGiven_ShouldReturnFalse() {
    // Arrange
    boolean some = Boolean.TRUE;
    BooleanValue one = new BooleanValue(ANY_NAME, some);
    Boolean another = some;

    // Act
    @SuppressWarnings("EqualsIncompatibleType")
    boolean result = one.equals(another);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void compareTo_ThisBiggerThanGiven_ShouldReturnPositive() {
    // Arrange
    boolean one = Boolean.TRUE;
    boolean another = Boolean.FALSE;
    BooleanValue oneValue = new BooleanValue(ANY_NAME, one);
    BooleanValue anotherValue = new BooleanValue(ANY_NAME, another);

    // Act
    int actual = oneValue.compareTo(anotherValue);

    // Assert
    assertThat(actual > 0).isTrue();
  }

  @Test
  public void compareTo_ThisEqualsToGiven_ShouldReturnZero() {
    // Arrange
    boolean one = Boolean.TRUE;
    boolean another = Boolean.TRUE;
    BooleanValue oneValue = new BooleanValue(ANY_NAME, one);
    BooleanValue anotherValue = new BooleanValue(ANY_NAME, another);

    // Act
    int actual = oneValue.compareTo(anotherValue);

    // Assert
    assertThat(actual == 0).isTrue();
  }

  @Test
  public void compareTo_ThisSmallerThanGiven_ShouldReturnNegative() {
    // Arrange
    boolean one = Boolean.FALSE;
    boolean another = Boolean.TRUE;
    BooleanValue oneValue = new BooleanValue(ANY_NAME, one);
    BooleanValue anotherValue = new BooleanValue(ANY_NAME, another);

    // Act
    int actual = oneValue.compareTo(anotherValue);

    // Assert
    assertThat(actual < 0).isTrue();
  }

  @Test
  public void constructor_NullGiven_ShouldThrowNullPointerException() {
    // Act Assert
    assertThatThrownBy(() -> new BooleanValue(null, true)).isInstanceOf(NullPointerException.class);
  }
}
