package com.scalar.db.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.junit.Test;

public class TextValueTest {
  private static final String ANY_NAME = "name";
  private static final String ANOTHER_NAME = "another_name";

  @Test
  public void get_ProperValueGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    String expected = "some_text";
    TextValue value = new TextValue(ANY_NAME, expected);

    // Act
    Optional<String> actual = value.get();

    // Assert
    assertThat(actual.isPresent()).isTrue();
    assertThat(expected.equals(actual.get())).isTrue();
  }

  @Test
  public void getAsString_ProperValueGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    String expected = "some_text";
    Value<?> value = new TextValue(ANY_NAME, expected);

    // Act
    Optional<String> actual = value.getAsString();

    // Assert
    assertThat(actual.isPresent()).isTrue();
    assertThat(expected.equals(actual.get())).isTrue();
  }

  @Test
  public void getAsBytes_ProperValueGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    String expected = "some_text";
    Value<?> value = new TextValue(ANY_NAME, expected);

    // Act
    Optional<byte[]> actual = value.getAsBytes();

    // Assert
    assertThat(actual.isPresent()).isTrue();
    assertThat(new String(actual.get(), StandardCharsets.UTF_8)).isEqualTo(expected);
  }

  @Test
  public void getAsByteBuffer_ProperValueGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    String expected = "some_text";
    Value<?> value = new TextValue(ANY_NAME, expected);

    // Act
    Optional<ByteBuffer> actual = value.getAsByteBuffer();

    // Assert
    assertThat(actual.isPresent()).isTrue();
    assertThat(new String(actual.get().array(), StandardCharsets.UTF_8)).isEqualTo(expected);
  }

  @Test
  public void
      getAsBoolean_ProperValueGivenInConstructor_ShouldThrowUnsupportedOperationException() {
    // Arrange
    String expected = "some_text";
    Value<?> value = new TextValue(ANY_NAME, expected);

    // Act Assert
    assertThatThrownBy(value::getAsBoolean).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void getAsInt_ProperValueGivenInConstructor_ShouldThrowUnsupportedOperationException() {
    // Arrange
    String expected = "some_text";
    Value<?> value = new TextValue(ANY_NAME, expected);

    // Act Assert
    assertThatThrownBy(value::getAsInt).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void getAsLong_ProperValueGivenInConstructor_ShouldThrowUnsupportedOperationException() {
    // Arrange
    String expected = "some_text";
    Value<?> value = new TextValue(ANY_NAME, expected);

    // Act Assert
    assertThatThrownBy(value::getAsLong).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void getAsFloat_ProperValueGivenInConstructor_ShouldThrowUnsupportedOperationException() {
    // Arrange
    String expected = "some_text";
    Value<?> value = new TextValue(ANY_NAME, expected);

    // Act Assert
    assertThatThrownBy(value::getAsFloat).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void getAsDouble_ProperValueGivenInConstructor_ShouldThrowUnsupportedOperationException() {
    // Arrange
    String expected = "some_text";
    Value<?> value = new TextValue(ANY_NAME, expected);

    // Act Assert
    assertThatThrownBy(value::getAsDouble).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void getWithTwoBytesCharacter_ProperValueGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    String expected = "あいうえお";
    TextValue value = new TextValue(ANY_NAME, expected);

    // Act
    Optional<String> actual = value.get();

    // Assert
    assertThat(expected.equals(actual.get())).isTrue();
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
    @SuppressWarnings("SelfEquals")
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
  @SuppressFBWarnings("EC_UNRELATED_TYPES")
  public void equals_DifferentTypesSameValuesGiven_ShouldReturnFalse() {
    // Arrange
    TextValue oneValue = new TextValue(ANY_NAME, "some_text");
    BlobValue anotherValue = new BlobValue(ANY_NAME, "some_text".getBytes(StandardCharsets.UTF_8));

    // Act
    @SuppressWarnings("EqualsIncompatibleType")
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
    assertThatThrownBy(() -> new TextValue(null, (String) null))
        .isInstanceOf(NullPointerException.class);
  }
}
