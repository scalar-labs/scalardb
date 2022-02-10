package com.scalar.db.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import org.junit.Test;

public class BlobValueTest {
  private static final String ANY_NAME = "name";
  private static final String ANOTHER_NAME = "another_name";
  private static final String SOME_TEXT = "some_text";
  private static final byte[] SOME_TEXT_BYTES = SOME_TEXT.getBytes(StandardCharsets.UTF_8);

  @Test
  public void get_ProperByteArrayValueGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    byte[] expected = SOME_TEXT_BYTES;
    BlobValue value = new BlobValue(ANY_NAME, expected);

    // Act
    Optional<byte[]> actual = value.get();

    // Assert
    assertThat(actual.isPresent()).isTrue();
    assertThat(Arrays.equals(expected, actual.get())).isTrue();
    assertThat(expected == actual.get()).isFalse();
  }

  @Test
  public void get_ProperByteBufferValueGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    byte[] expected = SOME_TEXT_BYTES;
    BlobValue value = new BlobValue(ANY_NAME, ByteBuffer.wrap(expected));

    // Act
    Optional<byte[]> actual = value.get();

    // Assert
    assertThat(actual.isPresent()).isTrue();
    assertThat(Arrays.equals(expected, actual.get())).isTrue();
    assertThat(expected == actual.get()).isFalse();
  }

  @Test
  public void getAsBytes_ProperValueGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    byte[] expected = SOME_TEXT_BYTES;
    Value<?> value = new BlobValue(ANY_NAME, expected);

    // Act
    Optional<byte[]> actual = value.getAsBytes();

    // Assert
    assertThat(actual.isPresent()).isTrue();
    assertThat(Arrays.equals(expected, actual.get())).isTrue();
    assertThat(expected == actual.get()).isFalse();
  }

  @SuppressWarnings("ReferenceEquality")
  @Test
  public void getAsByteBuffer_ProperValueGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    ByteBuffer expected = ByteBuffer.wrap(SOME_TEXT_BYTES);
    Value<?> value = new BlobValue(ANY_NAME, expected);
    expected.clear();

    // Act
    Optional<ByteBuffer> actual = value.getAsByteBuffer();

    // Assert
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get()).isEqualTo(expected);
    assertThat(expected == actual.get()).isFalse();
  }

  @Test
  public void
      getAsBoolean_ProperValueGivenInConstructor_ShouldThrowUnsupportedOperationException() {
    // Arrange
    byte[] expected = SOME_TEXT_BYTES;
    Value<?> value = new BlobValue(ANY_NAME, expected);

    // Act Assert
    assertThatThrownBy(value::getAsBoolean).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void getAsInt_ProperValueGivenInConstructor_ShouldThrowUnsupportedOperationException() {
    // Arrange
    byte[] expected = SOME_TEXT_BYTES;
    Value<?> value = new BlobValue(ANY_NAME, expected);

    // Act Assert
    assertThatThrownBy(value::getAsInt).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void getAsLong_ProperValueGivenInConstructor_ShouldThrowUnsupportedOperationException() {
    // Arrange
    byte[] expected = SOME_TEXT_BYTES;
    Value<?> value = new BlobValue(ANY_NAME, expected);

    // Act Assert
    assertThatThrownBy(value::getAsLong).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void getAsFloat_ProperValueGivenInConstructor_ShouldThrowUnsupportedOperationException() {
    // Arrange
    byte[] expected = SOME_TEXT_BYTES;
    Value<?> value = new BlobValue(ANY_NAME, expected);

    // Act Assert
    assertThatThrownBy(value::getAsFloat).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void getAsDouble_ProperValueGivenInConstructor_ShouldThrowUnsupportedOperationException() {
    // Arrange
    byte[] expected = SOME_TEXT_BYTES;
    Value<?> value = new BlobValue(ANY_NAME, expected);

    // Act Assert
    assertThatThrownBy(value::getAsDouble).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void getAsString_ProperValueGivenInConstructor_ShouldThrowUnsupportedOperationException() {
    // Arrange
    byte[] expected = SOME_TEXT_BYTES;
    Value<?> value = new BlobValue(ANY_NAME, expected);

    // Act Assert
    assertThatThrownBy(value::getAsString).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void copyWith_WithValuePresent_ShouldReturnNewBlobWithSameValue() {
    // Arrange
    BlobValue oneValue = new BlobValue(ANY_NAME, SOME_TEXT_BYTES);

    // Act
    BlobValue newValue = oneValue.copyWith("new name");

    // Assert
    assertThat(oneValue.get().get()).isEqualTo(newValue.get().get());
  }

  @Test
  public void copyWith_WithValueEmpty_ShouldReturnNewBlobWithValueEmpty() {
    // Arrange
    BlobValue oneValue = new BlobValue(ANY_NAME, (byte[]) null);

    // Act
    BlobValue newValue = oneValue.copyWith("new name");

    // Assert
    assertThat(newValue.get()).isEmpty();
  }

  @Test
  public void equals_DifferentObjectsSameValuesGiven_ShouldReturnTrue() {
    // Arrange
    BlobValue oneValue = new BlobValue(ANY_NAME, SOME_TEXT_BYTES);
    BlobValue anotherValue = new BlobValue(ANY_NAME, SOME_TEXT.getBytes(StandardCharsets.UTF_8));

    // Act
    boolean result = oneValue.equals(anotherValue);

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void equals_DifferentObjectsSameValuesDifferentNamesGiven_ShouldReturnFalse() {
    // Arrange
    BlobValue oneValue = new BlobValue(ANY_NAME, SOME_TEXT_BYTES);
    BlobValue anotherValue =
        new BlobValue(ANOTHER_NAME, SOME_TEXT.getBytes(StandardCharsets.UTF_8));

    // Act
    boolean result = oneValue.equals(anotherValue);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void equals_SameObjectsGiven_ShouldReturnTrue() {
    // Arrange
    BlobValue value = new BlobValue(ANY_NAME, SOME_TEXT_BYTES);

    // Act
    @SuppressWarnings("SelfEquals")
    boolean result = value.equals(value);

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void equals_DifferentObjectsDifferentValuesGiven_ShouldReturnFalse() {
    // Arrange
    BlobValue oneValue = new BlobValue(ANY_NAME, SOME_TEXT_BYTES);
    BlobValue anotherValue =
        new BlobValue(ANY_NAME, "another_text".getBytes(StandardCharsets.UTF_8));

    // Act
    boolean result = oneValue.equals(anotherValue);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  @SuppressFBWarnings("EC_UNRELATED_TYPES")
  public void equals_DifferentTypesSameValuesGiven_ShouldReturnFalse() {
    // Arrange
    BlobValue oneValue = new BlobValue(ANY_NAME, SOME_TEXT_BYTES);
    TextValue anotherValue = new TextValue(ANY_NAME, SOME_TEXT);

    // Act
    @SuppressWarnings("EqualsIncompatibleType")
    boolean result = oneValue.equals(anotherValue);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void compareTo_ThisBiggerThanGiven_ShouldReturnPositive() {
    // Arrange
    BlobValue oneValue = new BlobValue(ANY_NAME, "some_value2".getBytes(StandardCharsets.UTF_8));
    BlobValue anotherValue =
        new BlobValue(ANY_NAME, "some_value1".getBytes(StandardCharsets.UTF_8));

    // Act
    int actual = oneValue.compareTo(anotherValue);

    // Assert
    assertThat(actual > 0).isTrue();
  }

  @Test
  public void compareTo_ThisEqualsToGiven_ShouldReturnZero() {
    // Arrange
    BlobValue oneValue = new BlobValue(ANY_NAME, "some_value".getBytes(StandardCharsets.UTF_8));
    BlobValue anotherValue = new BlobValue(ANY_NAME, "some_value".getBytes(StandardCharsets.UTF_8));

    // Act
    int actual = oneValue.compareTo(anotherValue);

    // Assert
    assertThat(actual == 0).isTrue();
  }

  @Test
  public void compareTo_ThisSmallerThanGiven_ShouldReturnNegative() {
    // Arrange
    BlobValue oneValue = new BlobValue(ANY_NAME, "some_value1".getBytes(StandardCharsets.UTF_8));
    BlobValue anotherValue =
        new BlobValue(ANY_NAME, "some_value2".getBytes(StandardCharsets.UTF_8));

    // Act
    int actual = oneValue.compareTo(anotherValue);

    // Assert
    assertThat(actual < 0).isTrue();
  }

  @Test
  public void compareTo_ThisNonNullAndGivenNull_ShouldReturnPositive() {
    // Arrange
    BlobValue oneValue = new BlobValue(ANY_NAME, "some_value".getBytes(StandardCharsets.UTF_8));
    BlobValue anotherValue = new BlobValue(ANY_NAME, (byte[]) null);

    // Act
    int actual = oneValue.compareTo(anotherValue);

    // Assert
    assertThat(actual > 0).isTrue();
  }

  @Test
  public void compareTo_ThisNullAndGivenNonNull_ShouldReturnNegative() {
    // Arrange
    BlobValue oneValue = new BlobValue(ANY_NAME, (byte[]) null);
    BlobValue anotherValue = new BlobValue(ANY_NAME, "some_value".getBytes(StandardCharsets.UTF_8));

    // Act
    int actual = oneValue.compareTo(anotherValue);

    // Assert
    assertThat(actual < 0).isTrue();
  }

  @Test
  public void compareTo_ThisAndGivenAreNull_ShouldReturnZero() {
    // Arrange
    BlobValue oneValue = new BlobValue(ANY_NAME, (byte[]) null);
    BlobValue anotherValue = new BlobValue(ANY_NAME, (byte[]) null);

    // Act
    int actual = oneValue.compareTo(anotherValue);

    // Assert
    assertThat(actual == 0).isTrue();
  }

  @Test
  public void constructor_NullGiven_ShouldThrowNullPointerException() {
    // Act Assert
    assertThatThrownBy(() -> new BlobValue(null, (byte[]) null))
        .isInstanceOf(NullPointerException.class);
  }
}
