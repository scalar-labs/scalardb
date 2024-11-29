package com.scalar.db.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.time.Instant;
import java.time.temporal.ChronoField;
import org.junit.jupiter.api.Test;

class TimestampTZColumnTest {
  private static final Instant ANY_TIMESTAMPTZ =
      TimestampTZColumn.MAX_VALUE.with(ChronoField.NANO_OF_SECOND, 123_000_000);

  @Test
  public void of_ProperValueGiven_ShouldReturnWhatsSet() {
    // Arrange

    // Act
    TimestampTZColumn column = TimestampTZColumn.of("col", ANY_TIMESTAMPTZ);

    // Assert
    assertThat(column.getName()).isEqualTo("col");
    assertThat(column.getValue()).isPresent();
    assertThat(column.getValue().get()).isEqualTo(ANY_TIMESTAMPTZ);
    assertThat(column.getTimestampTZValue()).isEqualTo(ANY_TIMESTAMPTZ);
    assertThat(column.getDataType()).isEqualTo(DataType.TIMESTAMPTZ);
    assertThat(column.hasNullValue()).isFalse();
    assertThat(column.getValueAsObject()).isEqualTo(ANY_TIMESTAMPTZ);
    assertThatThrownBy(column::getIntValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getBigIntValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getFloatValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getDoubleValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getTextValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getBlobValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getBlobValueAsByteBuffer)
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getBlobValueAsBytes)
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getDateValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getTimeValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getTimestampValue).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void ofNull_ProperValueGiven_ShouldReturnWhatsSet() {
    // Arrange

    // Act
    TimestampTZColumn column = TimestampTZColumn.ofNull("col");

    // Assert
    assertThat(column.getName()).isEqualTo("col");
    assertThat(column.getValue()).isNotPresent();
    assertThat(column.getTimestampTZValue()).isNull();
    assertThat(column.getDataType()).isEqualTo(DataType.TIMESTAMPTZ);
    assertThat(column.hasNullValue()).isTrue();
    assertThat(column.getValueAsObject()).isNull();
    assertThatThrownBy(column::getIntValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getBigIntValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getFloatValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getDoubleValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getTextValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getBlobValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getBlobValueAsByteBuffer)
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getBlobValueAsBytes)
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getDateValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getTimeValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getTimestampValue).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void copyWith_ProperValueGiven_ShouldReturnSameValueButDifferentName() {
    // Arrange

    // Act
    TimestampTZColumn column = TimestampTZColumn.of("col", ANY_TIMESTAMPTZ).copyWith("col2");

    // Assert
    assertThat(column.getName()).isEqualTo("col2");
    assertThat(column.getValue()).isPresent();
    assertThat(column.getValue().get()).isEqualTo(ANY_TIMESTAMPTZ);
    assertThat(column.getTimestampTZValue()).isEqualTo(ANY_TIMESTAMPTZ);
    assertThat(column.getDataType()).isEqualTo(DataType.TIMESTAMPTZ);
    assertThat(column.hasNullValue()).isFalse();
    assertThat(column.getValueAsObject()).isEqualTo(ANY_TIMESTAMPTZ);
    assertThatThrownBy(column::getIntValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getBigIntValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getFloatValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getDoubleValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getTextValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getBlobValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getBlobValueAsByteBuffer)
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getBlobValueAsBytes)
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getDateValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getTimeValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getTimestampValue).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void compareTo_ShouldReturnProperResults() {
    // Arrange
    TimestampTZColumn column = TimestampTZColumn.of("col", ANY_TIMESTAMPTZ);

    // Act Assert
    assertThat(column.compareTo(TimestampTZColumn.of("col", ANY_TIMESTAMPTZ))).isEqualTo(0);
    assertThat(column.compareTo(TimestampTZColumn.of("col", ANY_TIMESTAMPTZ.minusSeconds(1))))
        .isGreaterThan(0);
    assertThat(column.compareTo(TimestampTZColumn.of("col", ANY_TIMESTAMPTZ.plusNanos(1))))
        .isLessThan(0);
    assertThat(column.compareTo(TimestampTZColumn.ofNull("col"))).isGreaterThan(0);
  }

  @Test
  public void constructor_valueInsideRange_ShouldNotThrowException() {
    // Act Assert
    assertDoesNotThrow(() -> TimestampTZColumn.of("col", TimestampTZColumn.MIN_VALUE));
    assertDoesNotThrow(() -> TimestampTZColumn.of("col", TimestampTZColumn.MAX_VALUE));
  }

  @Test
  public void constructor_valueOutOfRange_ShouldThrowIllegalArgumentException() {
    // Arrange
    Instant dateBeforeRangeMin = TimestampTZColumn.MIN_VALUE.minusMillis(1);
    Instant dateAfterRangeMax = TimestampTZColumn.MAX_VALUE.plusMillis(1);

    // Act Assert
    assertThatThrownBy(() -> TimestampTZColumn.of("col", dateBeforeRangeMin))
        .isInstanceOf((IllegalArgumentException.class));
    assertThatThrownBy(() -> TimestampTZColumn.of("col", dateAfterRangeMax))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void constructor_valueWithSubMillisecondPrecision_ShouldThrowIllegalArgumentException() {
    // Arrange
    Instant timestampWithThreeDigitNano =
        Instant.now().with(ChronoField.NANO_OF_SECOND, 123_456_789);
    Instant timestampWithTwoDigitNano = Instant.now().with(ChronoField.NANO_OF_SECOND, 123_456_780);
    Instant timestampWithOneDigitNano = Instant.now().with(ChronoField.NANO_OF_SECOND, 123_456_700);
    Instant timestampWithThreeDigitMicro =
        Instant.now().with(ChronoField.NANO_OF_SECOND, 123_456_000);
    Instant timestampWithTwoDigitMicro =
        Instant.now().with(ChronoField.NANO_OF_SECOND, 123_450_000);
    Instant timestampWithOneDigitMicro =
        Instant.now().with(ChronoField.NANO_OF_SECOND, 123_400_700);

    // Act Assert
    assertThatThrownBy(() -> TimestampTZColumn.of("col", timestampWithThreeDigitNano))
        .isInstanceOf((IllegalArgumentException.class));
    assertThatThrownBy(() -> TimestampTZColumn.of("col", timestampWithTwoDigitNano))
        .isInstanceOf((IllegalArgumentException.class));
    assertThatThrownBy(() -> TimestampTZColumn.of("col", timestampWithOneDigitNano))
        .isInstanceOf((IllegalArgumentException.class));
    assertThatThrownBy(() -> TimestampTZColumn.of("col", timestampWithThreeDigitMicro))
        .isInstanceOf((IllegalArgumentException.class));
    assertThatThrownBy(() -> TimestampTZColumn.of("col", timestampWithTwoDigitMicro))
        .isInstanceOf((IllegalArgumentException.class));
    assertThatThrownBy(() -> TimestampTZColumn.of("col", timestampWithOneDigitMicro))
        .isInstanceOf((IllegalArgumentException.class));
  }
}
