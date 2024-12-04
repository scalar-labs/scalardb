package com.scalar.db.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.time.Clock;
import java.time.LocalDateTime;
import org.junit.jupiter.api.Test;

class TimestampColumnTest {
  private static final LocalDateTime ANY_TIMESTAMP =
      LocalDateTime.now(Clock.systemUTC()).withNano(123_000_000);

  @Test
  public void of_ProperValueGiven_ShouldReturnWhatsSet() {
    // Arrange

    // Act
    TimestampColumn column = TimestampColumn.of("col", ANY_TIMESTAMP);

    // Assert
    assertThat(column.getName()).isEqualTo("col");
    assertThat(column.getValue()).isPresent();
    assertThat(column.getValue().get()).isEqualTo(ANY_TIMESTAMP);
    assertThat(column.getTimestampValue()).isEqualTo(ANY_TIMESTAMP);
    assertThat(column.getDataType()).isEqualTo(DataType.TIMESTAMP);
    assertThat(column.hasNullValue()).isFalse();
    assertThat(column.getValueAsObject()).isEqualTo(ANY_TIMESTAMP);
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
    assertThatThrownBy(column::getTimestampTZValue)
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void ofNull_ProperValueGiven_ShouldReturnWhatsSet() {
    // Arrange

    // Act
    TimestampColumn column = TimestampColumn.ofNull("col");

    // Assert
    assertThat(column.getName()).isEqualTo("col");
    assertThat(column.getValue()).isNotPresent();
    assertThat(column.getTimestampValue()).isNull();
    assertThat(column.getDataType()).isEqualTo(DataType.TIMESTAMP);
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
    assertThatThrownBy(column::getTimestampTZValue)
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void copyWith_ProperValueGiven_ShouldReturnSameValueButDifferentName() {
    // Arrange

    // Act
    TimestampColumn column = TimestampColumn.of("col", ANY_TIMESTAMP).copyWith("col2");

    // Assert
    assertThat(column.getName()).isEqualTo("col2");
    assertThat(column.getValue()).isPresent();
    assertThat(column.getValue().get()).isEqualTo(ANY_TIMESTAMP);
    assertThat(column.getTimestampValue()).isEqualTo(ANY_TIMESTAMP);
    assertThat(column.getDataType()).isEqualTo(DataType.TIMESTAMP);
    assertThat(column.hasNullValue()).isFalse();
    assertThat(column.getValueAsObject()).isEqualTo(ANY_TIMESTAMP);
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
    assertThatThrownBy(column::getTimestampTZValue)
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void compareTo_ShouldReturnProperResults() {
    // Arrange
    TimestampColumn column = TimestampColumn.of("col", ANY_TIMESTAMP);

    // Act Assert
    assertThat(column.compareTo(TimestampColumn.of("col", ANY_TIMESTAMP))).isZero();
    assertThat(column.compareTo(TimestampColumn.of("col", ANY_TIMESTAMP.minusHours(1))))
        .isPositive();
    assertThat(column.compareTo(TimestampColumn.of("col", ANY_TIMESTAMP.plusDays(1)))).isNegative();
    assertThat(column.compareTo(TimestampColumn.ofNull("col"))).isPositive();
  }

  @Test
  public void constructor_valueInsideRange_ShouldNotThrowException() {
    // Act Assert
    assertDoesNotThrow(() -> TimestampColumn.of("col", TimestampColumn.MIN_VALUE));
    assertDoesNotThrow(() -> TimestampColumn.of("col", TimestampColumn.MAX_VALUE));
  }

  @Test
  public void constructor_valueOutOfRange_ShouldThrowIllegalArgumentException() {
    // Arrange
    LocalDateTime dateBeforeRangeMin = TimestampColumn.MIN_VALUE.minusSeconds(1);
    LocalDateTime dateAfterRangeMax = TimestampColumn.MAX_VALUE.plusSeconds(1);

    // Act Assert
    assertThatThrownBy(() -> TimestampColumn.of("col", dateBeforeRangeMin))
        .isInstanceOf((IllegalArgumentException.class));
    assertThatThrownBy(() -> TimestampColumn.of("col", dateAfterRangeMax))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void constructor_valueWithSubMillisecondPrecision_ShouldThrowIllegalArgumentException() {
    // Arrange
    LocalDateTime timestampWithThreeDigitNano =
        LocalDateTime.now(Clock.systemUTC()).withNano(123_456_789);
    LocalDateTime timestampWithTwoDigitNano =
        LocalDateTime.now(Clock.systemUTC()).withNano(123_456_780);
    LocalDateTime timestampWithOneDigitNano =
        LocalDateTime.now(Clock.systemUTC()).withNano(123_456_700);
    LocalDateTime timestampWithThreeDigitMicro =
        LocalDateTime.now(Clock.systemUTC()).withNano(123_456_000);
    LocalDateTime timestampWithTwoDigitMicro =
        LocalDateTime.now(Clock.systemUTC()).withNano(123_450_000);
    LocalDateTime timestampWithOneDigitMicro =
        LocalDateTime.now(Clock.systemUTC()).withNano(123_400_700);

    // Act Assert
    assertThatThrownBy(() -> TimestampColumn.of("col", timestampWithThreeDigitNano))
        .isInstanceOf((IllegalArgumentException.class));
    assertThatThrownBy(() -> TimestampColumn.of("col", timestampWithTwoDigitNano))
        .isInstanceOf((IllegalArgumentException.class));
    assertThatThrownBy(() -> TimestampColumn.of("col", timestampWithOneDigitNano))
        .isInstanceOf((IllegalArgumentException.class));
    assertThatThrownBy(() -> TimestampColumn.of("col", timestampWithThreeDigitMicro))
        .isInstanceOf((IllegalArgumentException.class));
    assertThatThrownBy(() -> TimestampColumn.of("col", timestampWithTwoDigitMicro))
        .isInstanceOf((IllegalArgumentException.class));
    assertThatThrownBy(() -> TimestampColumn.of("col", timestampWithOneDigitMicro))
        .isInstanceOf((IllegalArgumentException.class));
  }
}
