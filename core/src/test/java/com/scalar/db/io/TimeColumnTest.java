package com.scalar.db.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.time.Clock;
import java.time.LocalDate;
import java.time.LocalTime;
import org.junit.jupiter.api.Test;

class TimeColumnTest {
  private static final LocalTime ANY_TIME = LocalTime.NOON;

  @Test
  public void of_ProperValueGiven_ShouldReturnWhatsSet() {
    // Arrange

    // Act
    TimeColumn column = TimeColumn.of("col", ANY_TIME);

    // Assert
    assertThat(column.getName()).isEqualTo("col");
    assertThat(column.getValue()).isPresent();
    assertThat(column.getValue().get()).isEqualTo(ANY_TIME);
    assertThat(column.getTimeValue()).isEqualTo(ANY_TIME);
    assertThat(column.getDataType()).isEqualTo(DataType.TIME);
    assertThat(column.hasNullValue()).isFalse();
    assertThat(column.getValueAsObject()).isEqualTo(ANY_TIME);
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
    assertThatThrownBy(column::getTimestampValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getTimestampTZValue)
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void ofNull_ProperValueGiven_ShouldReturnWhatsSet() {
    // Arrange

    // Act
    TimeColumn column = TimeColumn.ofNull("col");

    // Assert
    assertThat(column.getName()).isEqualTo("col");
    assertThat(column.getValue()).isNotPresent();
    assertThat(column.getTimeValue()).isNull();
    assertThat(column.getDataType()).isEqualTo(DataType.TIME);
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
    assertThatThrownBy(column::getTimestampValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getTimestampTZValue)
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void copyWith_ProperValueGiven_ShouldReturnSameValueButDifferentName() {
    // Arrange

    // Act
    TimeColumn column = TimeColumn.of("col", ANY_TIME).copyWith("col2");

    // Assert
    assertThat(column.getName()).isEqualTo("col2");
    assertThat(column.getValue()).isPresent();
    assertThat(column.getValue().get()).isEqualTo(ANY_TIME);
    assertThat(column.getTimeValue()).isEqualTo(ANY_TIME);
    assertThat(column.getDataType()).isEqualTo(DataType.TIME);
    assertThat(column.hasNullValue()).isFalse();
    assertThat(column.getValueAsObject()).isEqualTo(ANY_TIME);
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
    assertThatThrownBy(column::getTimestampValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getTimestampTZValue)
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void compareTo_ShouldReturnProperResults() {
    // Arrange
    TimeColumn column = TimeColumn.of("col", ANY_TIME);

    // Act Assert
    assertThat(column.compareTo(TimeColumn.of("col", ANY_TIME))).isZero();
    assertThat(column.compareTo(TimeColumn.of("col", ANY_TIME.minusHours(1)))).isPositive();
    assertThat(column.compareTo(TimeColumn.of("col", ANY_TIME.plusMinutes(1)))).isNegative();
    assertThat(column.compareTo(TimeColumn.ofNull("col"))).isPositive();
  }

  @Test
  public void constructor_valueInsideRange_ShouldNotThrowException() {

    // Act Assert
    assertDoesNotThrow(() -> TimeColumn.of("col", TimeColumn.MIN_VALUE));
    assertDoesNotThrow(() -> TimeColumn.of("col", TimeColumn.MAX_VALUE));
  }

  @Test
  public void constructor_valueOutOfRange_ShouldThrowIllegalArgumentException() {
    // Arrange
    LocalDate dateBeforeRangeMin = DateColumn.MIN_VALUE.minusDays(1);
    LocalDate dateAfterRangeMax = DateColumn.MAX_VALUE.plusDays(1);

    // Act Assert
    assertThatThrownBy(() -> DateColumn.of("col", dateBeforeRangeMin))
        .isInstanceOf((IllegalArgumentException.class));
    assertThatThrownBy(() -> DateColumn.of("col", dateAfterRangeMax))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void constructor_valueWithSubMicrosecondPrecision_ShouldThrowIllegalArgumentException() {
    // Arrange
    LocalTime timeWithThreeDigitNano = LocalTime.now(Clock.systemUTC()).withNano(123_456_789);
    LocalTime timeWithTwoDigitNano = LocalTime.now(Clock.systemUTC()).withNano(123_456_780);
    LocalTime timeWithOneDigitNano = LocalTime.now(Clock.systemUTC()).withNano(123_456_700);

    // Act Assert
    assertThatThrownBy(() -> TimeColumn.of("col", timeWithThreeDigitNano))
        .isInstanceOf((IllegalArgumentException.class));
    assertThatThrownBy(() -> TimeColumn.of("col", timeWithTwoDigitNano))
        .isInstanceOf((IllegalArgumentException.class));
    assertThatThrownBy(() -> TimeColumn.of("col", timeWithOneDigitNano))
        .isInstanceOf((IllegalArgumentException.class));
  }
}
