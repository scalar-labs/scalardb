package com.scalar.db.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Clock;
import java.time.LocalDate;
import org.junit.jupiter.api.Test;

class DateColumnTest {
  private static final LocalDate ANY_DATE = LocalDate.now(Clock.systemUTC());

  @Test
  public void of_ProperValueGiven_ShouldReturnWhatsSet() {
    // Arrange

    // Act
    DateColumn column = DateColumn.of("col", ANY_DATE);

    // Assert
    assertThat(column.getName()).isEqualTo("col");
    assertThat(column.getValue()).isPresent();
    assertThat(column.getValue().get()).isEqualTo(ANY_DATE);
    assertThat(column.getDateValue()).isEqualTo(ANY_DATE);
    assertThat(column.getDataType()).isEqualTo(DataType.DATE);
    assertThat(column.hasNullValue()).isFalse();
    assertThat(column.getValueAsObject()).isEqualTo(ANY_DATE);
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
    assertThatThrownBy(column::getTimeValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getTimestampValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getTimestampTZValue)
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void ofNull_ProperValueGiven_ShouldReturnWhatsSet() {
    // Arrange

    // Act
    DateColumn column = DateColumn.ofNull("col");

    // Assert
    assertThat(column.getName()).isEqualTo("col");
    assertThat(column.getValue()).isNotPresent();
    assertThat(column.getDateValue()).isNull();
    assertThat(column.getDataType()).isEqualTo(DataType.DATE);
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
    assertThatThrownBy(column::getTimeValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getTimestampValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getTimestampTZValue)
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void copyWith_ProperValueGiven_ShouldReturnSameValueButDifferentName() {
    // Arrange

    // Act
    DateColumn column = DateColumn.of("col", ANY_DATE).copyWith("col2");

    // Assert
    assertThat(column.getName()).isEqualTo("col2");
    assertThat(column.getValue()).isPresent();
    assertThat(column.getValue().get()).isEqualTo(ANY_DATE);
    assertThat(column.getDateValue()).isEqualTo(ANY_DATE);
    assertThat(column.getDataType()).isEqualTo(DataType.DATE);
    assertThat(column.hasNullValue()).isFalse();
    assertThat(column.getValueAsObject()).isEqualTo(ANY_DATE);
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
    assertThatThrownBy(column::getTimeValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getTimestampValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getTimestampTZValue)
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void compareTo_ShouldReturnProperResults() {
    // Arrange
    DateColumn column = DateColumn.of("col", ANY_DATE);

    // Act Assert
    assertThat(column.compareTo(DateColumn.of("col", ANY_DATE))).isZero();
    assertThat(column.compareTo(DateColumn.of("col", ANY_DATE.minusDays(1)))).isPositive();
    assertThat(column.compareTo(DateColumn.of("col", ANY_DATE.plusDays(1)))).isNegative();
    assertThat(column.compareTo(DateColumn.ofNull("col"))).isPositive();
  }

  @Test
  public void constructor_valueInsideRange_ShouldNotThrowException() {
    // Act Assert
    assertDoesNotThrow(() -> DateColumn.of("col", DateColumn.MIN_VALUE));
    assertDoesNotThrow(() -> DateColumn.of("col", DateColumn.MAX_VALUE));
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
}
