package com.scalar.db.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

public class BigIntColumnTest {

  @Test
  public void of_ProperValueGiven_ShouldReturnWhatsSet() {
    // Arrange

    // Act
    BigIntColumn column = BigIntColumn.of("col", 100L);

    // Assert
    assertThat(column.getName()).isEqualTo("col");
    assertThat(column.getValue()).isPresent();
    assertThat(column.getValue().get()).isEqualTo(100L);
    assertThat(column.getBigIntValue()).isEqualTo(100L);
    assertThat(column.getDataType()).isEqualTo(DataType.BIGINT);
    assertThat(column.hasNullValue()).isFalse();
    assertThat(column.getValueAsObject()).isEqualTo(100L);
    assertThatThrownBy(column::getBooleanValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getIntValue).isInstanceOf(UnsupportedOperationException.class);
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
    assertThatThrownBy(column::getTimestampTZValue)
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void of_OutOfRangeBigIntValuesGiven_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatCode(() -> BigIntColumn.of("col", BigIntColumn.MAX_VALUE)).doesNotThrowAnyException();
    assertThatThrownBy(() -> BigIntColumn.of("col", BigIntColumn.MAX_VALUE + 1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatCode(() -> BigIntColumn.of("col", BigIntColumn.MIN_VALUE)).doesNotThrowAnyException();
    assertThatThrownBy(() -> BigIntColumn.of("col", BigIntColumn.MIN_VALUE - 1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void ofNull_ProperValueGiven_ShouldReturnWhatsSet() {
    // Arrange

    // Act
    BigIntColumn column = BigIntColumn.ofNull("col");

    // Assert
    assertThat(column.getName()).isEqualTo("col");
    assertThat(column.getValue()).isNotPresent();
    assertThat(column.getBigIntValue()).isEqualTo(0L);
    assertThat(column.getDataType()).isEqualTo(DataType.BIGINT);
    assertThat(column.hasNullValue()).isTrue();
    assertThat(column.getValueAsObject()).isNull();
    assertThatThrownBy(column::getBooleanValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getIntValue).isInstanceOf(UnsupportedOperationException.class);
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
    assertThatThrownBy(column::getTimestampTZValue)
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void copyWith_ProperValueGiven_ShouldReturnSameValueButDifferentName() {
    // Arrange

    // Act
    BigIntColumn column = BigIntColumn.of("col", 100L).copyWith("col2");

    // Assert
    assertThat(column.getName()).isEqualTo("col2");
    assertThat(column.getValue()).isPresent();
    assertThat(column.getValue().get()).isEqualTo(100L);
    assertThat(column.getBigIntValue()).isEqualTo(100L);
    assertThat(column.getDataType()).isEqualTo(DataType.BIGINT);
    assertThat(column.hasNullValue()).isFalse();
    assertThat(column.getValueAsObject()).isEqualTo(100L);
    assertThatThrownBy(column::getBooleanValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getIntValue).isInstanceOf(UnsupportedOperationException.class);
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
    assertThatThrownBy(column::getTimestampTZValue)
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void compareTo_ShouldReturnProperResults() {
    // Arrange
    BigIntColumn column = BigIntColumn.of("col", 100L);

    // Act Assert
    assertThat(column.compareTo(BigIntColumn.of("col", 100L))).isEqualTo(0);
    assertThat(column.compareTo(BigIntColumn.of("col", 200L))).isLessThan(0);
    assertThat(column.compareTo(BigIntColumn.of("col", 0L))).isGreaterThan(0);
    assertThat(column.compareTo(BigIntColumn.ofNull("col"))).isGreaterThan(0);
  }
}
