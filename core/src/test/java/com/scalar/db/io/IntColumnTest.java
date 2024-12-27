package com.scalar.db.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

public class IntColumnTest {

  @Test
  public void of_ProperValueGiven_ShouldReturnWhatsSet() {
    // Arrange

    // Act
    IntColumn column = IntColumn.of("col", 10);

    // Assert
    assertThat(column.getName()).isEqualTo("col");
    assertThat(column.getValue()).isPresent();
    assertThat(column.getValue().get()).isEqualTo(10);
    assertThat(column.getIntValue()).isEqualTo(10);
    assertThat(column.getDataType()).isEqualTo(DataType.INT);
    assertThat(column.hasNullValue()).isFalse();
    assertThat(column.getValueAsObject()).isEqualTo(10);
    assertThatThrownBy(column::getBooleanValue).isInstanceOf(UnsupportedOperationException.class);
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
    assertThatThrownBy(column::getTimestampTZValue)
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void ofNull_ProperValueGiven_ShouldReturnWhatsSet() {
    // Arrange

    // Act
    IntColumn column = IntColumn.ofNull("col");

    // Assert
    assertThat(column.getName()).isEqualTo("col");
    assertThat(column.getValue()).isNotPresent();
    assertThat(column.getIntValue()).isEqualTo(0);
    assertThat(column.getDataType()).isEqualTo(DataType.INT);
    assertThat(column.hasNullValue()).isTrue();
    assertThat(column.getValueAsObject()).isNull();
    assertThatThrownBy(column::getBooleanValue).isInstanceOf(UnsupportedOperationException.class);
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
    assertThatThrownBy(column::getTimestampTZValue)
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void copyWith_ProperValueGiven_ShouldReturnSameValueButDifferentName() {
    // Arrange

    // Act
    IntColumn column = IntColumn.of("col", 10).copyWith("col2");

    // Assert
    assertThat(column.getName()).isEqualTo("col2");
    assertThat(column.getValue()).isPresent();
    assertThat(column.getValue().get()).isEqualTo(10);
    assertThat(column.getIntValue()).isEqualTo(10);
    assertThat(column.getDataType()).isEqualTo(DataType.INT);
    assertThat(column.hasNullValue()).isFalse();
    assertThat(column.getValueAsObject()).isEqualTo(10);
    assertThatThrownBy(column::getBooleanValue).isInstanceOf(UnsupportedOperationException.class);
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
    assertThatThrownBy(column::getTimestampTZValue)
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void compareTo_ShouldReturnProperResults() {
    // Arrange
    IntColumn column = IntColumn.of("col", 100);

    // Act Assert
    assertThat(column.compareTo(IntColumn.of("col", 100))).isEqualTo(0);
    assertThat(column.compareTo(IntColumn.of("col", 200))).isLessThan(0);
    assertThat(column.compareTo(IntColumn.of("col", 0))).isGreaterThan(0);
    assertThat(column.compareTo(IntColumn.ofNull("col"))).isGreaterThan(0);
  }
}
