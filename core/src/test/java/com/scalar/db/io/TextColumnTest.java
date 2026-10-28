package com.scalar.db.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

public class TextColumnTest {

  @Test
  public void of_ProperValueGiven_ShouldReturnWhatsSet() {
    // Arrange

    // Act
    TextColumn column = TextColumn.of("col", "text");

    // Assert
    assertThat(column.getName()).isEqualTo("col");
    assertThat(column.getValue()).isPresent();
    assertThat(column.getValue().get()).isEqualTo("text");
    assertThat(column.getTextValue()).isEqualTo("text");
    assertThat(column.getDataType()).isEqualTo(DataType.TEXT);
    assertThat(column.hasNullValue()).isFalse();
    assertThat(column.getValueAsObject()).isEqualTo("text");
    assertThatThrownBy(column::getBooleanValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getIntValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getBigIntValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getFloatValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getDoubleValue).isInstanceOf(UnsupportedOperationException.class);
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
    TextColumn column = TextColumn.ofNull("col");

    // Assert
    assertThat(column.getName()).isEqualTo("col");
    assertThat(column.getValue()).isNotPresent();
    assertThat(column.getTextValue()).isNull();
    assertThat(column.getDataType()).isEqualTo(DataType.TEXT);
    assertThat(column.hasNullValue()).isTrue();
    assertThat(column.getValueAsObject()).isNull();
    assertThatThrownBy(column::getBooleanValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getIntValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getBigIntValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getFloatValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getDoubleValue).isInstanceOf(UnsupportedOperationException.class);
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
    TextColumn column = TextColumn.of("col", "text").copyWith("col2");

    // Assert
    assertThat(column.getName()).isEqualTo("col2");
    assertThat(column.getValue()).isPresent();
    assertThat(column.getValue().get()).isEqualTo("text");
    assertThat(column.getTextValue()).isEqualTo("text");
    assertThat(column.getDataType()).isEqualTo(DataType.TEXT);
    assertThat(column.hasNullValue()).isFalse();
    assertThat(column.getValueAsObject()).isEqualTo("text");
    assertThatThrownBy(column::getBooleanValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getIntValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getBigIntValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getFloatValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getDoubleValue).isInstanceOf(UnsupportedOperationException.class);
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
    TextColumn column = TextColumn.of("col", "aaa");

    // Act Assert
    assertThat(column.compareTo(TextColumn.of("col", "aaa"))).isEqualTo(0);
    assertThat(column.compareTo(TextColumn.of("col", "aaaa"))).isLessThan(0);
    assertThat(column.compareTo(TextColumn.of("col", "aa"))).isGreaterThan(0);
    assertThat(column.compareTo(TextColumn.ofNull("col"))).isGreaterThan(0);
  }

  @Test
  public void compareTo_SupplementaryCharacterAndCharacterAboveSurrogates_ShouldOrderByCodePoint() {
    // Arrange
    TextColumn supplementary = TextColumn.of("col", "\uD83D\uDE00"); // U+1F600
    TextColumn aboveSurrogates = TextColumn.of("col", "\uFF61"); // U+FF61

    // Act Assert
    assertThat(supplementary.compareTo(aboveSurrogates)).isGreaterThan(0);
    assertThat(aboveSurrogates.compareTo(supplementary)).isLessThan(0);
  }

  @Test
  public void compareTo_SupplementaryCharacterAndUnpairedHighSurrogate_ShouldOrderByCodePoint() {
    // Arrange
    TextColumn supplementary = TextColumn.of("col", "\uD83D\uDE00"); // U+1F600
    TextColumn unpairedHighSurrogate = TextColumn.of("col", "\uD83D\uE000"); // U+D83D U+E000

    // Act Assert
    assertThat(supplementary.compareTo(unpairedHighSurrogate)).isGreaterThan(0);
    assertThat(unpairedHighSurrogate.compareTo(supplementary)).isLessThan(0);
  }
}
