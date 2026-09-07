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
  public void compareTo_SupplementaryCharacterGiven_ShouldOrderByCodePoint() {
    // Arrange
    TextColumn lastBmp = TextColumn.of("col", "\uFFFF");
    TextColumn firstSupplementary = TextColumn.of("col", "\uD800\uDC00"); // U+10000

    // Act Assert
    assertThat(lastBmp.compareTo(firstSupplementary)).isNegative();
    assertThat(firstSupplementary.compareTo(lastBmp)).isPositive();
  }

  @Test
  public void compareTo_SharedSupplementaryPrefixGiven_ShouldCompareTheFollowingCharacters() {
    // Arrange
    TextColumn prefix = TextColumn.of("col", "\uD800\uDC00");
    TextColumn a = TextColumn.of("col", "\uD800\uDC00a");
    TextColumn b = TextColumn.of("col", "\uD800\uDC00b");

    // Act Assert
    assertThat(a.compareTo(b)).isNegative();
    assertThat(b.compareTo(a)).isPositive();
    assertThat(prefix.compareTo(a)).isNegative();
    assertThat(a.compareTo(TextColumn.of("col", "\uD800\uDC00a"))).isZero();
  }

  @Test
  public void
      compareTo_UnpairedSurrogateAndSupplementaryCharacterGiven_ShouldOrderSurrogateFirst() {
    // Arrange
    TextColumn unpairedLow = TextColumn.of("col", "\uDC00");
    TextColumn unpairedHigh = TextColumn.of("col", "\uD801");
    TextColumn supplementary = TextColumn.of("col", "\uD800\uDC00"); // U+10000

    // Act Assert
    assertThat(unpairedLow.compareTo(supplementary)).isNegative();
    assertThat(unpairedHigh.compareTo(supplementary)).isNegative();
  }

  @Test
  public void compareTo_UnpairedSurrogatesGiven_ShouldBeConsistentWithEquals() {
    // Arrange
    TextColumn high = TextColumn.of("col", "a\uD800");
    TextColumn low = TextColumn.of("col", "a\uDC00");

    // Act Assert
    assertThat(high.compareTo(low)).isNotZero();
    assertThat(high.compareTo(TextColumn.of("col", "a?"))).isNotZero();
    assertThat(high.compareTo(TextColumn.of("col", "a\uD800"))).isZero();
  }

  @Test
  public void equals_CollateEqualButByteDifferentValuesGiven_ShouldReturnFalse() {
    // Arrange
    TextColumn upper = TextColumn.of("col", "Apple");
    TextColumn lower = TextColumn.of("col", "apple");

    // Act Assert
    assertThat(upper).isNotEqualTo(lower);
    assertThat(upper).isEqualTo(TextColumn.of("col", "Apple"));
    assertThat(upper.hashCode()).isEqualTo(TextColumn.of("col", "Apple").hashCode());
  }
}
