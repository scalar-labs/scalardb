package com.scalar.db.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

public class BlobColumnTest {

  @Test
  public void of_ProperByteBufferValueGiven_ShouldReturnWhatsSet() {
    // Arrange

    // Act
    BlobColumn column =
        BlobColumn.of("col", ByteBuffer.wrap("blob".getBytes(StandardCharsets.UTF_8)));

    // Assert
    assertThat(column.getName()).isEqualTo("col");
    assertThat(column.getValue()).isPresent();
    assertThat(column.getValue().get())
        .isEqualTo(ByteBuffer.wrap("blob".getBytes(StandardCharsets.UTF_8)));
    assertThat(column.getBlobValue())
        .isEqualTo(ByteBuffer.wrap("blob".getBytes(StandardCharsets.UTF_8)));
    assertThat(column.getBlobValueAsByteBuffer())
        .isEqualTo(ByteBuffer.wrap("blob".getBytes(StandardCharsets.UTF_8)));
    assertThat(column.getBlobValueAsBytes()).isEqualTo("blob".getBytes(StandardCharsets.UTF_8));
    assertThat(column.getDataType()).isEqualTo(DataType.BLOB);
    assertThat(column.hasNullValue()).isFalse();
    assertThat(column.getValueAsObject())
        .isEqualTo(ByteBuffer.wrap("blob".getBytes(StandardCharsets.UTF_8)));
    assertThatThrownBy(column::getBooleanValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getIntValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getBigIntValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getFloatValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getDoubleValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getTextValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getDateValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getTimeValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getTimestampValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getTimestampTZValue)
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void of_ProperByteArrayValueGiven_ShouldReturnWhatsSet() {
    // Arrange

    // Act
    BlobColumn column = BlobColumn.of("col", "blob".getBytes(StandardCharsets.UTF_8));

    // Assert
    assertThat(column.getName()).isEqualTo("col");
    assertThat(column.getValue()).isPresent();
    assertThat(column.getValue().get())
        .isEqualTo(ByteBuffer.wrap("blob".getBytes(StandardCharsets.UTF_8)));
    assertThat(column.getBlobValue())
        .isEqualTo(ByteBuffer.wrap("blob".getBytes(StandardCharsets.UTF_8)));
    assertThat(column.getBlobValueAsByteBuffer())
        .isEqualTo(ByteBuffer.wrap("blob".getBytes(StandardCharsets.UTF_8)));
    assertThat(column.getBlobValueAsBytes()).isEqualTo("blob".getBytes(StandardCharsets.UTF_8));
    assertThat(column.getDataType()).isEqualTo(DataType.BLOB);
    assertThat(column.hasNullValue()).isFalse();
    assertThat(column.getValueAsObject())
        .isEqualTo(ByteBuffer.wrap("blob".getBytes(StandardCharsets.UTF_8)));
    assertThatThrownBy(column::getBooleanValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getIntValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getBigIntValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getFloatValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getDoubleValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getTextValue).isInstanceOf(UnsupportedOperationException.class);
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
    BlobColumn column = BlobColumn.ofNull("col");

    // Assert
    assertThat(column.getName()).isEqualTo("col");
    assertThat(column.getValue()).isNotPresent();
    assertThat(column.getBlobValue()).isNull();
    assertThat(column.getBlobValueAsByteBuffer()).isNull();
    assertThat(column.getBlobValueAsBytes()).isNull();
    assertThat(column.getDataType()).isEqualTo(DataType.BLOB);
    assertThat(column.hasNullValue()).isTrue();
    assertThat(column.getValueAsObject()).isNull();
    assertThatThrownBy(column::getBooleanValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getIntValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getBigIntValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getFloatValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getDoubleValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getTextValue).isInstanceOf(UnsupportedOperationException.class);
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
    BlobColumn column =
        BlobColumn.of("col", ByteBuffer.wrap("blob".getBytes(StandardCharsets.UTF_8)))
            .copyWith("col2");

    // Assert
    assertThat(column.getName()).isEqualTo("col2");
    assertThat(column.getValue()).isPresent();
    assertThat(column.getValue().get())
        .isEqualTo(ByteBuffer.wrap("blob".getBytes(StandardCharsets.UTF_8)));
    assertThat(column.getBlobValue())
        .isEqualTo(ByteBuffer.wrap("blob".getBytes(StandardCharsets.UTF_8)));
    assertThat(column.getBlobValueAsByteBuffer())
        .isEqualTo(ByteBuffer.wrap("blob".getBytes(StandardCharsets.UTF_8)));
    assertThat(column.getBlobValueAsBytes()).isEqualTo("blob".getBytes(StandardCharsets.UTF_8));
    assertThat(column.getDataType()).isEqualTo(DataType.BLOB);
    assertThat(column.hasNullValue()).isFalse();
    assertThat(column.getValueAsObject())
        .isEqualTo(ByteBuffer.wrap("blob".getBytes(StandardCharsets.UTF_8)));
    assertThatThrownBy(column::getBooleanValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getIntValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getBigIntValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getFloatValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getDoubleValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getTextValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getDateValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getTimeValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getTimestampValue).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(column::getTimestampTZValue)
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void compareTo_ShouldReturnProperResults() {
    // Arrange
    BlobColumn column = BlobColumn.of("col", new byte[] {1, 1, 1, 1});

    // Act Assert
    assertThat(column.compareTo(BlobColumn.of("col", new byte[] {1, 1, 1, 1}))).isEqualTo(0);
    assertThat(column.compareTo(BlobColumn.of("col", new byte[] {-1, -1, -1, -1}))).isLessThan(0);
    assertThat(column.compareTo(BlobColumn.of("col", new byte[] {0, 0, 0, 0}))).isGreaterThan(0);
    assertThat(column.compareTo(BlobColumn.ofNull("col"))).isGreaterThan(0);
  }
}
