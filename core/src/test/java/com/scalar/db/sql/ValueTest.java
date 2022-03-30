package com.scalar.db.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

public class ValueTest {

  @Test
  public void ofBoolean_ShouldBuildProperValue() {
    // Arrange

    // Act
    Value value = Value.ofBoolean(true);

    // Assert
    assertThat(value.type).isEqualTo(Value.Type.BOOLEAN);
    assertThat(value.value).isInstanceOf(Boolean.class);
    assertThat(value.value).isEqualTo(true);
  }

  @Test
  public void ofInt_ShouldBuildProperValue() {
    // Arrange

    // Act
    Value value = Value.ofInt(10);

    // Assert
    assertThat(value.type).isEqualTo(Value.Type.INT);
    assertThat(value.value).isInstanceOf(Integer.class);
    assertThat(value.value).isEqualTo(10);
  }

  @Test
  public void ofBigInt_ShouldBuildProperValue() {
    // Arrange

    // Act
    Value value = Value.ofBigInt(100L);

    // Assert
    assertThat(value.type).isEqualTo(Value.Type.BIGINT);
    assertThat(value.value).isInstanceOf(Long.class);
    assertThat(value.value).isEqualTo(100L);
  }

  @Test
  public void ofFloat_ShouldBuildProperValue() {
    // Arrange

    // Act
    Value value = Value.ofFloat(1.23F);

    // Assert
    assertThat(value.type).isEqualTo(Value.Type.FLOAT);
    assertThat(value.value).isInstanceOf(Float.class);
    assertThat(value.value).isEqualTo(1.23F);
  }

  @Test
  public void ofDouble_ShouldBuildProperValue() {
    // Arrange

    // Act
    Value value = Value.ofDouble(4.56);

    // Assert
    assertThat(value.type).isEqualTo(Value.Type.DOUBLE);
    assertThat(value.value).isInstanceOf(Double.class);
    assertThat(value.value).isEqualTo(4.56);
  }

  @Test
  public void ofText_ShouldBuildProperValue() {
    // Arrange

    // Act
    Value value = Value.ofText("text");

    // Assert
    assertThat(value.type).isEqualTo(Value.Type.TEXT);
    assertThat(value.value).isInstanceOf(String.class);
    assertThat(value.value).isEqualTo("text");
  }

  @Test
  public void ofBlob_ShouldBuildProperValue() {
    // Arrange

    // Act
    Value value1 = Value.ofBlob(ByteBuffer.wrap("blob1".getBytes(StandardCharsets.UTF_8)));
    Value value2 = Value.ofBlob("blob1".getBytes(StandardCharsets.UTF_8));

    // Assert
    assertThat(value1.type).isEqualTo(Value.Type.BLOB_BYTE_BUFFER);
    assertThat(value1.value).isInstanceOf(ByteBuffer.class);
    assertThat(value1.value).isEqualTo(ByteBuffer.wrap("blob1".getBytes(StandardCharsets.UTF_8)));

    assertThat(value2.type).isEqualTo(Value.Type.BLOB_BYTES);
    assertThat(value2.value).isInstanceOf(byte[].class);
    assertThat(value2.value).isEqualTo("blob1".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void ofNull_ShouldBuildProperValue() {
    // Arrange

    // Act
    Value value = Value.ofNull();

    // Assert
    assertThat(value.type).isEqualTo(Value.Type.NULL);
    assertThat(value.value).isNull();
  }
}
