package com.scalar.db.storage.dynamo.bytes;

import java.nio.ByteBuffer;
import java.util.Optional;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class BytesUtilsTest {

  @Test
  public void getTheClosestNextBytes_EmptyBytesGiven_ShouldReturnTheClosestNextBytesCorrectly() {
    // Arrange
    ByteBuffer bytes = ByteBuffer.wrap(new byte[] {});

    // Act
    Optional<ByteBuffer> actual = BytesUtils.getTheClosestNextBytes(bytes);

    // Assert
    Assertions.assertThat(actual).isPresent();
    Assertions.assertThat(actual.get()).isEqualTo(ByteBuffer.wrap(new byte[] {0}));
  }

  @Test
  public void getTheClosestNextBytes_All00sBytesGiven_ShouldReturnTheClosestNextBytesCorrectly() {
    // Arrange
    ByteBuffer bytes = ByteBuffer.wrap(new byte[] {0, 0, 0});

    // Act
    Optional<ByteBuffer> actual = BytesUtils.getTheClosestNextBytes(bytes);

    // Assert
    Assertions.assertThat(actual).isPresent();
    Assertions.assertThat(actual.get()).isEqualTo(ByteBuffer.wrap(new byte[] {0, 0, 1}));
  }

  @Test
  public void
      getTheClosestNextBytes_BytesWithFFsBytesInTheTailGiven_ShouldReturnTheClosestNextBytesCorrectly() {
    // Arrange
    ByteBuffer bytes = ByteBuffer.wrap(new byte[] {0, 0, (byte) 0xff});

    // Act
    Optional<ByteBuffer> actual = BytesUtils.getTheClosestNextBytes(bytes);

    // Assert
    Assertions.assertThat(actual).isPresent();
    Assertions.assertThat(actual.get()).isEqualTo(ByteBuffer.wrap(new byte[] {0, 1}));
  }

  @Test
  public void getTheClosestNextBytes_AllFFsBytesGiven_ShouldReturnEmpty() {
    // Arrange
    ByteBuffer bytes = ByteBuffer.wrap(new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff});

    // Act
    Optional<ByteBuffer> actual = BytesUtils.getTheClosestNextBytes(bytes);

    // Assert
    Assertions.assertThat(actual).isNotPresent();
  }

  @Test
  public void getTheClosestPreviousBytes_EmptyBytesGiven_ShouldReturnEmpty() {
    // Arrange
    ByteBuffer bytes = ByteBuffer.wrap(new byte[] {});

    // Act
    Optional<ByteBuffer> actual = BytesUtils.getTheClosestPreviousBytes(bytes);

    // Assert
    Assertions.assertThat(actual).isNotPresent();
  }

  @Test
  public void
      getTheClosestPreviousBytes_All00sBytesGiven_ShouldReturnTheClosestNextBytesCorrectly() {
    // Arrange
    ByteBuffer bytes = ByteBuffer.wrap(new byte[] {0, 0, 0});

    // Act
    Optional<ByteBuffer> actual = BytesUtils.getTheClosestPreviousBytes(bytes);

    // Assert
    Assertions.assertThat(actual).isPresent();
    Assertions.assertThat(actual.get()).isEqualTo(ByteBuffer.wrap(new byte[] {0, 0}));
  }

  @Test
  public void
      getTheClosestPreviousBytes_BytesWithFFsBytesInTheTailGiven_ShouldReturnTheClosestNextBytesCorrectly() {
    // Arrange
    ByteBuffer bytes = ByteBuffer.wrap(new byte[] {0, 0, (byte) 0xff});

    // Act
    Optional<ByteBuffer> actual = BytesUtils.getTheClosestPreviousBytes(bytes);

    // Assert
    Assertions.assertThat(actual).isPresent();
    Assertions.assertThat(actual.get()).isEqualTo(ByteBuffer.wrap(new byte[] {0, 0, (byte) 0xfe}));
  }
}
