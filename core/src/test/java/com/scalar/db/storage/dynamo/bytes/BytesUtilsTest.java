package com.scalar.db.storage.dynamo.bytes;

import java.nio.ByteBuffer;
import java.util.Optional;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class BytesUtilsTest {

  @Test
  public void getClosestNextBytes_EmptyBytesGiven_ShouldReturnClosestNextBytesCorrectly() {
    // Arrange
    ByteBuffer bytes = ByteBuffer.wrap(new byte[] {});

    // Act
    Optional<ByteBuffer> actual = BytesUtils.getClosestNextBytes(bytes);

    // Assert
    Assertions.assertThat(actual).isPresent();
    Assertions.assertThat(actual.get()).isEqualTo(ByteBuffer.wrap(new byte[] {0}));
  }

  @Test
  public void getClosestNextBytes_All00sBytesGiven_ShouldReturnClosestNextBytesCorrectly() {
    // Arrange
    ByteBuffer bytes = ByteBuffer.wrap(new byte[] {0, 0, 0});

    // Act
    Optional<ByteBuffer> actual = BytesUtils.getClosestNextBytes(bytes);

    // Assert
    Assertions.assertThat(actual).isPresent();
    Assertions.assertThat(actual.get()).isEqualTo(ByteBuffer.wrap(new byte[] {0, 0, 1}));
  }

  @Test
  public void
      getClosestNextBytes_BytesWithFFsBytesInTheTailGiven_ShouldReturnClosestNextBytesCorrectly() {
    // Arrange
    ByteBuffer bytes = ByteBuffer.wrap(new byte[] {0, 0, (byte) 0xff});

    // Act
    Optional<ByteBuffer> actual = BytesUtils.getClosestNextBytes(bytes);

    // Assert
    Assertions.assertThat(actual).isPresent();
    Assertions.assertThat(actual.get()).isEqualTo(ByteBuffer.wrap(new byte[] {0, 1}));
  }

  @Test
  public void getClosestNextBytes_AllFFsBytesGiven_ShouldReturnEmpty() {
    // Arrange
    ByteBuffer bytes = ByteBuffer.wrap(new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff});

    // Act
    Optional<ByteBuffer> actual = BytesUtils.getClosestNextBytes(bytes);

    // Assert
    Assertions.assertThat(actual).isNotPresent();
  }

  @Test
  public void getClosestPreviousBytes_EmptyBytesGiven_ShouldReturnEmpty() {
    // Arrange
    ByteBuffer bytes = ByteBuffer.wrap(new byte[] {});

    // Act
    Optional<ByteBuffer> actual = BytesUtils.getClosestPreviousBytes(bytes);

    // Assert
    Assertions.assertThat(actual).isNotPresent();
  }

  @Test
  public void getClosestPreviousBytes_All00sBytesGiven_ShouldReturnClosestNextBytesCorrectly() {
    // Arrange
    ByteBuffer bytes = ByteBuffer.wrap(new byte[] {0, 0, 0});

    // Act
    Optional<ByteBuffer> actual = BytesUtils.getClosestPreviousBytes(bytes);

    // Assert
    Assertions.assertThat(actual).isPresent();
    Assertions.assertThat(actual.get()).isEqualTo(ByteBuffer.wrap(new byte[] {0, 0}));
  }

  @Test
  public void
      getClosestPreviousBytes_BytesWithFFsBytesInTheTailGiven_ShouldReturnClosestNextBytesCorrectly() {
    // Arrange
    ByteBuffer bytes = ByteBuffer.wrap(new byte[] {0, 0, (byte) 0xff});

    // Act
    Optional<ByteBuffer> actual = BytesUtils.getClosestPreviousBytes(bytes);

    // Assert
    Assertions.assertThat(actual).isPresent();
    Assertions.assertThat(actual.get()).isEqualTo(ByteBuffer.wrap(new byte[] {0, 0, (byte) 0xfe}));
  }
}
