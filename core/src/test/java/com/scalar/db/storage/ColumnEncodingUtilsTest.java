package com.scalar.db.storage;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.io.DateColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class ColumnEncodingUtilsTest {
  @Test
  public void encodeDate_ShouldWorkProperly() {
    // Arrange
    DateColumn column = DateColumn.of("date", LocalDate.of(2023, 10, 1));

    // Act
    long encoded = ColumnEncodingUtils.encode(column);

    // Assert
    assertThat(encoded).isEqualTo(LocalDate.of(2023, 10, 1).toEpochDay());
  }

  @Test
  public void encodeTime_ShouldWorkProperly() {
    // Arrange
    TimeColumn column = TimeColumn.of("time", LocalTime.of(12, 34, 56, 123_456_000));

    // Act
    long encoded = ColumnEncodingUtils.encode(column);

    // Assert
    assertThat(encoded).isEqualTo(LocalTime.of(12, 34, 56, 123_456_000).toNanoOfDay());
  }

  @Test
  public void encodeTimestamp_ShouldWorkProperly() {
    // Arrange
    TimestampColumn column =
        TimestampColumn.of("timestamp", LocalDateTime.of(2023, 10, 1, 12, 34, 56, 789000000));

    // Act
    String encoded = ColumnEncodingUtils.encode(column);

    // Assert
    assertThat(encoded).isEqualTo("20231001123456789");
  }

  @Test
  public void encodeTimestampTZ_ShouldWorkProperly() {
    // Arrange
    TimestampTZColumn column =
        TimestampTZColumn.of(
            "timestamptz",
            LocalDateTime.of(2023, 10, 1, 12, 34, 56, 789000000).toInstant(ZoneOffset.UTC));

    // Act
    String encoded = ColumnEncodingUtils.encode(column);

    // Assert
    assertThat(encoded).isEqualTo("20231001123456789");
  }

  @Test
  public void decodeDate_ShouldWorkProperly() {
    // Arrange Act
    LocalDate date = ColumnEncodingUtils.decodeDate(LocalDate.of(2023, 10, 1).toEpochDay());

    // Assert
    assertThat(date).isEqualTo(LocalDate.of(2023, 10, 1));
  }

  @Test
  public void decodeTime_ShouldWorkProperly() {
    // Arrange Act
    LocalTime time =
        ColumnEncodingUtils.decodeTime(LocalTime.of(12, 34, 56, 123_456_000).toNanoOfDay());

    // Assert
    assertThat(time).isEqualTo(LocalTime.of(12, 34, 56, 123_456_000));
  }

  @Test
  public void decodeTimestamp_ShouldWorkProperly() {
    // Arrange
    LocalDateTime timestamp = ColumnEncodingUtils.decodeTimestamp("20231001123456789");

    // Act assert
    assertThat(timestamp).isEqualTo(LocalDateTime.of(2023, 10, 1, 12, 34, 56, 789_000_000));
  }

  @Test
  public void decodeTimestampTZ_ShouldWorkProperly() {
    // Arrange
    Instant timestampTZ = ColumnEncodingUtils.decodeTimestampTZ("20231001123456789");

    // Act assert
    assertThat(timestampTZ)
        .isEqualTo(
            LocalDateTime.of(2023, 10, 1, 12, 34, 56, 789_000_000).toInstant(ZoneOffset.UTC));
  }
}
