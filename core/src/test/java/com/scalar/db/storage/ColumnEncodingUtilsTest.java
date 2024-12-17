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
    TimestampColumn positiveEpochSecondWithNano =
        TimestampColumn.of("timestamp", LocalDateTime.of(2023, 10, 1, 12, 34, 56, 789_000_000));
    TimestampColumn positiveEpochSecondWithZeroNano =
        TimestampColumn.of("timestamp", LocalDateTime.of(2023, 10, 1, 12, 34, 56, 0));
    TimestampColumn negativeEpochSecondWithNano =
        TimestampColumn.of("timestamp", LocalDateTime.of(1234, 10, 1, 12, 34, 56, 456_000_000));
    TimestampColumn negativeEpochSecondWithZeroNano =
        TimestampColumn.of("timestamp", LocalDateTime.of(1234, 10, 1, 12, 34, 56, 0));
    TimestampColumn epoch =
        TimestampColumn.of(
            "timestamp", LocalDateTime.of(LocalDate.ofEpochDay(0), LocalTime.of(0, 0)));

    // Act
    long actualPositiveEpochSecondWithNano =
        ColumnEncodingUtils.encode(positiveEpochSecondWithNano);
    long actualPositiveEpochSecondWithZeroNano =
        ColumnEncodingUtils.encode(positiveEpochSecondWithZeroNano);
    long actualNegativeEpochSecondWithNano =
        ColumnEncodingUtils.encode(negativeEpochSecondWithNano);
    long actualNegativeEpochSecondWithZeroNano =
        ColumnEncodingUtils.encode(negativeEpochSecondWithZeroNano);
    long actualEpoch = ColumnEncodingUtils.encode(epoch);

    // Assert
    assertThat(actualPositiveEpochSecondWithNano).isEqualTo(1696163696789L);
    assertThat(actualPositiveEpochSecondWithZeroNano).isEqualTo(1696163696000L);
    assertThat(actualNegativeEpochSecondWithNano).isEqualTo(-23202242704456L);
    assertThat(actualNegativeEpochSecondWithZeroNano).isEqualTo(-23202242704000L);
    assertThat(actualEpoch).isEqualTo(0L);
  }

  @Test
  public void encodeTimestampTZ_ShouldWorkProperly() {
    // Arrange
    TimestampTZColumn positiveEpochSecondWithNano =
        TimestampTZColumn.of(
            "timestamptz",
            LocalDateTime.of(2023, 10, 1, 12, 34, 56, 789_000_000).toInstant(ZoneOffset.UTC));
    TimestampTZColumn positiveEpochSecondWithZeroNano =
        TimestampTZColumn.of(
            "timestamptz", LocalDateTime.of(2023, 10, 1, 12, 34, 56, 0).toInstant(ZoneOffset.UTC));
    TimestampTZColumn negativeEpochSecondWithNano =
        TimestampTZColumn.of(
            "timestamptz",
            LocalDateTime.of(1234, 10, 1, 12, 34, 56, 456_000_000).toInstant(ZoneOffset.UTC));
    TimestampTZColumn negativeEpochSecondWithZeroNano =
        TimestampTZColumn.of(
            "timestamptz", LocalDateTime.of(1234, 10, 1, 12, 34, 56, 0).toInstant(ZoneOffset.UTC));
    TimestampTZColumn epoch =
        TimestampTZColumn.of(
            "timestamptz",
            LocalDateTime.of(LocalDate.ofEpochDay(0), LocalTime.of(0, 0))
                .toInstant(ZoneOffset.UTC));

    // Act
    long actualPositiveEpochSecondWithNano =
        ColumnEncodingUtils.encode(positiveEpochSecondWithNano);
    long actualPositiveEpochSecondWithZeroNano =
        ColumnEncodingUtils.encode(positiveEpochSecondWithZeroNano);
    long actualNegativeEpochSecondWithNano =
        ColumnEncodingUtils.encode(negativeEpochSecondWithNano);
    long actualNegativeEpochSecondWithZeroNano =
        ColumnEncodingUtils.encode(negativeEpochSecondWithZeroNano);
    long actualEpoch = ColumnEncodingUtils.encode(epoch);

    // Assert
    assertThat(actualPositiveEpochSecondWithNano).isEqualTo(1696163696789L);
    assertThat(actualPositiveEpochSecondWithZeroNano).isEqualTo(1696163696000L);
    assertThat(actualNegativeEpochSecondWithNano).isEqualTo(-23202242704456L);
    assertThat(actualNegativeEpochSecondWithZeroNano).isEqualTo(-23202242704000L);
    assertThat(actualEpoch).isEqualTo(0L);
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
    // Act
    LocalDateTime positiveEpochSecondWithNano = ColumnEncodingUtils.decodeTimestamp(1696163696789L);
    LocalDateTime positiveEpochSecondWithZeroNano =
        ColumnEncodingUtils.decodeTimestamp(1696163696000L);
    LocalDateTime negativeEpochSecondWithNano =
        ColumnEncodingUtils.decodeTimestamp(-23202242704456L);
    LocalDateTime negativeEpochSecondWithZeroNano =
        ColumnEncodingUtils.decodeTimestamp(-23202242704000L);
    LocalDateTime epoch = ColumnEncodingUtils.decodeTimestamp(0L);

    // Act assert
    assertThat(positiveEpochSecondWithNano)
        .isEqualTo(LocalDateTime.of(2023, 10, 1, 12, 34, 56, 789_000_000));
    assertThat(positiveEpochSecondWithZeroNano)
        .isEqualTo(LocalDateTime.of(2023, 10, 1, 12, 34, 56, 0));
    assertThat(negativeEpochSecondWithNano)
        .isEqualTo(LocalDateTime.of(1234, 10, 1, 12, 34, 56, 456_000_000));
    assertThat(negativeEpochSecondWithZeroNano)
        .isEqualTo(LocalDateTime.of(1234, 10, 1, 12, 34, 56, 0));
    assertThat(epoch).isEqualTo(LocalDateTime.of(LocalDate.ofEpochDay(0), LocalTime.of(0, 0)));
  }

  @Test
  public void decodeTimestampTZ_ShouldWorkProperly() {
    // Arrange
    Instant positiveEpochSecondWithNano = ColumnEncodingUtils.decodeTimestampTZ(1696163696789L);
    Instant positiveEpochSecondWithZeroNano = ColumnEncodingUtils.decodeTimestampTZ(1696163696000L);
    Instant negativeEpochSecondWithNano = ColumnEncodingUtils.decodeTimestampTZ(-23202242704456L);
    Instant negativeEpochSecondWithZeroNano =
        ColumnEncodingUtils.decodeTimestampTZ(-23202242704000L);
    Instant epoch = ColumnEncodingUtils.decodeTimestampTZ(0);

    // Act assert
    assertThat(positiveEpochSecondWithNano)
        .isEqualTo(
            LocalDateTime.of(2023, 10, 1, 12, 34, 56, 789_000_000).toInstant(ZoneOffset.UTC));
    assertThat(positiveEpochSecondWithZeroNano)
        .isEqualTo(LocalDateTime.of(2023, 10, 1, 12, 34, 56, 0).toInstant(ZoneOffset.UTC));
    assertThat(negativeEpochSecondWithNano)
        .isEqualTo(
            LocalDateTime.of(1234, 10, 1, 12, 34, 56, 456_000_000).toInstant(ZoneOffset.UTC));
    assertThat(negativeEpochSecondWithZeroNano)
        .isEqualTo(LocalDateTime.of(1234, 10, 1, 12, 34, 56, 0).toInstant(ZoneOffset.UTC));
    assertThat(epoch).isEqualTo(Instant.EPOCH);
  }
}
