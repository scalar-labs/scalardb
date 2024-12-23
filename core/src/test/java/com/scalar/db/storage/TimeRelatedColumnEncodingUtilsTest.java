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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

class TimeRelatedColumnEncodingUtilsTest {
  @Test
  public void encodeDate_ShouldWorkProperly() {
    // Arrange
    DateColumn column = DateColumn.of("date", LocalDate.of(2023, 10, 1));

    // Act
    long encoded = TimeRelatedColumnEncodingUtils.encode(column);

    // Assert
    assertThat(encoded).isEqualTo(LocalDate.of(2023, 10, 1).toEpochDay());
  }

  @Test
  public void encodeTime_ShouldWorkProperly() {
    // Arrange
    TimeColumn column = TimeColumn.of("time", LocalTime.of(12, 34, 56, 123_456_000));

    // Act
    long encoded = TimeRelatedColumnEncodingUtils.encode(column);

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
        TimeRelatedColumnEncodingUtils.encode(positiveEpochSecondWithNano);
    long actualPositiveEpochSecondWithZeroNano =
        TimeRelatedColumnEncodingUtils.encode(positiveEpochSecondWithZeroNano);
    long actualNegativeEpochSecondWithNano =
        TimeRelatedColumnEncodingUtils.encode(negativeEpochSecondWithNano);
    long actualNegativeEpochSecondWithZeroNano =
        TimeRelatedColumnEncodingUtils.encode(negativeEpochSecondWithZeroNano);
    long actualEpoch = TimeRelatedColumnEncodingUtils.encode(epoch);

    // Assert
    assertThat(actualPositiveEpochSecondWithNano).isEqualTo(1696163696789L);
    assertThat(actualPositiveEpochSecondWithZeroNano).isEqualTo(1696163696000L);
    assertThat(actualNegativeEpochSecondWithNano).isEqualTo(-23202242704543L);
    assertThat(actualNegativeEpochSecondWithZeroNano).isEqualTo(-23202242704999L);
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
        TimeRelatedColumnEncodingUtils.encode(positiveEpochSecondWithNano);
    long actualPositiveEpochSecondWithZeroNano =
        TimeRelatedColumnEncodingUtils.encode(positiveEpochSecondWithZeroNano);
    long actualNegativeEpochSecondWithNano =
        TimeRelatedColumnEncodingUtils.encode(negativeEpochSecondWithNano);
    long actualNegativeEpochSecondWithZeroNano =
        TimeRelatedColumnEncodingUtils.encode(negativeEpochSecondWithZeroNano);
    long actualEpoch = TimeRelatedColumnEncodingUtils.encode(epoch);

    // Assert
    assertThat(actualPositiveEpochSecondWithNano).isEqualTo(1696163696789L);
    assertThat(actualPositiveEpochSecondWithZeroNano).isEqualTo(1696163696000L);
    assertThat(actualNegativeEpochSecondWithNano).isEqualTo(-23202242704543L);
    assertThat(actualNegativeEpochSecondWithZeroNano).isEqualTo(-23202242704999L);
    assertThat(actualEpoch).isEqualTo(0L);
  }

  @Test
  public void decodeDate_ShouldWorkProperly() {
    // Arrange Act
    LocalDate date =
        TimeRelatedColumnEncodingUtils.decodeDate(LocalDate.of(2023, 10, 1).toEpochDay());

    // Assert
    assertThat(date).isEqualTo(LocalDate.of(2023, 10, 1));
  }

  @Test
  public void decodeTime_ShouldWorkProperly() {
    // Arrange Act
    LocalTime time =
        TimeRelatedColumnEncodingUtils.decodeTime(
            LocalTime.of(12, 34, 56, 123_456_000).toNanoOfDay());

    // Assert
    assertThat(time).isEqualTo(LocalTime.of(12, 34, 56, 123_456_000));
  }

  @Test
  public void decodeTimestamp_ShouldWorkProperly() {
    // Act
    LocalDateTime positiveEpochSecondWithNano =
        TimeRelatedColumnEncodingUtils.decodeTimestamp(1696163696789L);
    LocalDateTime positiveEpochSecondWithZeroNano =
        TimeRelatedColumnEncodingUtils.decodeTimestamp(1696163696000L);
    LocalDateTime negativeEpochSecondWithNano =
        TimeRelatedColumnEncodingUtils.decodeTimestamp(-23202242704543L);
    LocalDateTime negativeEpochSecondWithZeroNano =
        TimeRelatedColumnEncodingUtils.decodeTimestamp(-23202242704999L);
    LocalDateTime epoch = TimeRelatedColumnEncodingUtils.decodeTimestamp(0L);

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
    Instant positiveEpochSecondWithNano =
        TimeRelatedColumnEncodingUtils.decodeTimestampTZ(1696163696789L);
    Instant positiveEpochSecondWithZeroNano =
        TimeRelatedColumnEncodingUtils.decodeTimestampTZ(1696163696000L);
    Instant negativeEpochSecondWithNano =
        TimeRelatedColumnEncodingUtils.decodeTimestampTZ(-23202242704543L);
    Instant negativeEpochSecondWithZeroNano =
        TimeRelatedColumnEncodingUtils.decodeTimestampTZ(-23202242704999L);
    Instant epoch = TimeRelatedColumnEncodingUtils.decodeTimestampTZ(0);

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

  @Test
  public void encodeThenDecodeTimestamp_ShouldPreserveDataIntegrity() {
    // Arrange
    TimestampColumn min = TimestampColumn.of("timestamp", TimestampColumn.MIN_VALUE);
    TimestampColumn max = TimestampColumn.of("timestamp", TimestampColumn.MAX_VALUE);

    // Act Assert
    assertThat(
            TimeRelatedColumnEncodingUtils.decodeTimestamp(
                TimeRelatedColumnEncodingUtils.encode(min)))
        .isEqualTo(TimestampColumn.MIN_VALUE);
    assertThat(
            TimeRelatedColumnEncodingUtils.decodeTimestamp(
                TimeRelatedColumnEncodingUtils.encode(max)))
        .isEqualTo(TimestampColumn.MAX_VALUE);
    LocalDateTime start = LocalDateTime.ofEpochSecond(-2, 0, ZoneOffset.UTC);
    LocalDateTime end = LocalDateTime.ofEpochSecond(3, 0, ZoneOffset.UTC);
    for (LocalDateTime dt = start; dt.isBefore(end); dt = dt.plusNanos(1_000_000)) {
      assertThat(
              TimeRelatedColumnEncodingUtils.decodeTimestamp(
                  TimeRelatedColumnEncodingUtils.encode(TimestampColumn.of("ts", dt))))
          .isEqualTo(dt);
    }
  }

  @Test
  public void encodeThenDecodeTimestampTZ_ShouldPreserverDataIntegrity() {
    // Arrange
    TimestampTZColumn min = TimestampTZColumn.of("timestampTZ", TimestampTZColumn.MIN_VALUE);
    TimestampTZColumn max = TimestampTZColumn.of("timestampTZ", TimestampTZColumn.MAX_VALUE);

    // Act Assert
    assertThat(
            TimeRelatedColumnEncodingUtils.decodeTimestampTZ(
                TimeRelatedColumnEncodingUtils.encode(min)))
        .isEqualTo(TimestampTZColumn.MIN_VALUE);
    assertThat(
            TimeRelatedColumnEncodingUtils.decodeTimestampTZ(
                TimeRelatedColumnEncodingUtils.encode(max)))
        .isEqualTo(TimestampTZColumn.MAX_VALUE);
    Instant start = Instant.ofEpochSecond(-2, 0);
    Instant end = Instant.ofEpochSecond(3, 0);
    for (Instant instant = start; instant.isBefore(end); instant = instant.plusNanos(1_000_000)) {
      assertThat(
              TimeRelatedColumnEncodingUtils.decodeTimestampTZ(
                  TimeRelatedColumnEncodingUtils.encode(TimestampTZColumn.of("ts", instant))))
          .isEqualTo(instant);
    }
  }

  @Test
  public void encodeTimestamp_ShouldPreserveOrder() {
    // Arrange
    List<Long> expectedTimestamps = new ArrayList<>();
    LocalDateTime start = LocalDateTime.ofEpochSecond(-2, 0, ZoneOffset.UTC);
    LocalDateTime end = LocalDateTime.ofEpochSecond(3, 0, ZoneOffset.UTC);
    for (LocalDateTime dt = start; dt.isBefore(end); dt = dt.plusNanos(1_000_000)) {
      expectedTimestamps.add(TimeRelatedColumnEncodingUtils.encode(TimestampColumn.of("ts", dt)));
    }
    long seed = System.currentTimeMillis();
    System.out.printf(
        "The seed used in the %s.%s unit test is %s%n",
        this.getClass().getSimpleName(), "encodeTimestamp_ShouldPreserveOrder", seed);
    ThreadLocal<Random> random = ThreadLocal.withInitial(Random::new);
    random.get().setSeed(seed);

    // Act
    List<Long> shuffledThenSorted = new ArrayList<>(expectedTimestamps);
    Collections.shuffle(shuffledThenSorted, random.get());
    shuffledThenSorted.sort(Comparator.naturalOrder());

    // Assert
    assertThat(shuffledThenSorted).containsExactlyElementsOf(expectedTimestamps);
  }

  @Test
  public void encodeTimestampTZ_ShouldPreserveOrder() {
    // Arrange
    List<Long> expectedTimestamps = new ArrayList<>();
    Instant start = Instant.ofEpochSecond(-2, 0);
    Instant end = Instant.ofEpochSecond(3, 0);
    for (Instant instant = start; instant.isBefore(end); instant = instant.plusNanos(1_000_000)) {
      expectedTimestamps.add(
          TimeRelatedColumnEncodingUtils.encode(TimestampTZColumn.of("ts", instant)));
    }
    long seed = System.currentTimeMillis();
    System.out.printf(
        "The seed used in the %s.%s unit test is %s%n",
        this.getClass().getSimpleName(), "encodeTimestampTZ_ShouldPreserveOrder", seed);
    ThreadLocal<Random> random = ThreadLocal.withInitial(Random::new);
    random.get().setSeed(seed);

    // Act
    List<Long> shuffledThenSorted = new ArrayList<>(expectedTimestamps);
    Collections.shuffle(shuffledThenSorted, random.get());
    shuffledThenSorted.sort(Comparator.naturalOrder());

    // Assert
    assertThat(shuffledThenSorted).containsExactlyElementsOf(expectedTimestamps);
  }
}
