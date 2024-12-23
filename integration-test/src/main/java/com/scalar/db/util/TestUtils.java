package com.scalar.db.util;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Assertions;

public final class TestUtils {

  public static final int MAX_TEXT_COUNT = 20;
  public static final int MAX_BLOB_LENGTH = 20;

  private TestUtils() {}

  public static Column<?> getColumnWithRandomValue(
      Random random, String columnName, DataType dataType) {
    return getColumnWithRandomValue(random, columnName, dataType, false);
  }

  public static Column<?> getColumnWithRandomValue(
      Random random, String columnName, DataType dataType, boolean allowEmpty) {
    switch (dataType) {
      case BOOLEAN:
        return BooleanColumn.of(columnName, random.nextBoolean());
      case INT:
        return IntColumn.of(columnName, random.nextInt());
      case BIGINT:
        return BigIntColumn.of(columnName, nextBigInt(random));
      case FLOAT:
        return FloatColumn.of(columnName, nextFloat(random));
      case DOUBLE:
        return DoubleColumn.of(columnName, nextDouble(random));
      case TEXT:
        int count =
            allowEmpty ? random.nextInt(MAX_TEXT_COUNT) : random.nextInt(MAX_TEXT_COUNT - 1) + 1;
        return TextColumn.of(
            columnName, RandomStringUtils.random(count, 0, 0, true, true, null, random));
      case BLOB:
        int length =
            allowEmpty ? random.nextInt(MAX_BLOB_LENGTH) : random.nextInt(MAX_BLOB_LENGTH - 1) + 1;
        byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        return BlobColumn.of(columnName, bytes);
      case DATE:
        return DateColumn.of(columnName, nextDate(random));
      case TIME:
        return TimeColumn.of(columnName, nextTime(random));
      case TIMESTAMP:
        return TimestampColumn.of(columnName, nextTimestamp(random));
      case TIMESTAMPTZ:
        return TimestampTZColumn.of(columnName, nextTimestampTZ(random));
      default:
        throw new AssertionError();
    }
  }

  public static long nextBigInt(Random random) {
    return random
        .longs(1, BigIntColumn.MIN_VALUE, (BigIntColumn.MAX_VALUE + 1))
        .findFirst()
        .orElse(0);
  }

  public static float nextFloat(Random random) {
    return (float) random.doubles(1, Float.MIN_VALUE, Float.MAX_VALUE).findFirst().orElse(0.0d);
  }

  public static double nextDouble(Random random) {
    return random.doubles(1, Double.MIN_VALUE, Double.MAX_VALUE).findFirst().orElse(0.0d);
  }

  public static LocalDate nextDate(Random random) {
    return nextLocalDate(
        random, DateColumn.MIN_VALUE.toEpochDay(), DateColumn.MAX_VALUE.toEpochDay());
  }

  public static LocalTime nextTime(Random random) {
    return nextLocalTime(
        random,
        TimeColumn.MIN_VALUE.toNanoOfDay(),
        TimeColumn.MAX_VALUE.toNanoOfDay(),
        TimeColumn.FRACTIONAL_SECONDS_PRECISION_IN_NANOSECONDS);
  }

  public static LocalDateTime nextTimestamp(Random random) {
    LocalDate date =
        nextLocalDate(
            random,
            TimestampColumn.MIN_VALUE.getLong(ChronoField.EPOCH_DAY),
            TimestampColumn.MAX_VALUE.getLong(ChronoField.EPOCH_DAY));
    LocalTime time =
        nextLocalTime(
            random,
            TimestampColumn.MIN_VALUE.getLong(ChronoField.NANO_OF_DAY),
            TimestampColumn.MAX_VALUE.getLong(ChronoField.NANO_OF_DAY),
            TimestampColumn.FRACTIONAL_SECONDS_PRECISION_IN_NANOSECONDS);

    return LocalDateTime.of(date, time);
  }

  public static Instant nextTimestampTZ(Random random) {
    LocalDate date =
        nextLocalDate(
            random,
            TimestampTZColumn.MIN_VALUE.atOffset(ZoneOffset.UTC).getLong(ChronoField.EPOCH_DAY),
            TimestampTZColumn.MAX_VALUE.atOffset(ZoneOffset.UTC).getLong(ChronoField.EPOCH_DAY));
    LocalTime time =
        nextLocalTime(
            random,
            TimestampTZColumn.MIN_VALUE.atOffset(ZoneOffset.UTC).getLong(ChronoField.NANO_OF_DAY),
            TimestampTZColumn.MAX_VALUE.atOffset(ZoneOffset.UTC).getLong(ChronoField.NANO_OF_DAY),
            TimestampTZColumn.FRACTIONAL_SECONDS_PRECISION_IN_NANOSECONDS);

    return LocalDateTime.of(date, time).toInstant(ZoneOffset.UTC);
  }

  private static LocalDate nextLocalDate(Random random, long minEpochDay, long maxEpochDay) {
    long epochDay = random.longs(1, minEpochDay, maxEpochDay + 1).findFirst().orElse(0);
    return LocalDate.ofEpochDay(epochDay);
  }

  @SuppressWarnings("JavaLocalTimeGetNano")
  public static LocalTime nextLocalTime(
      Random random, long minNanoOfDay, long maxNanoOfDay, int resolutionInNanos) {
    long nanoOfDay = random.longs(1, minNanoOfDay, maxNanoOfDay + 1).findFirst().orElse(0);
    LocalTime time = LocalTime.ofNanoOfDay(nanoOfDay);

    return time.withNano(time.getNano() / resolutionInNanos * resolutionInNanos);
  }

  public static Column<?> getColumnWithMinValue(String columnName, DataType dataType) {
    return getColumnWithMinValue(columnName, dataType, false);
  }

  public static Column<?> getColumnWithMinValue(
      String columnName, DataType dataType, boolean allowEmpty) {
    switch (dataType) {
      case BIGINT:
        return BigIntColumn.of(columnName, BigIntValue.MIN_VALUE);
      case INT:
        return IntColumn.of(columnName, Integer.MIN_VALUE);
      case FLOAT:
        return FloatColumn.of(columnName, Float.MIN_VALUE);
      case DOUBLE:
        return DoubleColumn.of(columnName, Double.MIN_VALUE);
      case BLOB:
        return BlobColumn.of(columnName, allowEmpty ? new byte[0] : new byte[] {0x00});
      case TEXT:
        return TextColumn.of(columnName, allowEmpty ? "" : "\u0001");
      case BOOLEAN:
        return BooleanColumn.of(columnName, false);
      case DATE:
        return DateColumn.of(columnName, DateColumn.MIN_VALUE);
      case TIME:
        return TimeColumn.of(columnName, TimeColumn.MIN_VALUE);
      case TIMESTAMP:
        return TimestampColumn.of(columnName, TimestampColumn.MIN_VALUE);
      case TIMESTAMPTZ:
        return TimestampTZColumn.of(columnName, TimestampTZColumn.MIN_VALUE);
      default:
        throw new AssertionError();
    }
  }

  public static Column<?> getColumnWithMaxValue(String columnName, DataType dataType) {
    switch (dataType) {
      case BIGINT:
        return BigIntColumn.of(columnName, BigIntValue.MAX_VALUE);
      case INT:
        return IntColumn.of(columnName, Integer.MAX_VALUE);
      case FLOAT:
        return FloatColumn.of(columnName, Float.MAX_VALUE);
      case DOUBLE:
        return DoubleColumn.of(columnName, Double.MAX_VALUE);
      case BLOB:
        byte[] blobBytes = new byte[MAX_BLOB_LENGTH];
        Arrays.fill(blobBytes, (byte) 0xff);
        return BlobColumn.of(columnName, blobBytes);
      case TEXT:
        StringBuilder builder = new StringBuilder();
        IntStream.range(0, MAX_TEXT_COUNT).forEach(i -> builder.append(Character.MAX_VALUE));
        return TextColumn.of(columnName, builder.toString());
      case BOOLEAN:
        return BooleanColumn.of(columnName, true);
      case DATE:
        return DateColumn.of(columnName, DateColumn.MAX_VALUE);
      case TIME:
        return TimeColumn.of(columnName, TimeColumn.MAX_VALUE);
      case TIMESTAMP:
        return TimestampColumn.of(columnName, TimestampColumn.MAX_VALUE);
      case TIMESTAMPTZ:
        return TimestampTZColumn.of(columnName, TimestampTZColumn.MAX_VALUE);
      default:
        throw new AssertionError();
    }
  }

  public static List<BooleanColumn> booleanColumns(String columnName) {
    return Arrays.asList(BooleanColumn.of(columnName, false), BooleanColumn.of(columnName, true));
  }

  public static Order reverseOrder(Order order) {
    switch (order) {
      case ASC:
        return Order.DESC;
      case DESC:
        return Order.ASC;
      default:
        throw new AssertionError();
    }
  }

  public static Ordering getOrdering(String columnName, Order order) {
    switch (order) {
      case ASC:
        return Ordering.asc(columnName);
      case DESC:
        return Ordering.desc(columnName);
      default:
        throw new AssertionError();
    }
  }

  /**
   * Find and return an expected result that matches the result
   *
   * @param result a result
   * @param expectedResults a list of expected results
   * @return the first {@link ExpectedResult} that matches the {@link Result}, otherwise return null
   */
  private static ExpectedResult findFirstMatchingResult(
      Result result, List<ExpectedResult> expectedResults) {
    for (ExpectedResult er : expectedResults) {
      if (er.equalsResult(result)) {
        return er;
      }
    }
    return null;
  }

  /**
   * Asserts the actualResults and expectedResults lists elements are equals without taking the list
   * order into consideration
   *
   * @param actualResults a list of results
   * @param expectedResults a list of expected results
   */
  public static void assertResultsContainsExactlyInAnyOrder(
      List<Result> actualResults, List<ExpectedResult> expectedResults) {
    expectedResults = new ArrayList<>(expectedResults);
    for (Result actualResult : actualResults) {
      ExpectedResult matchedExpectedResult = findFirstMatchingResult(actualResult, expectedResults);
      if (matchedExpectedResult == null) {
        Assertions.fail("This actual result is not expected: " + actualResult);
      } else {
        expectedResults.remove(matchedExpectedResult);
      }
    }
    if (!expectedResults.isEmpty()) {
      Assertions.fail(
          "The given expected results are missing from the actual results" + expectedResults);
    }
  }

  /**
   * Asserts the actualResults are a subset of expectedResults. In other words, actualResults are
   * contained into expectedResults
   *
   * @param actualResults of list of results
   * @param expectedResults a list of expected results
   */
  public static void assertResultsAreASubsetOf(
      List<Result> actualResults, List<ExpectedResult> expectedResults) {
    expectedResults = new ArrayList<>(expectedResults);
    for (Result actualResult : actualResults) {
      ExpectedResult matchedExpectedResult = findFirstMatchingResult(actualResult, expectedResults);
      if (matchedExpectedResult == null) {
        Assertions.fail("The actual result " + actualResult + " is not expected");
      } else {
        expectedResults.remove(matchedExpectedResult);
      }
    }
  }

  /** Utility class used in testing to facilitate the comparison of {@link Result} */
  public static class ExpectedResult {
    private final ImmutableSet<Column<?>> columns;

    private ExpectedResult(ExpectedResultBuilder builder) {
      this.columns = ImmutableSet.copyOf(builder.columns);
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public Set<Column<?>> getColumns() {
      return columns;
    }

    /**
     * Verify the equality with a {@link Result} but ignores the columns ordering.
     *
     * @param other a Result
     * @return true if this object is equal to the other
     */
    public boolean equalsResult(Result other) {
      if (columns.size() != other.getColumns().size()) {
        return false;
      }

      // Columns ordering is not taken into account
      for (Column<?> column : columns) {
        if (!column.equals(other.getColumns().get(column.getName()))) {
          return false;
        }
      }

      return true;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("columns", columns).toString();
    }

    public static class ExpectedResultBuilder {
      private final List<Column<?>> columns = new ArrayList<>();

      public ExpectedResultBuilder column(Column<?> column) {
        columns.add(column);
        return this;
      }

      public ExpectedResultBuilder columns(Collection<Column<?>> columns) {
        this.columns.addAll(columns);
        return this;
      }

      public ExpectedResult build() {
        return new ExpectedResult(this);
      }
    }
  }
}
