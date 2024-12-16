package com.scalar.db.storage;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;

import com.google.common.base.Strings;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;

public final class ColumnEncodingUtils {
  // A formatter similar to the ISO date time format (DateTimeFormatter.ISO_DATE_TIME)
  // '2011-12-03T10:15:30.1234' where all
  // non-numerical characters are removed to reduce the memory footprint.
  public static final DateTimeFormatter TIMESTAMP_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendValue(YEAR, 4)
          .appendValue(MONTH_OF_YEAR, 2)
          .appendValue(DAY_OF_MONTH, 2)
          .appendValue(HOUR_OF_DAY, 2)
          .appendValue(MINUTE_OF_HOUR, 2)
          .appendValue(SECOND_OF_MINUTE, 2)
          .optionalStart()
          .appendFraction(NANO_OF_SECOND, 0, 9, false)
          .toFormatter()
          .withChronology(IsoChronology.INSTANCE)
          .withResolverStyle(ResolverStyle.STRICT);
  public static final DateTimeFormatter TIMESTAMPTZ_FORMATTER =
      TIMESTAMP_FORMATTER.withZone(ZoneOffset.UTC);

  private ColumnEncodingUtils() {}

  public static long encode(DateColumn column) {
    assert column.getDateValue() != null;
    return column.getDateValue().toEpochDay();
  }

  public static long encode(TimeColumn column) {
    assert column.getTimeValue() != null;
    return column.getTimeValue().toNanoOfDay();
  }

  public static String encode(TimestampColumn column) {
    assert column.getTimestampValue() != null;
    return TIMESTAMP_FORMATTER.format(column.getTimestampValue());
  }

  public static String encode(TimestampTZColumn column) {
    assert column.getTimestampTZValue() != null;
    return TIMESTAMPTZ_FORMATTER.format(column.getTimestampTZValue());
  }

  public static LocalDate decodeDate(long epochDay) {
    return LocalDate.ofEpochDay(epochDay);
  }

  public static LocalTime decodeTime(long nanoOfDay) {
    return LocalTime.ofNanoOfDay(nanoOfDay);
  }

  public static LocalDateTime decodeTimestamp(String text) {
    return Strings.isNullOrEmpty(text)
        ? null
        : TIMESTAMP_FORMATTER.parse(text, LocalDateTime::from);
  }

  public static Instant decodeTimestampTZ(String text) {
    return Strings.isNullOrEmpty(text) ? null : TIMESTAMPTZ_FORMATTER.parse(text, Instant::from);
  }
}
