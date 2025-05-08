package com.scalar.db.storage.jdbc;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;

class RdbEngineTimeTypeDb2
    implements RdbEngineTimeTypeStrategy<String, LocalDateTime, String, String> {
  /** A formatter for a Db2 DATE literal. The format is "YYYY-MM-DD". For example, "2020-03-04". */
  public static final DateTimeFormatter DATE_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendValue(YEAR, 4, 4, SignStyle.NEVER)
          .appendLiteral('-')
          .appendValue(MONTH_OF_YEAR, 2)
          .appendLiteral('-')
          .appendValue(DAY_OF_MONTH, 2)
          .toFormatter()
          .withResolverStyle(ResolverStyle.STRICT)
          .withChronology(IsoChronology.INSTANCE);
  /**
   * A formatter for a Db2 TIMESTAMP type literal. The format is "YYYY-MM-DD HH:MM:SS[.FFFFFF]". For
   * example, "2020-03-04 12:34:56.123". The fractional second is optional.
   */
  public static final DateTimeFormatter TIMESTAMP_FORMATTER =
      new DateTimeFormatterBuilder()
          .append(DATE_FORMATTER)
          .appendLiteral(' ')
          .appendValue(HOUR_OF_DAY, 2)
          .appendLiteral(':')
          .appendValue(MINUTE_OF_HOUR, 2)
          .appendLiteral(':')
          .appendValue(SECOND_OF_MINUTE, 2)
          .optionalStart()
          .appendFraction(NANO_OF_SECOND, 0, 3, true)
          .toFormatter()
          .withZone(ZoneOffset.UTC);

  private final JdbcConfig config;

  public RdbEngineTimeTypeDb2(JdbcConfig config) {
    this.config = config;
  }

  @Override
  public String convert(LocalDate date) {
    return DATE_FORMATTER.format(date);
  }

  @Override
  public LocalDateTime convert(LocalTime time) {
    return LocalDateTime.of(config.getDb2TimeColumnDefaultDateComponent(), time);
  }

  @Override
  public String convert(LocalDateTime timestamp) {
    return TIMESTAMP_FORMATTER.format(timestamp);
  }

  @Override
  public String convert(OffsetDateTime timestamp) {
    return TIMESTAMP_FORMATTER.format(timestamp);
  }
}
