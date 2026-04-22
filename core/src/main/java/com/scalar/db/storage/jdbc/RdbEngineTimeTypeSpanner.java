package com.scalar.db.storage.jdbc;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
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


class RdbEngineTimeTypeSpanner
    implements RdbEngineTimeTypeStrategy<
        LocalDate, OffsetDateTime, OffsetDateTime, OffsetDateTime> {
  /**
   * A formatter for a Spanner DATE literal. The format is "YYYY-MM-DD". For example, "2020-03-04".
   */
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

  private final JdbcConfig config;

  public RdbEngineTimeTypeSpanner(JdbcConfig config) {
    this.config = config;
  }

  @Override
  public LocalDate convert(LocalDate date) {
    return date;
  }

  @Override
  public OffsetDateTime convert(LocalTime time) {
    return OffsetDateTime.of(
        config.getSpannerTimeColumnDefaultDateComponent(), time, ZoneOffset.UTC);
  }

  @Override
  public OffsetDateTime convert(LocalDateTime timestamp) {
    return timestamp.atOffset(ZoneOffset.UTC);
  }

  @Override
  public OffsetDateTime convert(OffsetDateTime timestampTZ) {
    return timestampTZ;
  }
}
