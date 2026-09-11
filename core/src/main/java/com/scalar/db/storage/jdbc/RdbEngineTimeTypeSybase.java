package com.scalar.db.storage.jdbc;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Converts time related columns to the text representation that SAP ASE accepts for its {@code
 * date}, {@code bigtime} and {@code bigdatetime} columns.
 *
 * <p>The values are passed as text rather than as {@code java.sql.Date}/{@code java.sql.Timestamp}
 * for the same reason as in {@link RdbEngineTimeTypeSqlServer}: those types go through {@code
 * java.util.GregorianCalendar}, which applies the Julian to Gregorian transition and therefore
 * shifts dates before October 15, 1582 by ten days. ScalarDB allows dates from the year 1000, so
 * that range is reachable.
 *
 * <p>The date part uses the unseparated {@code yyyyMMdd} form, which Transact-SQL reads the same
 * way regardless of the server's {@code dateformat} and language settings.
 */
public class RdbEngineTimeTypeSybase
    implements RdbEngineTimeTypeStrategy<String, String, String, String> {

  private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.BASIC_ISO_DATE;
  private static final DateTimeFormatter TIME_FORMATTER =
      DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS");
  private static final DateTimeFormatter TIMESTAMP_FORMATTER =
      DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss.SSSSSS");

  @Override
  public String convert(LocalDate date) {
    return date.format(DATE_FORMATTER);
  }

  @Override
  public String convert(LocalTime time) {
    return time.format(TIME_FORMATTER);
  }

  @Override
  public String convert(LocalDateTime timestamp) {
    return timestamp.format(TIMESTAMP_FORMATTER);
  }

  @Override
  public String convert(OffsetDateTime timestampTZ) {
    // SAP ASE has no time zone aware type, so the instant is stored in UTC in a bigdatetime column.
    return timestampTZ
        .withOffsetSameInstant(ZoneOffset.UTC)
        .toLocalDateTime()
        .format(TIMESTAMP_FORMATTER);
  }
}
