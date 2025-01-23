package com.scalar.db.storage.jdbc;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import microsoft.sql.DateTimeOffset;

public class RdbEngineTimeTypeSqlServer
    implements RdbEngineTimeTypeStrategy<String, LocalTime, String, DateTimeOffset> {

  @Override
  public String convert(LocalDate date) {
    // Pass the date value as text otherwise the dates before the Julian to Gregorian Calendar
    // transition (October 15, 1582) will be offset by 10 days.
    return date.format(DateTimeFormatter.BASIC_ISO_DATE);
  }

  @Override
  public LocalTime convert(LocalTime time) {
    return time;
  }

  @Override
  public String convert(LocalDateTime timestamp) {
    // Pass the timestamp value as text otherwise the dates before the Julian to Gregorian Calendar
    // transition (October 15, 1582) will be offset by 10 days.
    return timestamp.format(DateTimeFormatter.ISO_DATE_TIME);
  }

  @Override
  public DateTimeOffset convert(OffsetDateTime timestampTZ) {
    // When using SQLServer DATETIMEOFFSET data type, we should use the SQLServer JDBC driver's
    // microsoft.sql.DateTimeOffset class for encoding the value.
    return DateTimeOffset.valueOf(timestampTZ);
  }
}
