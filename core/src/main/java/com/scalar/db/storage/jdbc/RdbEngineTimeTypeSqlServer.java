package com.scalar.db.storage.jdbc;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

public class RdbEngineTimeTypeSqlServer
    implements RdbEngineTimeTypeStrategy<String, LocalTime, String, String> {

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
  public String convert(OffsetDateTime timestampTZ) {
    // Pass the timestamptz value as text otherwise the driver encodes the dates before the Julian
    // to Gregorian Calendar transition (October 15, 1582) with the Julian calendar, storing them
    // up to 10 days off.
    return timestampTZ.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
  }
}
