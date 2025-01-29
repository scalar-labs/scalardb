package com.scalar.db.storage.jdbc;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

public class RdbEngineTimeTypeMysql
    implements RdbEngineTimeTypeStrategy<LocalDate, LocalTime, LocalDateTime, LocalDateTime> {
  @Override
  public LocalDate convert(LocalDate date) {
    return date;
  }

  @Override
  public LocalTime convert(LocalTime time) {
    return time;
  }

  @Override
  public LocalDateTime convert(LocalDateTime timestamp) {
    return timestamp;
  }

  @Override
  public LocalDateTime convert(OffsetDateTime timestampTZ) {
    // Encoding as an OffsetDateTime result in the time being offset arbitrarily depending on the
    // client, session or server time zone. So we encode it as a LocalDateTime instead.
    return timestampTZ.toLocalDateTime();
  }
}
