package com.scalar.db.storage.jdbc;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

public class RdbEngineTimeTypePostgresql
    implements RdbEngineTimeTypeStrategy<LocalDate, LocalTime, LocalDateTime, OffsetDateTime> {
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
  public OffsetDateTime convert(OffsetDateTime timestampTZ) {
    return timestampTZ;
  }
}
