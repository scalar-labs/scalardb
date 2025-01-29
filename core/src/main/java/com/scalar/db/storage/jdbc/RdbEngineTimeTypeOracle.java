package com.scalar.db.storage.jdbc;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

public class RdbEngineTimeTypeOracle
    implements RdbEngineTimeTypeStrategy<LocalDate, LocalDateTime, LocalDateTime, OffsetDateTime> {
  private final JdbcConfig config;

  public RdbEngineTimeTypeOracle(JdbcConfig config) {
    this.config = config;
  }

  @Override
  public LocalDate convert(LocalDate date) {
    return date;
  }

  @Override
  public LocalDateTime convert(LocalTime time) {
    return LocalDateTime.of(config.getOracleTimeColumnDefaultDateComponent(), time);
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
