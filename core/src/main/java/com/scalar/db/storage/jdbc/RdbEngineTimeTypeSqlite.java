package com.scalar.db.storage.jdbc;

import com.scalar.db.util.TimeRelatedColumnEncodingUtils;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

public class RdbEngineTimeTypeSqlite implements RdbEngineTimeTypeStrategy<Long, Long, Long, Long> {

  @Override
  public Long convert(LocalDate date) {
    return TimeRelatedColumnEncodingUtils.encode(date);
  }

  @Override
  public Long convert(LocalTime time) {
    return TimeRelatedColumnEncodingUtils.encode(time);
  }

  @Override
  public Long convert(LocalDateTime timestamp) {
    return TimeRelatedColumnEncodingUtils.encode(timestamp);
  }

  @Override
  public Long convert(OffsetDateTime timestampTZ) {
    return TimeRelatedColumnEncodingUtils.encode(timestampTZ.toInstant());
  }
}
