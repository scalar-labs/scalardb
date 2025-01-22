package com.scalar.db.storage.jdbc;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

/**
 * An interface to convert time related types columns backing Java 8 time type to a corresponding
 * type that can be used in prepared statement by the JDBC driver
 *
 * @param <T_DATE> the converted type for LocalDate
 * @param <T_TIME> the converted type for LocalTime
 * @param <T_TIMESTAMP> the converted type for LocalDateTime
 * @param <T_TIMESTAMPTZ> the converted type for OffsetDateTime
 */
public interface RdbEngineTimeTypeStrategy<T_DATE, T_TIME, T_TIMESTAMP, T_TIMESTAMPTZ> {
  T_DATE convert(LocalDate date);

  T_TIME convert(LocalTime time);

  T_TIMESTAMP convert(LocalDateTime timestamp);

  T_TIMESTAMPTZ convert(OffsetDateTime timestampTZ);
}
