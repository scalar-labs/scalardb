package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RdbEngineTimeTypeSpannerTest {

  private RdbEngineTimeTypeSpanner strategy;
  private JdbcConfig config;

  @BeforeEach
  void setUp() {
    config = mock(JdbcConfig.class);
    when(config.getSpannerTimeColumnDefaultDateComponent()).thenReturn(LocalDate.of(1970, 1, 1));
    strategy = new RdbEngineTimeTypeSpanner(config);
  }

  @Test
  void convert_LocalDate_ShouldReturnSameDate() {
    LocalDate date = LocalDate.of(2024, 6, 15);
    assertThat(strategy.convert(date)).isEqualTo(date);
  }

  @Test
  void convert_LocalTime_ShouldReturnOffsetDateTimeAtEpochDateUtc() {
    LocalTime time = LocalTime.of(14, 30, 45, 123456789);
    OffsetDateTime result = strategy.convert(time);
    assertThat(result.toLocalDate()).isEqualTo(LocalDate.of(1970, 1, 1));
    assertThat(result.toLocalTime()).isEqualTo(time);
    assertThat(result.getOffset()).isEqualTo(ZoneOffset.UTC);
  }

  @Test
  void convert_LocalTime_Midnight_ShouldReturnEpochMidnightUtc() {
    LocalTime midnight = LocalTime.MIDNIGHT;
    OffsetDateTime result = strategy.convert(midnight);
    assertThat(result).isEqualTo(OffsetDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC));
  }

  @Test
  void convert_LocalTime_MaxTime_ShouldReturnEpochMaxTimeUtc() {
    LocalTime maxTime = LocalTime.of(23, 59, 59, 999999999);
    OffsetDateTime result = strategy.convert(maxTime);
    assertThat(result.toLocalTime()).isEqualTo(maxTime);
    assertThat(result.toLocalDate()).isEqualTo(LocalDate.of(1970, 1, 1));
  }

  @Test
  void convert_LocalTime_ShouldUseConfiguredDateComponent() {
    LocalDate customDate = LocalDate.of(2024, 1, 1);
    when(config.getSpannerTimeColumnDefaultDateComponent()).thenReturn(customDate);
    LocalTime time = LocalTime.of(12, 34, 56);
    OffsetDateTime result = strategy.convert(time);
    assertThat(result.toLocalDate()).isEqualTo(customDate);
    assertThat(result.toLocalTime()).isEqualTo(time);
    assertThat(result.getOffset()).isEqualTo(ZoneOffset.UTC);
  }

  @Test
  void convert_LocalDateTime_ShouldReturnSameTimestamp() {
    LocalDateTime timestamp = LocalDateTime.of(2024, 6, 15, 14, 30, 45);
    assertThat(strategy.convert(timestamp)).isEqualTo(OffsetDateTime.of(timestamp, ZoneOffset.UTC));
  }

  @Test
  void convert_OffsetDateTime_ShouldReturnSameTimestampTz() {
    OffsetDateTime timestampTz = OffsetDateTime.of(2024, 6, 15, 14, 30, 45, 0, ZoneOffset.UTC);
    assertThat(strategy.convert(timestampTz)).isEqualTo(timestampTz);
  }
}
