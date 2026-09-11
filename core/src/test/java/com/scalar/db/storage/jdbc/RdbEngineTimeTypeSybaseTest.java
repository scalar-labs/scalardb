package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class RdbEngineTimeTypeSybaseTest {

  private final RdbEngineTimeTypeSybase timeType = new RdbEngineTimeTypeSybase();

  @Test
  void convert_Date_ShouldUseTheUnseparatedForm() {
    // The unseparated form is read the same way whatever the server's dateformat setting is
    assertThat(timeType.convert(LocalDate.of(2020, 1, 2))).isEqualTo("20200102");
    // A date before the Julian to Gregorian transition, which is why text is used at all
    assertThat(timeType.convert(LocalDate.of(1000, 1, 2))).isEqualTo("10000102");
  }

  @Test
  void convert_Time_ShouldKeepMicroseconds() {
    assertThat(timeType.convert(LocalTime.of(1, 2, 3, 123_456_000))).isEqualTo("01:02:03.123456");
    assertThat(timeType.convert(LocalTime.of(0, 0))).isEqualTo("00:00:00.000000");
  }

  @Test
  void convert_Timestamp_ShouldKeepMicroseconds() {
    assertThat(timeType.convert(LocalDateTime.of(2020, 1, 2, 3, 4, 5, 123_456_000)))
        .isEqualTo("20200102 03:04:05.123456");
  }

  @Test
  void convert_TimestampTZ_ShouldNormalizeToUtc() {
    // ASE has no time zone aware type, so the instant is stored in UTC
    assertThat(timeType.convert(OffsetDateTime.of(2020, 1, 2, 3, 4, 5, 0, ZoneOffset.ofHours(9))))
        .isEqualTo("20200101 18:04:05.000000");
    assertThat(timeType.convert(OffsetDateTime.of(2020, 1, 2, 3, 4, 5, 0, ZoneOffset.UTC)))
        .isEqualTo("20200102 03:04:05.000000");
  }
}
