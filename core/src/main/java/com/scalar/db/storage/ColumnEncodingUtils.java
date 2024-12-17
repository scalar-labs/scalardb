package com.scalar.db.storage;

import com.scalar.db.io.DateColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;

public final class ColumnEncodingUtils {
  private ColumnEncodingUtils() {}

  public static long encode(DateColumn column) {
    assert column.getDateValue() != null;
    return column.getDateValue().toEpochDay();
  }

  public static long encode(TimeColumn column) {
    assert column.getTimeValue() != null;
    return column.getTimeValue().toNanoOfDay();
  }

  public static long encode(TimestampColumn column) {
    assert column.getTimestampValue() != null;

    return encodeInstant(column.getTimestampValue().toInstant(ZoneOffset.UTC));
  }

  @SuppressWarnings("JavaInstantGetSecondsGetNano")
  public static long encode(TimestampTZColumn column) {
    assert column.getTimestampTZValue() != null;

    return encodeInstant(column.getTimestampTZValue());
  }

  private static long encodeInstant(Instant instant) {
    long encoded = instant.getEpochSecond() * 1000;
    // Subtract the nanoOfSeconds when the epochSecond is negative, that is for a date before 1970
    if (encoded >= 0) {
      encoded += instant.getNano() / 1_000_000;
    } else {
      encoded -= instant.getNano() / 1_000_000;
    }
    return encoded;
  }

  public static LocalDate decodeDate(long epochDay) {
    return LocalDate.ofEpochDay(epochDay);
  }

  public static LocalTime decodeTime(long nanoOfDay) {
    return LocalTime.ofNanoOfDay(nanoOfDay);
  }

  public static LocalDateTime decodeTimestamp(long longTimestamp) {
    long nanoOfSeconds = longTimestamp % 1000 * 1_000_000;
    // Invert the nanoOfSeconds when the encoded instant is negative, that is for a date before 1970
    if (longTimestamp < 0) {
      nanoOfSeconds *= -1;
    }
    return LocalDateTime.ofEpochSecond(
        longTimestamp / 1000, Math.toIntExact(nanoOfSeconds), ZoneOffset.UTC);
  }

  public static Instant decodeTimestampTZ(long longTimestampTZ) {
    long nanoOfSeconds = longTimestampTZ % 1000 * 1_000_000;
    // Invert the nanoOfSeconds when the encoded instant is negative, that is for a date before 1970
    if (longTimestampTZ < 0) {
      nanoOfSeconds *= -1;
    }
    return Instant.ofEpochSecond(longTimestampTZ / 1000, nanoOfSeconds);
  }
}
