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

/**
 * This class provides utility methods for encoding and decoding time related column values for
 * DynamoDB, CosmosDB and SQLite
 */
public final class ColumnEncodingUtils {
  private ColumnEncodingUtils() {}

  public static long encode(DateColumn column) {
    assert column.getDateValue() != null;
    return column.getDateValue().toEpochDay();
  }

  public static LocalDate decodeDate(long epochDay) {
    return LocalDate.ofEpochDay(epochDay);
  }

  public static long encode(TimeColumn column) {
    assert column.getTimeValue() != null;
    return column.getTimeValue().toNanoOfDay();
  }

  public static LocalTime decodeTime(long nanoOfDay) {
    return LocalTime.ofNanoOfDay(nanoOfDay);
  }

  public static long encode(TimestampColumn column) {
    assert column.getTimestampValue() != null;
    return encodeInstant(column.getTimestampValue().toInstant(ZoneOffset.UTC));
  }

  public static LocalDateTime decodeTimestamp(long longTimestamp) {
    long milliOfSecond = longTimestamp % 1000;
    if (longTimestamp < 0) {
      // Convert the complement of the millisecondOfSecond to the actual millisecondOfSecond
      milliOfSecond += 1000 - 1;
    }
    return LocalDateTime.ofEpochSecond(
        longTimestamp / 1000, Math.toIntExact(milliOfSecond * 1_000_000), ZoneOffset.UTC);
  }

  public static long encode(TimestampTZColumn column) {
    assert column.getTimestampTZValue() != null;
    return encodeInstant(column.getTimestampTZValue());
  }

  public static Instant decodeTimestampTZ(long longTimestampTZ) {
    long milliOfSecond = longTimestampTZ % 1000;

    if (longTimestampTZ < 0) {
      // Convert the complement of the millisecondOfSecond to the actual millisecondOfSecond
      milliOfSecond += 1000 - 1;
    }
    return Instant.ofEpochSecond(longTimestampTZ / 1000, milliOfSecond * 1_000_000);
  }

  @SuppressWarnings("JavaInstantGetSecondsGetNano")
  private static long encodeInstant(Instant instant) {
    // Encoding format on a long : <epochSecond><millisecondOfSecond>
    // The rightmost three digits are the number of milliseconds from the start of the
    // second, the other digits on the left are the epochSecond.
    // If the epochSecond is negative (for a date before 1970), to preserve the timestamp ordering
    // the
    // millisecondOfSecond is converted to its complement
    // with the formula "complementOfN = 1000 - 1 - N", where N is the millisecondOfSecond.
    //
    // For example:
    // - if epochSecond=12345 and millisecondOfSecond=789, then the encoded value will be 12345789
    // - if epochSecond=-12345 and millisecondOfSecond=789, then
    //   millisecondOfSecondComplement = 1000 - 1 - 789 = 210. So the encoded value will be
    //   -12345210.

    long encoded = instant.getEpochSecond() * 1000;
    if (encoded < 0) {
      // Convert the nanosecondOfSecond to millisecondOfSecond, compute its complement and subtract
      // it
      encoded -= 1000 - 1 - instant.getNano() / 1_000_000;
    } else {
      encoded += instant.getNano() / 1_000_000;
    }
    return encoded;
  }
}
