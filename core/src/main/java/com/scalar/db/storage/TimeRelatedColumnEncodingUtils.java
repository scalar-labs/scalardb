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
 * This class provides utility methods for encoding and decoding time related column value for
 * DynamoDB, CosmosDB and SQLite
 */
public final class TimeRelatedColumnEncodingUtils {
  private TimeRelatedColumnEncodingUtils() {}

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
    // A Java Instant object is internally represented with two components:
    // - the epoch second: the number of seconds since the epoch date 1970-01-01
    // - the nano of second: the number of nanoseconds since the start of the second.
    //
    // The range and precision of a ScalarDB timestamp and timestampTZ is from January 1st of 1999
    // to December 31st of 9999, and its precision is up to 1 millisecond. Since the range is
    // smaller and precision less precise, both components can be encoded into a single long value.
    // The long value format is "<epochSecond><millisecondOfSecond>", where the rightmost 3 digits
    // are the millisecondOfSecond with a value from 000 to 999, the other digits on the left are
    // the epochSecond.
    // To preserve the timestamp ordering if the epochSecond is negative (in case an instant is
    // before 1970), the millisecondOfSecond is converted to its complement with the
    // formula "complementOfN = 1000 - 1 - N", where N is the millisecondOfSecond.
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
