package com.scalar.db.io;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ComparisonChain;
import com.scalar.db.common.error.CoreError;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * A {@code Column} for a TIMESTAMP type. It represents a date-time without a time-zone in the
 * ISO-8601 calendar system, such as 2017-06-19T16:15:30, and can be expressed with millisecond
 * precision.
 */
public class TimestampColumn implements Column<LocalDateTime> {
  /** The minimum TIMESTAMP value is 1000-01-01T00:00:00.000 */
  public static final LocalDateTime MIN_VALUE = LocalDateTime.of(1000, 1, 1, 0, 0);
  /** The maximum TIMESTAMP value is 9999-12-31T23:59:59.999 */
  public static final LocalDateTime MAX_VALUE =
      LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999_000_000);
  /** The precision of a TIMESTAMP is up to 1 millisecond. */
  public static final int FRACTIONAL_SECONDS_PRECISION_IN_NANOSECONDS = 1_000_000;

  private final String name;
  @Nullable private final LocalDateTime value;

  @SuppressWarnings("JavaLocalTimeGetNano")
  private TimestampColumn(String name, @Nullable LocalDateTime value) {
    this.name = Objects.requireNonNull(name);
    this.value = value;

    if (value == null) {
      return;
    }
    if (value.isBefore(MIN_VALUE) || value.isAfter(MAX_VALUE)) {
      throw new IllegalArgumentException(
          CoreError.OUT_OF_RANGE_COLUMN_VALUE_FOR_TIMESTAMP.buildMessage(value));
    }
    if (value.getNano() % FRACTIONAL_SECONDS_PRECISION_IN_NANOSECONDS != 0) {
      throw new IllegalArgumentException(
          CoreError.SUBMILLISECOND_PRECISION_NOT_SUPPORTED_FOR_TIMESTAMP.buildMessage(value));
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Optional<LocalDateTime> getValue() {
    return Optional.ofNullable(value);
  }

  @Nullable
  @Override
  public LocalDateTime getTimestampValue() {
    return value;
  }

  @Override
  public TimestampColumn copyWith(String name) {
    return new TimestampColumn(name, value);
  }

  @Override
  public DataType getDataType() {
    return DataType.TIMESTAMP;
  }

  @Override
  public boolean hasNullValue() {
    return value == null;
  }

  @Nullable
  @Override
  public Object getValueAsObject() {
    return value;
  }

  @Override
  public int compareTo(Column<LocalDateTime> o) {
    return ComparisonChain.start()
        .compare(getName(), o.getName())
        .compareTrueFirst(hasNullValue(), o.hasNullValue())
        .compare(value, o.getTimestampValue(), Comparator.nullsFirst(Comparator.naturalOrder()))
        .result();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TimestampColumn)) {
      return false;
    }
    TimestampColumn that = (TimestampColumn) o;
    return Objects.equals(name, that.name) && Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, value);
  }

  @Override
  public void accept(ColumnVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("name", name).add("value", value).toString();
  }
  /**
   * Returns a Timestamp column instance with the specified column name and value.
   *
   * @param columnName a column name
   * @param value a column value
   * @return a Timestamp column instance with the specified column name and value
   */
  public static TimestampColumn of(String columnName, LocalDateTime value) {
    return new TimestampColumn(columnName, value);
  }

  /**
   * Returns a Timestamp column instance with the specified column name and a null value.
   *
   * @param columnName a column name
   * @return a Timestamp column instance with the specified column name and a null value
   */
  public static TimestampColumn ofNull(String columnName) {
    return new TimestampColumn(columnName, null);
  }
}
