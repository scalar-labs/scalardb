package com.scalar.db.io;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ComparisonChain;
import com.scalar.db.common.error.CoreError;
import java.time.LocalTime;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * A {@code Column} for a TIME type. It represents a time without a time-zone in the ISO-8601
 * calendar system, such as 16:15:30, and can be expressed with microsecond precision.
 */
public class TimeColumn implements Column<LocalTime> {

  /** The minimum TIME value is 00:00:00.000000 */
  public static final LocalTime MIN_VALUE = LocalTime.of(0, 0, 0, 0);
  /** The maximum TIME value is 23:59:59.999999 */
  public static final LocalTime MAX_VALUE = LocalTime.of(23, 59, 59, 999_999_000);
  /** The precision of a TIME is up to 1 microsecond. */
  public static final int FRACTIONAL_SECONDS_PRECISION_IN_NANOSECONDS = 1_000;

  private final String name;
  @Nullable private final LocalTime value;

  @SuppressWarnings("JavaLocalTimeGetNano")
  private TimeColumn(String name, @Nullable LocalTime value) {
    this.name = Objects.requireNonNull(name);
    this.value = value;

    if (value != null && value.getNano() % FRACTIONAL_SECONDS_PRECISION_IN_NANOSECONDS != 0) {
      throw new IllegalArgumentException(
          CoreError.SUBMICROSECOND_PRECISION_NOT_SUPPORTED_FOR_TIME.buildMessage(value));
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Optional<LocalTime> getValue() {
    return Optional.ofNullable(value);
  }

  @Nullable
  @Override
  public LocalTime getTimeValue() {
    return value;
  }

  @Override
  public TimeColumn copyWith(String name) {
    return new TimeColumn(name, value);
  }

  @Override
  public DataType getDataType() {
    return DataType.TIME;
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
  public int compareTo(Column<LocalTime> o) {
    return ComparisonChain.start()
        .compare(getName(), o.getName())
        .compareTrueFirst(hasNullValue(), o.hasNullValue())
        .compare(value, o.getTimeValue(), Comparator.nullsFirst(Comparator.naturalOrder()))
        .result();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TimeColumn)) {
      return false;
    }
    TimeColumn that = (TimeColumn) o;
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
   * Returns a Time column instance with the specified column name and value.
   *
   * @param columnName a column name
   * @param value a column value
   * @return a Time column instance with the specified column name and value
   */
  public static TimeColumn of(String columnName, LocalTime value) {
    return new TimeColumn(columnName, value);
  }

  /**
   * Returns a Time column instance with the specified column name and a null value.
   *
   * @param columnName a column name
   * @return a Time column instance with the specified column name and a null value
   */
  public static TimeColumn ofNull(String columnName) {
    return new TimeColumn(columnName, null);
  }
}
