package com.scalar.db.io;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ComparisonChain;
import com.scalar.db.common.error.CoreError;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/**
 * A {@code Column} for a TIMESTAMPTZ type. It represents a date-time in the UTC time zone in the
 * ISO-8601 calendar system, such as 2017-06-19T16:15:30Z, and can be expressed with millisecond
 * precision.
 */
@Immutable
public class TimestampTZColumn implements Column<Instant> {
  /** The minimum TIMESTAMPTZ value is 1000-01-01T00:00:00.000Z */
  public static final Instant MIN_VALUE =
      LocalDateTime.of(1000, 1, 1, 0, 0).toInstant(ZoneOffset.UTC);
  /** The maximum TIMESTAMPTZ value is 9999-12-31T23:59:59.999Z */
  public static final Instant MAX_VALUE =
      LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999_000_000).toInstant(ZoneOffset.UTC);
  /** The precision of a TIMESTAMPTZ is up to 1 millisecond. */
  public static final int FRACTIONAL_SECONDS_PRECISION_IN_NANOSECONDS = 1_000_000;

  private final String name;
  @Nullable private final Instant value;

  @SuppressWarnings("JavaInstantGetSecondsGetNano")
  private TimestampTZColumn(String name, @Nullable Instant value) {
    if (value != null && (value.isBefore(MIN_VALUE) || value.isAfter(MAX_VALUE))) {
      throw new IllegalArgumentException(
          CoreError.OUT_OF_RANGE_COLUMN_VALUE_FOR_TIMESTAMPTZ.buildMessage(value));
    }
    if (value != null && value.getNano() % FRACTIONAL_SECONDS_PRECISION_IN_NANOSECONDS != 0) {
      throw new IllegalArgumentException(
          CoreError.SUBMILLISECOND_PRECISION_NOT_SUPPORTED_FOR_TIMESTAMPTZ.buildMessage(value));
    }

    this.name = Objects.requireNonNull(name);
    this.value = value;
  }

  @Override
  protected final void finalize() throws Throwable {
    // Override the finalize method to prevent a finalizer attack in case an
    // IllegalArgumentException is thrown in the constructor
    // cf. Spotbug CT_CONSTRUCTOR_THROW alert
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Optional<Instant> getValue() {
    return Optional.ofNullable(value);
  }

  @Nullable
  @Override
  public Instant getTimestampTZValue() {
    return value;
  }

  @Override
  public TimestampTZColumn copyWith(String name) {
    return new TimestampTZColumn(name, value);
  }

  @Override
  public DataType getDataType() {
    return DataType.TIMESTAMPTZ;
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
  public int compareTo(Column<Instant> o) {
    return ComparisonChain.start()
        .compare(getName(), o.getName())
        .compareTrueFirst(hasNullValue(), o.hasNullValue())
        .compare(value, o.getTimestampTZValue(), Comparator.nullsFirst(Comparator.naturalOrder()))
        .result();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TimestampTZColumn)) {
      return false;
    }
    TimestampTZColumn that = (TimestampTZColumn) o;
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
   * Returns a TimestampTZ column instance with the specified column name and value.
   *
   * @param columnName a column name
   * @param value a column value
   * @return a TimestampTZ column instance with the specified column name and value
   */
  public static TimestampTZColumn of(String columnName, Instant value) {
    return new TimestampTZColumn(columnName, value);
  }

  /**
   * Returns a TimestampTZ column instance with the specified column name and a null value.
   *
   * @param columnName a column name
   * @return a TimestampTZ column instance with the specified column name and a null value
   */
  public static TimestampTZColumn ofNull(String columnName) {
    return new TimestampTZColumn(columnName, null);
  }
}
