package com.scalar.db.io;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ComparisonChain;
import java.time.LocalDate;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;

/** A {@code Column} for a DATE type. */
public class DateColumn implements Column<LocalDate> {

  private final String name;
  @Nullable private final LocalDate value;

  private DateColumn(String name, @Nullable LocalDate value) {
    this.name = Objects.requireNonNull(name);
    this.value = value;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Optional<LocalDate> getValue() {
    return Optional.ofNullable(value);
  }

  @Nullable
  @Override
  public LocalDate getDateValue() {
    return value;
  }

  @Override
  public DateColumn copyWith(String name) {
    return new DateColumn(name, value);
  }

  @Override
  public DataType getDataType() {
    return DataType.DATE;
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
  public int compareTo(Column<LocalDate> o) {
    return ComparisonChain.start()
        .compare(getName(), o.getName())
        .compareTrueFirst(hasNullValue(), o.hasNullValue())
        .compare(value, o.getDateValue(), Comparator.nullsFirst(Comparator.naturalOrder()))
        .result();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DateColumn)) {
      return false;
    }
    DateColumn that = (DateColumn) o;
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
   * Returns a Date column instance with the specified column name and value.
   *
   * @param columnName a column name
   * @param value a column value
   * @return a Date column instance with the specified column name and value
   */
  public static DateColumn of(String columnName, LocalDate value) {
    return new DateColumn(columnName, value);
  }

  /**
   * Returns a Date column instance with the specified column name and a null value.
   *
   * @param columnName a column name
   * @return a Date column instance with the specified column name and a null value
   */
  public static DateColumn ofNull(String columnName) {
    return new DateColumn(columnName, null);
  }
}
