package com.scalar.db.io;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ComparisonChain;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/** A {@code Column} for an INT type. */
@Immutable
public class IntColumn implements Column<Integer> {

  private final String name;
  private final int value;
  private final boolean hasNullValue;

  private IntColumn(String name, int value) {
    this(name, value, false);
  }

  private IntColumn(String name) {
    this(name, 0, true);
  }

  private IntColumn(String name, int value, boolean hasNullValue) {
    this.name = Objects.requireNonNull(name);
    this.value = value;
    this.hasNullValue = hasNullValue;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Optional<Integer> getValue() {
    if (hasNullValue) {
      return Optional.empty();
    }
    return Optional.of(value);
  }

  @Override
  public int getIntValue() {
    return value;
  }

  @Override
  public IntColumn copyWith(String name) {
    return new IntColumn(name, value, hasNullValue);
  }

  @Override
  public DataType getDataType() {
    return DataType.INT;
  }

  @Override
  public boolean hasNullValue() {
    return hasNullValue;
  }

  @Override
  @Nullable
  public Object getValueAsObject() {
    if (hasNullValue) {
      return null;
    }
    return value;
  }

  @Override
  public int compareTo(Column<Integer> o) {
    return ComparisonChain.start()
        .compare(getName(), o.getName())
        .compareTrueFirst(hasNullValue(), o.hasNullValue())
        .compare(getIntValue(), o.getIntValue())
        .result();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof IntColumn)) {
      return false;
    }
    IntColumn intColumn = (IntColumn) o;
    return value == intColumn.value
        && hasNullValue == intColumn.hasNullValue
        && Objects.equals(name, intColumn.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, value, hasNullValue);
  }

  @Override
  public void accept(ColumnVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("value", value)
        .add("hasNullValue", hasNullValue)
        .toString();
  }

  /**
   * Returns an Int column instance with the specified column name and value.
   *
   * @param columnName a column name
   * @param value a column value
   * @return an Int column instance with the specified column name and value
   */
  public static IntColumn of(String columnName, int value) {
    return new IntColumn(columnName, value);
  }

  /**
   * Returns an Int column instance with the specified column name and a null value.
   *
   * @param columnName a column name
   * @return an Int column instance with the specified column name and a null value
   */
  public static IntColumn ofNull(String columnName) {
    return new IntColumn(columnName);
  }
}
