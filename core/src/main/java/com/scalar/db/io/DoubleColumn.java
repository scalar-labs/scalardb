package com.scalar.db.io;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ComparisonChain;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/** A {@code Column} for an DOUBLE type. */
@Immutable
public class DoubleColumn implements Column<Double> {

  private final String name;
  private final double value;
  private final boolean hasNullValue;

  private DoubleColumn(String name, double value) {
    this(name, value, false);
  }

  private DoubleColumn(String name) {
    this(name, 0.0, true);
  }

  private DoubleColumn(String name, double value, boolean hasNullValue) {
    this.name = Objects.requireNonNull(name);
    this.value = value;
    this.hasNullValue = hasNullValue;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Optional<Double> getValue() {
    if (hasNullValue) {
      return Optional.empty();
    }
    return Optional.of(value);
  }

  @Override
  public double getDoubleValue() {
    return value;
  }

  @Override
  public DoubleColumn copyWith(String name) {
    return new DoubleColumn(name, value, hasNullValue);
  }

  @Override
  public DataType getDataType() {
    return DataType.DOUBLE;
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
  public int compareTo(Column<Double> o) {
    return ComparisonChain.start()
        .compare(getName(), o.getName())
        .compareTrueFirst(hasNullValue(), o.hasNullValue())
        .compare(getDoubleValue(), o.getDoubleValue())
        .result();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DoubleColumn)) {
      return false;
    }
    DoubleColumn that = (DoubleColumn) o;
    return Double.compare(that.value, value) == 0
        && hasNullValue == that.hasNullValue
        && Objects.equals(name, that.name);
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
   * Returns a Double column instance with the specified column name and value.
   *
   * @param columnName a column name
   * @param value a column value
   * @return a Double column instance with the specified column name and value
   */
  public static DoubleColumn of(String columnName, double value) {
    return new DoubleColumn(columnName, value);
  }

  /**
   * Returns a Double column instance with the specified column name and a null value.
   *
   * @param columnName a column name
   * @return a Double column instance with the specified column name and a null value
   */
  public static DoubleColumn ofNull(String columnName) {
    return new DoubleColumn(columnName);
  }
}
