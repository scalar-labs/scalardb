package com.scalar.db.io;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Comparators;
import com.google.common.collect.ComparisonChain;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/** A {@code Column} for a BOOLEAN type. */
@Immutable
public class BooleanColumn implements Column<Boolean> {

  private final String name;
  private final boolean value;
  private final boolean hasNullValue;

  private BooleanColumn(String name, boolean value) {
    this(name, value, false);
  }

  private BooleanColumn(String name) {
    this(name, false, true);
  }

  private BooleanColumn(String name, boolean value, boolean hasNullValue) {
    this.name = Objects.requireNonNull(name);
    this.value = value;
    this.hasNullValue = hasNullValue;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Optional<Boolean> getValue() {
    if (hasNullValue) {
      return Optional.empty();
    }
    return Optional.of(value);
  }

  @Override
  public boolean getBooleanValue() {
    return value;
  }

  @Override
  public BooleanColumn copyWith(String name) {
    return new BooleanColumn(name, value, hasNullValue);
  }

  @Override
  public DataType getDataType() {
    return DataType.BOOLEAN;
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

  @SuppressWarnings("UnstableApiUsage")
  @Override
  public int compareTo(Column<Boolean> o) {
    return ComparisonChain.start()
        .compare(getName(), o.getName())
        .compare(getValue(), o.getValue(), Comparators.emptiesLast(Comparator.naturalOrder()))
        .result();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BooleanColumn)) {
      return false;
    }
    BooleanColumn column = (BooleanColumn) o;
    return value == column.value
        && hasNullValue == column.hasNullValue
        && Objects.equals(name, column.name);
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

  public static BooleanColumn of(String columnName, boolean value) {
    return new BooleanColumn(columnName, value);
  }

  public static BooleanColumn ofNull(String columnName) {
    return new BooleanColumn(columnName);
  }
}
