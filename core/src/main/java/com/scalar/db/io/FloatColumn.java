package com.scalar.db.io;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Comparators;
import com.google.common.collect.ComparisonChain;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/** A {@code Column} for an FLOAT type. */
@Immutable
public class FloatColumn implements Column<Float> {

  private final String name;
  private final float value;
  private final boolean hasNullValue;

  private FloatColumn(String name, float value) {
    this(name, value, false);
  }

  private FloatColumn(String name) {
    this(name, 0.0F, true);
  }

  private FloatColumn(String name, float value, boolean hasNullValue) {
    this.name = Objects.requireNonNull(name);
    this.value = value;
    this.hasNullValue = hasNullValue;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Optional<Float> getValue() {
    if (hasNullValue) {
      return Optional.empty();
    }
    return Optional.of(value);
  }

  @Override
  public float getFloatValue() {
    return value;
  }

  @Override
  public FloatColumn copyWith(String name) {
    return new FloatColumn(name, value, hasNullValue);
  }

  @Override
  public DataType getDataType() {
    return DataType.FLOAT;
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
  public int compareTo(Column<Float> o) {
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
    if (!(o instanceof FloatColumn)) {
      return false;
    }
    FloatColumn that = (FloatColumn) o;
    return Float.compare(that.value, value) == 0
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

  public static FloatColumn of(String columnName, float value) {
    return new FloatColumn(columnName, value);
  }

  public static FloatColumn ofNull(String columnName) {
    return new FloatColumn(columnName);
  }
}
