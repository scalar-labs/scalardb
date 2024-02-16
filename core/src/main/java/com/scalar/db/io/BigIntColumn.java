package com.scalar.db.io;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ComparisonChain;
import com.scalar.db.common.error.CoreError;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/**
 * A {@code Column} for a BIGINT type. It is for a signed integer from -2^53 to 2^53. The range is
 * determined by the numbers Double-precision floating-point format can exactly represent.
 */
@Immutable
public class BigIntColumn implements Column<Long> {

  public static final long MAX_VALUE = 9007199254740992L;
  public static final long MIN_VALUE = -9007199254740992L;

  private final String name;
  private final long value;
  private final boolean hasNullValue;

  private BigIntColumn(String name, long value) {
    this(name, value, false);
  }

  private BigIntColumn(String name) {
    this(name, 0L, true);
  }

  private BigIntColumn(String name, long value, boolean hasNullValue) {
    this.name = Objects.requireNonNull(name);
    if (value < MIN_VALUE || value > MAX_VALUE) {
      throw new IllegalArgumentException(
          CoreError.OUT_OF_RANGE_COLUMN_VALUE_FOR_BIGINT.buildMessage(value));
    }
    this.value = value;
    this.hasNullValue = hasNullValue;
  }

  // For the SpotBugs warning CT_CONSTRUCTOR_THROW
  @Override
  protected final void finalize() {}

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Optional<Long> getValue() {
    if (hasNullValue) {
      return Optional.empty();
    }
    return Optional.of(value);
  }

  @Override
  public long getBigIntValue() {
    return value;
  }

  @Override
  public BigIntColumn copyWith(String name) {
    return new BigIntColumn(name, value, hasNullValue);
  }

  @Override
  public DataType getDataType() {
    return DataType.BIGINT;
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
  public int compareTo(Column<Long> o) {
    return ComparisonChain.start()
        .compare(getName(), o.getName())
        .compareTrueFirst(hasNullValue(), o.hasNullValue())
        .compare(getBigIntValue(), o.getBigIntValue())
        .result();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BigIntColumn)) {
      return false;
    }
    BigIntColumn that = (BigIntColumn) o;
    return value == that.value
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
   * Returns a BigInt column instance with the specified column name and value.
   *
   * @param columnName a column name
   * @param value a column value
   * @return a BigInt column instance with the specified column name and value
   */
  public static BigIntColumn of(String columnName, long value) {
    return new BigIntColumn(columnName, value);
  }

  /**
   * Returns a BigInt column instance with the specified column name and a null value.
   *
   * @param columnName a column name
   * @return a BigInt column instance with the specified column name and a null value
   */
  public static BigIntColumn ofNull(String columnName) {
    return new BigIntColumn(columnName);
  }
}
