package com.scalar.db.io;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ComparisonChain;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * A {@code Value} (column) for a 32-bit signed integer
 *
 * @author Hiroyuki Yamada
 * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
 */
@Deprecated
@Immutable
public final class IntValue implements Value<Integer> {
  private static final String ANONYMOUS = "";
  private final String name;
  private final int value;

  /**
   * Constructs a {@code IntValue} with the specified name and value
   *
   * @param name name of the {@code Value} (column)
   * @param value value of the {@code Value} (column)
   */
  public IntValue(String name, int value) {
    this.name = checkNotNull(name);
    this.value = value;
  }

  /**
   * Constructs a {@code IntValue} with the specified value. The name of this value (column) is
   * anonymous.
   *
   * @param value value of the {@code Value} (column)
   */
  public IntValue(int value) {
    this(ANONYMOUS, value);
  }

  @Override
  @Nonnull
  public Integer get() {
    return value;
  }

  @Override
  public DataType getDataType() {
    return DataType.INT;
  }

  @Override
  public int getAsInt() {
    return value;
  }

  @Override
  public long getAsLong() {
    return value;
  }

  @Override
  public float getAsFloat() {
    return value;
  }

  @Override
  public double getAsDouble() {
    return value;
  }

  @Override
  @Nonnull
  public String getName() {
    return name;
  }

  @Override
  public IntValue copyWith(String name) {
    return new IntValue(name, value);
  }

  @Override
  public void accept(ValueVisitor v) {
    v.visit(this);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, value);
  }

  /**
   * Indicates whether some other object is "equal to" this object. The other object is considered
   * equal if:
   *
   * <ul>
   *   <li>both super class instances are equal and
   *   <li>it is also an {@code IntValue} and
   *   <li>both instances have the same name and value
   * </ul>
   *
   * @param o an object to be tested for equality
   * @return {@code true} if the other object is "equal to" this object otherwise {@code false}
   */
  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof IntValue)) {
      return false;
    }
    IntValue other = (IntValue) o;
    return (name.equals(other.name) && value == other.value);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("name", name).add("value", value).toString();
  }

  @Override
  public int compareTo(Value<Integer> o) {
    return ComparisonChain.start()
        .compare(value, o.get().intValue())
        .compare(name, o.getName())
        .result();
  }
}
