package com.scalar.db.io;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ComparisonChain;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * A {@code Value} for a 64-bit singed integer
 *
 * @author Hiroyuki Yamada
 */
@Immutable
public final class BigIntValue implements Value<BigIntValue> {
  private static final String ANONYMOUS = "";
  private final String name;
  private final long value;

  /**
   * Constructs a {@code BigIntValue} with the specified name and value
   *
   * @param name name of the {@code Value}
   * @param value content of the {@code Value}
   */
  public BigIntValue(String name, long value) {
    this.name = checkNotNull(name);
    this.value = value;
  }

  /**
   * Constructs a {@code BigIntValue} with the specified value. The name of this value is anonymous.
   *
   * @param value content of the {@code Value}
   */
  public BigIntValue(long value) {
    this(ANONYMOUS, value);
  }

  /**
   * Returns the content of this {@code Value}
   *
   * @return the content of this {@code Value}
   */
  public long get() {
    return value;
  }

  @Override
  public long getAsLong() {
    return get();
  }

  @Override
  public float getAsFloat() {
    return get();
  }

  @Override
  public double getAsDouble() {
    return get();
  }

  @Override
  @Nonnull
  public String getName() {
    return name;
  }

  @Override
  public BigIntValue copyWith(String name) {
    return new BigIntValue(name, value);
  }

  @Override
  public void accept(ValueVisitor v) {
    v.visit(this);
  }

  @Override
  public int hashCode() {
    return Long.hashCode(value);
  }

  /**
   * Indicates whether some other object is "equal to" this object. The other object is considered
   * equal if:
   *
   * <ul>
   *   <li>both super class instances are equal and
   *   <li>it is also an {@code BigIntValue} and
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
    if (!(o instanceof BigIntValue)) {
      return false;
    }
    BigIntValue other = (BigIntValue) o;
    return (this.name.equals(other.name) && this.value == other.value);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("name", name).add("value", value).toString();
  }

  @Override
  public int compareTo(BigIntValue o) {
    return ComparisonChain.start().compare(this.value, o.value).compare(this.name, o.name).result();
  }
}
