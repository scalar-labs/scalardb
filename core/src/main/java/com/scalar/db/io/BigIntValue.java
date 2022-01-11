package com.scalar.db.io;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ComparisonChain;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * A {@code Value} for a signed integer from -2^53 to 2^53. The range is determined by the numbers
 * Double-precision floating-point format can exactly represent.
 *
 * @author Hiroyuki Yamada
 */
@Immutable
public final class BigIntValue implements Value<Long> {
  public static final long MAX_VALUE = 9007199254740992L;
  public static final long MIN_VALUE = -9007199254740992L;

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
    checkArgument(value >= MIN_VALUE && value <= MAX_VALUE, "Out of range value for BigIntValue");
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

  @Override
  @Nonnull
  public Long get() {
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
  public BigIntValue copyWith(String name) {
    return new BigIntValue(name, value);
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
    return (name.equals(other.name) && value == other.value);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("name", name).add("value", value).toString();
  }

  @Override
  public int compareTo(Value<Long> o) {
    return ComparisonChain.start()
        .compare(value, o.get().longValue())
        .compare(name, o.getName())
        .result();
  }
}
