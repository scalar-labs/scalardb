package com.scalar.db.io;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ComparisonChain;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * A {@code Value} for a single precision floating point number
 *
 * @author Hiroyuki Yamada
 */
@Immutable
public final class FloatValue implements Value<FloatValue> {
  private static final String ANONYMOUS = "";
  private final String name;
  private final float value;

  /**
   * Constructs a {@code FloatValue} with the specified name and value
   *
   * @param name name of the {@code Value}
   * @param value content of the {@code Value}
   */
  public FloatValue(String name, float value) {
    this.name = checkNotNull(name);
    this.value = value;
  }

  /**
   * Constructs a {@code FloatValue} with the specified value. The name of this value is anonymous.
   *
   * @param value content of the {@code Value}
   */
  public FloatValue(float value) {
    this(ANONYMOUS, value);
  }

  /**
   * Returns the content of this {@code Value}
   *
   * @return the content of this {@code Value}
   */
  public float get() {
    return value;
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
  public FloatValue copyWith(String name) {
    return new FloatValue(name, value);
  }

  @Override
  public void accept(ValueVisitor v) {
    v.visit(this);
  }

  @Override
  public int hashCode() {
    return Float.hashCode(value);
  }

  /**
   * Indicates whether some other object is "equal to" this object. The other object is considered
   * equal if:
   *
   * <ul>
   *   <li>both super class instances are equal and
   *   <li>it is also an {@code FloatValue} and
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
    if (!(o instanceof FloatValue)) {
      return false;
    }
    FloatValue other = (FloatValue) o;
    return (this.name.equals(other.name) && this.value == other.value);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("name", name).add("value", value).toString();
  }

  @Override
  public int compareTo(FloatValue o) {
    return ComparisonChain.start().compare(this.value, o.value).compare(this.name, o.name).result();
  }
}
