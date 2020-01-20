package com.scalar.db.io;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ComparisonChain;
import javax.annotation.Nonnull;

/**
 * A {@code Value} for a boolean data
 *
 * @author Hiroyuki Yamada
 */
public final class BooleanValue implements Value<BooleanValue> {
  private static final String ANONYMOUS = "";
  private final String name;
  private final boolean value;

  /**
   * Constructs a {@code BooleanValue} with the specified name and value
   *
   * @param name name of the {@code Value}
   * @param value content of the {@code Value}
   */
  public BooleanValue(String name, boolean value) {
    this.name = checkNotNull(name);
    this.value = value;
  }

  /**
   * Constructs a {@code BooleanValue} with the specified value. The name of this value is
   * anonymous.
   *
   * @param value content of the {@code Value}
   */
  public BooleanValue(boolean value) {
    this(ANONYMOUS, value);
  }

  /**
   * Returns the content of this {@code Value}
   *
   * @return the content of this {@code Value}
   */
  public boolean get() {
    return value;
  }

  @Override
  @Nonnull
  public String getName() {
    return name;
  }

  @Override
  public BooleanValue copyWith(String name) {
    return new BooleanValue(name, value);
  }

  @Override
  public void accept(ValueVisitor v) {
    v.visit(this);
  }

  @Override
  public int hashCode() {
    return Boolean.hashCode(value);
  }

  /**
   * Indicates whether some other object is "equal to" this object. The other object is considered
   * equal if:
   *
   * <ul>
   *   <li>both super class instances are equal and
   *   <li>it is also an {@code BooleanValue} and
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
    if (!(o instanceof BooleanValue)) {
      return false;
    }
    BooleanValue other = (BooleanValue) o;
    return (this.name.equals(other.name) && this.value == other.value);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("name", name).add("value", value).toString();
  }

  @Override
  public int compareTo(BooleanValue o) {
    return ComparisonChain.start()
        .compareFalseFirst(this.value, o.value)
        .compare(this.name, o.name)
        .result();
  }
}
