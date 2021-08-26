package com.scalar.db.io;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/**
 * An abstraction for keys
 *
 * @author Hiroyuki Yamada
 */
@Immutable
public final class Key implements Comparable<Key>, Iterable<Value<?>> {
  private final List<Value<?>> values;

  /**
   * Constructs a {@code Key} with the specified {@link Value}s
   *
   * @param values one or more {@link Value}s which this key is composed of
   */
  public Key(Value<?>... values) {
    checkNotNull(values);
    this.values = new ArrayList<>(values.length);
    this.values.addAll(Arrays.asList(values));
  }

  /**
   * Constructs a {@code Key} with the specified list of {@link Value}s
   *
   * @param values a list of {@link Value}s which this key is composed of
   */
  public Key(List<Value<?>> values) {
    checkNotNull(values);
    this.values = new ArrayList<>(values.size());
    this.values.addAll(values);
  }

  /**
   * Constructs a {@code Key} with a single {@link Value} as a boolean type
   *
   * @param name name of the {@code Value}
   * @param value content of the {@code Value}
   */
  public Key(String name, boolean value) {
    values = Collections.singletonList(new BooleanValue(name, value));
  }

  /**
   * Constructs a {@code Key} with a single {@link Value} as an integer type
   *
   * @param name name of the {@code Value}
   * @param value content of the {@code Value}
   */
  public Key(String name, int value) {
    values = Collections.singletonList(new IntValue(name, value));
  }

  /**
   * Constructs a {@code Key} with a single {@link Value} as a long type
   *
   * @param name name of the {@code Value}
   * @param value content of the {@code Value}
   */
  public Key(String name, long value) {
    values = Collections.singletonList(new BigIntValue(name, value));
  }

  /**
   * Constructs a {@code Key} with a single {@link Value} as a float type
   *
   * @param name name of the {@code Value}
   * @param value content of the {@code Value}
   */
  public Key(String name, float value) {
    values = Collections.singletonList(new FloatValue(name, value));
  }

  /**
   * Constructs a {@code Key} with a single {@link Value} as a double type
   *
   * @param name name of the {@code Value}
   * @param value content of the {@code Value}
   */
  public Key(String name, double value) {
    values = Collections.singletonList(new DoubleValue(name, value));
  }

  /**
   * Constructs a {@code Key} with a single {@link Value} as a string type
   *
   * @param name name of the {@code Value}
   * @param value content of the {@code Value}
   */
  public Key(String name, @Nullable String value) {
    values = Collections.singletonList(new TextValue(name, value));
  }

  /**
   * Constructs a {@code Key} with a single {@link Value} as a byte array type
   *
   * @param name name of the {@code Value}
   * @param value content of the {@code Value}
   */
  public Key(String name, @Nullable byte[] value) {
    values = Collections.singletonList(new BlobValue(name, value));
  }

  /**
   * Returns the list of {@code Value} which this key is composed of
   *
   * @return list of {@code Value} which this key is composed of
   */
  @Nonnull
  public List<Value<?>> get() {
    return Collections.unmodifiableList(values);
  }

  /**
   * Returns the size of the list of {@code Value} which this key is composed of
   *
   * @return the size of list of {@code Value} which this key is composed of
   */
  public int size() {
    return values.size();
  }

  @Override
  public int hashCode() {
    return values.hashCode();
  }

  /**
   * Indicates whether some other object is "equal to" this object. The other object is considered
   * equal if:
   *
   * <ul>
   *   <li>both super class instances are equal and
   *   <li>it is also an {@code Key} and
   *   <li>both instances have the same list of {@code Value}s
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
    if (!(o instanceof Key)) {
      return false;
    }
    Key that = (Key) o;
    return values.equals(that.values);
  }

  @Override
  public String toString() {
    MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
    values.forEach(helper::addValue);
    return helper.toString();
  }

  @Nonnull
  @Override
  public Iterator<Value<?>> iterator() {
    return values.iterator();
  }

  @Override
  public int compareTo(Key o) {
    return ComparisonChain.start()
        .compare(values, o.values, Ordering.<Value<?>>natural().lexicographical())
        .result();
  }

  /**
   * Returns a new builder instance
   *
   * @return a new builder instance
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /** A builder class that builds a {@code Key} instance */
  public static final class Builder {
    private final List<Value<?>> values = new ArrayList<>();

    private Builder() {}

    public Builder addBoolean(String name, boolean value) {
      values.add(new BooleanValue(name, value));
      return this;
    }

    public Builder addInt(String name, int value) {
      values.add(new IntValue(name, value));
      return this;
    }

    public Builder addBigInt(String name, long value) {
      values.add(new BigIntValue(name, value));
      return this;
    }

    public Builder addFloat(String name, float value) {
      values.add(new FloatValue(name, value));
      return this;
    }

    public Builder addDouble(String name, double value) {
      values.add(new DoubleValue(name, value));
      return this;
    }

    public Builder addText(String name, @Nullable String value) {
      values.add(new TextValue(name, value));
      return this;
    }

    public Builder addBlob(String name, @Nullable byte[] value) {
      values.add(new BlobValue(name, value));
      return this;
    }

    public Builder add(Value<?> value) {
      values.add(value);
      return this;
    }

    public Builder addAll(Collection<? extends Value<?>> values) {
      this.values.addAll(values);
      return this;
    }

    public Key build() {
      return new Key(values);
    }
  }
}
