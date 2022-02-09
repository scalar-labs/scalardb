package com.scalar.db.io;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
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
  public Key(String name, String value) {
    values = Collections.singletonList(new TextValue(name, value));
  }

  /**
   * Constructs a {@code Key} with a single {@link Value} as a byte array type
   *
   * @param name name of the {@code Value}
   * @param value content of the {@code Value}
   */
  public Key(String name, byte[] value) {
    values = Collections.singletonList(new BlobValue(name, value));
  }

  /**
   * Constructs a {@code Key} with a single {@link Value} as a byte buffer type
   *
   * @param name name of the {@code Value}
   * @param value content of the {@code Value}
   */
  public Key(String name, ByteBuffer value) {
    values = Collections.singletonList(new BlobValue(name, value));
  }

  /**
   * Constructs a {@code Key} with multiple {@link Value}s
   *
   * @param n1 name of the 1st {@code Value}
   * @param v1 content of the 1st {@code Value}
   * @param n2 name of the 2nd {@code Value}
   * @param v2 content of the 2nd {@code Value}
   */
  public Key(String n1, Object v1, String n2, Object v2) {
    values = Arrays.asList(toValue(n1, v1), toValue(n2, v2));
  }

  /**
   * Constructs a {@code Key} with multiple {@link Value}s
   *
   * @param n1 name of the 1st {@code Value}
   * @param v1 content of the 1st {@code Value}
   * @param n2 name of the 2nd {@code Value}
   * @param v2 content of the 2nd {@code Value}
   * @param n3 name of the 3rd {@code Value}
   * @param v3 content of the 3rd {@code Value}
   */
  public Key(String n1, Object v1, String n2, Object v2, String n3, Object v3) {
    values = Arrays.asList(toValue(n1, v1), toValue(n2, v2), toValue(n3, v3));
  }

  /**
   * Constructs a {@code Key} with multiple {@link Value}s
   *
   * @param n1 name of the 1st {@code Value}
   * @param v1 content of the 1st {@code Value}
   * @param n2 name of the 2nd {@code Value}
   * @param v2 content of the 2nd {@code Value}
   * @param n3 name of the 3rd {@code Value}
   * @param v3 content of the 3rd {@code Value}
   * @param n4 name of the 4th {@code Value}
   * @param v4 content of the 4th {@code Value}
   */
  public Key(
      String n1, Object v1, String n2, Object v2, String n3, Object v3, String n4, Object v4) {
    values = Arrays.asList(toValue(n1, v1), toValue(n2, v2), toValue(n3, v3), toValue(n4, v4));
  }

  /**
   * Constructs a {@code Key} with multiple {@link Value}s
   *
   * @param n1 name of the 1st {@code Value}
   * @param v1 content of the 1st {@code Value}
   * @param n2 name of the 2nd {@code Value}
   * @param v2 content of the 2nd {@code Value}
   * @param n3 name of the 3rd {@code Value}
   * @param v3 content of the 3rd {@code Value}
   * @param n4 name of the 4th {@code Value}
   * @param v4 content of the 4th {@code Value}
   * @param n5 name of the 5th {@code Value}
   * @param v5 content of the 5th {@code Value}
   */
  public Key(
      String n1,
      Object v1,
      String n2,
      Object v2,
      String n3,
      Object v3,
      String n4,
      Object v4,
      String n5,
      Object v5) {
    values =
        Arrays.asList(
            toValue(n1, v1), toValue(n2, v2), toValue(n3, v3), toValue(n4, v4), toValue(n5, v5));
  }

  private Value<?> toValue(String name, Object value) {
    if (value instanceof Boolean) {
      return new BooleanValue(name, (Boolean) value);
    } else if (value instanceof Integer) {
      return new IntValue(name, (Integer) value);
    } else if (value instanceof Long) {
      return new BigIntValue(name, (Long) value);
    } else if (value instanceof Float) {
      return new FloatValue(name, (Float) value);
    } else if (value instanceof Double) {
      return new DoubleValue(name, (Double) value);
    } else if (value instanceof String) {
      return new TextValue(name, (String) value);
    } else if (value instanceof byte[]) {
      return new BlobValue(name, (byte[]) value);
    } else if (value instanceof ByteBuffer) {
      return new BlobValue(name, (ByteBuffer) value);
    } else {
      throw new IllegalArgumentException(
          "Unsupported type, name: " + name + ", type: " + value.getClass().getName());
    }
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
    return Objects.hash(values);
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

    /**
     * Adds Boolean value.
     *
     * @param name name of the {@code Value}
     * @param value content of the {@code Value}
     * @return a builder object
     */
    public Builder addBoolean(String name, boolean value) {
      values.add(new BooleanValue(name, value));
      return this;
    }

    /**
     * Adds Int value.
     *
     * @param name name of the {@code Value}
     * @param value content of the {@code Value}
     * @return a builder object
     */
    public Builder addInt(String name, int value) {
      values.add(new IntValue(name, value));
      return this;
    }

    /**
     * Adds BigInt value.
     *
     * @param name name of the {@code Value}
     * @param value content of the {@code Value}
     * @return a builder object
     */
    public Builder addBigInt(String name, long value) {
      values.add(new BigIntValue(name, value));
      return this;
    }

    /**
     * Adds Float value.
     *
     * @param name name of the {@code Value}
     * @param value content of the {@code Value}
     * @return a builder object
     */
    public Builder addFloat(String name, float value) {
      values.add(new FloatValue(name, value));
      return this;
    }

    /**
     * Adds Double value.
     *
     * @param name name of the {@code Value}
     * @param value content of the {@code Value}
     * @return a builder object
     */
    public Builder addDouble(String name, double value) {
      values.add(new DoubleValue(name, value));
      return this;
    }

    /**
     * Adds Text value.
     *
     * @param name name of the {@code Value}
     * @param value content of the {@code Value}
     * @return a builder object
     */
    public Builder addText(String name, String value) {
      values.add(new TextValue(name, value));
      return this;
    }

    /**
     * Adds Blob value.
     *
     * @param name name of the {@code Value}
     * @param value content of the {@code Value}
     * @return a builder object
     */
    public Builder addBlob(String name, byte[] value) {
      values.add(new BlobValue(name, value));
      return this;
    }

    /**
     * Adds Blob value.
     *
     * @param name name of the {@code Value}
     * @param value content of the {@code Value}
     * @return a builder object
     */
    public Builder addBlob(String name, ByteBuffer value) {
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
