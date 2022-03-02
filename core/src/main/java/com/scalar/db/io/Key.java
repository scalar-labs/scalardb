package com.scalar.db.io;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
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
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  public Key(Value<?>... values) {
    checkNotNull(values);
    this.values = new ArrayList<>(values.length);
    this.values.addAll(Arrays.asList(values));
  }

  /**
   * Constructs a {@code Key} with the specified list of {@link Value}s
   *
   * @param values a list of {@link Value}s which this key is composed of
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  public Key(List<Value<?>> values) {
    checkNotNull(values);
    this.values = new ArrayList<>(values.size());
    this.values.addAll(values);
  }

  /**
   * Constructs a {@code Key} with a single column with a BOOLEAN type
   *
   * @param columnName a column name
   * @param booleanValue a BOOLEAN value of the column
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link
   *     #ofBoolean(String, boolean)} instead
   */
  @Deprecated
  public Key(String columnName, boolean booleanValue) {
    values = Collections.singletonList(new BooleanValue(columnName, booleanValue));
  }

  /**
   * Constructs a {@code Key} with a single column with an INT type
   *
   * @param columnName a column name
   * @param intValue an INT value of the column
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link #ofInt(String,
   *     int)} instead
   */
  @Deprecated
  public Key(String columnName, int intValue) {
    values = Collections.singletonList(new IntValue(columnName, intValue));
  }

  /**
   * Constructs a {@code Key} with a single column with a BIGINT type
   *
   * @param columnName a column name
   * @param bigIntValue a BIGINT value of the column
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link #ofBigInt(String,
   *     long)} instead
   */
  @Deprecated
  public Key(String columnName, long bigIntValue) {
    values = Collections.singletonList(new BigIntValue(columnName, bigIntValue));
  }

  /**
   * Constructs a {@code Key} with a single column with a FLOAT type
   *
   * @param columnName a column name
   * @param floatValue a FLOAT value of the column
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link #ofFloat(String,
   *     float)} instead
   */
  @Deprecated
  public Key(String columnName, float floatValue) {
    values = Collections.singletonList(new FloatValue(columnName, floatValue));
  }

  /**
   * Constructs a {@code Key} with a single column with a DOUBLE type
   *
   * @param columnName a column name
   * @param doubleValue a DOUBLE value of the column
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link #ofDouble(String,
   *     double)} instead
   */
  @Deprecated
  public Key(String columnName, double doubleValue) {
    values = Collections.singletonList(new DoubleValue(columnName, doubleValue));
  }

  /**
   * Constructs a {@code Key} with a single column with a TEXT type
   *
   * @param columnName a column name
   * @param textValue a TEXT value of the column
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link #ofText(String,
   *     String)} instead
   */
  @Deprecated
  public Key(String columnName, String textValue) {
    values = Collections.singletonList(new TextValue(columnName, textValue));
  }

  /**
   * Constructs a {@code Key} with a single column with a BLOB type from a byte array
   *
   * @param columnName a column name
   * @param blobValue a BLOB value of the column
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link #ofBlob(String,
   *     byte[])} instead
   */
  @Deprecated
  public Key(String columnName, byte[] blobValue) {
    values = Collections.singletonList(new BlobValue(columnName, blobValue));
  }

  /**
   * Constructs a {@code Key} with a single column with a BLOB type from a ByteBuffer
   *
   * @param columnName a column name
   * @param blobValue a BLOB value of the column
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link #ofBlob(String,
   *     ByteBuffer)} instead
   */
  @Deprecated
  public Key(String columnName, ByteBuffer blobValue) {
    values = Collections.singletonList(new BlobValue(columnName, blobValue));
  }

  /**
   * Constructs a {@code Key} with two columns
   *
   * @param n1 a column name of the 1st column
   * @param v1 a value of the 1st column
   * @param n2 a column name of the 2nd column
   * @param v2 a value of the 2nd column
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link #of(String,
   *     Object, String, Object)} instead
   */
  @Deprecated
  public Key(String n1, Object v1, String n2, Object v2) {
    values = Arrays.asList(toValue(n1, v1), toValue(n2, v2));
  }

  /**
   * Constructs a {@code Key} with three columns
   *
   * @param n1 a column name of the 1st column
   * @param v1 a value of the 1st column
   * @param n2 a column name of the 2nd column
   * @param v2 a value of the 2nd column
   * @param n3 a column name of the 3rd column
   * @param v3 a value of the 3rd column
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link #of(String,
   *     Object, String, Object, String, Object)} instead
   */
  @Deprecated
  public Key(String n1, Object v1, String n2, Object v2, String n3, Object v3) {
    values = Arrays.asList(toValue(n1, v1), toValue(n2, v2), toValue(n3, v3));
  }

  /**
   * Constructs a {@code Key} with four columns
   *
   * @param n1 a column name of the 1st column
   * @param v1 a value of the 1st column
   * @param n2 a column name of the 2nd column
   * @param v2 a value of the 2nd column
   * @param n3 a column name of the 3rd column
   * @param v3 a value of the 3rd column
   * @param n4 a column of the 4th column
   * @param v4 a value of the 4th column
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link #of(String,
   *     Object, String, Object, String, Object, String, Object)} instead
   */
  @Deprecated
  public Key(
      String n1, Object v1, String n2, Object v2, String n3, Object v3, String n4, Object v4) {
    values = Arrays.asList(toValue(n1, v1), toValue(n2, v2), toValue(n3, v3), toValue(n4, v4));
  }

  /**
   * Constructs a {@code Key} with five columns
   *
   * @param n1 a column name of the 1st column
   * @param v1 a value of the 1st column
   * @param n2 a column name of the 2nd column
   * @param v2 a value of the 2nd column
   * @param n3 a column name of the 3rd column
   * @param v3 a value of the 3rd column
   * @param n4 a column of the 4th column
   * @param v4 a value of the 4th column
   * @param n5 a column of the 5th column
   * @param v5 a value of the 5th column
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link #of(String,
   *     Object, String, Object, String, Object, String, Object, String, Object)} instead
   */
  @Deprecated
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
   * Returns the list of {@code Value} which this key is composed of.
   *
   * <p>This method is primarily for internal use. Breaking changes can and will be introduced to
   * this method. Users should not depend on it.
   *
   * @return list of {@code Value} which this key is composed of
   */
  @Nonnull
  public List<Value<?>> get() {
    return Collections.unmodifiableList(values);
  }

  /**
   * Returns the size of the list of columns which this key is composed of.
   *
   * @return the size of list of columns which this key is composed of
   */
  public int size() {
    return values.size();
  }

  /**
   * Return the column name of the i-th column which this key is composed of.
   *
   * @param i the position of the column which this key is composed of
   * @return the column name of the i-th column which this key is composed of
   */
  public String getColumnName(int i) {
    return values.get(i).getName();
  }

  /**
   * Returns the BOOLEAN value of the i-th column which this key is composed of.
   *
   * @param i the position of the column which this key is composed of
   * @return the BOOLEAN value of the i-th column which this key is composed of
   */
  public boolean getBoolean(int i) {
    return values.get(i).getAsBoolean();
  }

  /**
   * Returns the INT value of the i-th column which this key is composed of.
   *
   * @param i the position of the column which this key is composed of
   * @return the INT value of the i-th column which this key is composed of
   */
  public int getInt(int i) {
    return values.get(i).getAsInt();
  }

  /**
   * Returns the BIGINT value of the i-th column which this key is composed of.
   *
   * @param i the position of the column which this key is composed of
   * @return the BIGINT value of the i-th column which this key is composed of
   */
  public long getBigInt(int i) {
    return values.get(i).getAsLong();
  }

  /**
   * Returns the FLOAT value of the i-th column which this key is composed of.
   *
   * @param i the position of the column which this key is composed of
   * @return the FLOAT value of the i-th column which this key is composed of
   */
  public float getFloat(int i) {
    return values.get(i).getAsFloat();
  }

  /**
   * Returns the DOUBLE value of the i-th column which this key is composed of.
   *
   * @param i the position of the column which this key is composed of
   * @return the DOUBLE value of the i-th column which this key is composed of
   */
  public double getDouble(int i) {
    return values.get(i).getAsDouble();
  }

  /**
   * Returns the TEXT value of the i-th column which this key is composed of.
   *
   * @param i the position of the column which this key is composed of
   * @return the TEXT value of the i-th column which this key is composed of
   */
  public String getText(int i) {
    return values.get(i).getAsString().orElse(null);
  }

  /**
   * Returns the BLOB value of the i-th column which this key is composed of as a ByteBuffer type.
   *
   * @param i the position of the column which this key is composed of
   * @return the BLOB value of the i-th column which this key is composed of as a ByteBuffer type
   */
  public ByteBuffer getBlob(int i) {
    return getBlobAsByteBuffer(i);
  }

  /**
   * Returns the BLOB value of the i-th column which this key is composed of as a ByteBuffer type.
   *
   * @param i the position of the column which this key is composed of
   * @return the BLOB value of the i-th column which this key is composed of as a ByteBuffer type
   */
  public ByteBuffer getBlobAsByteBuffer(int i) {
    return values.get(i).getAsByteBuffer().orElse(null);
  }

  /**
   * Returns the BLOB value of the i-th column which this key is composed of as a byte array type.
   *
   * @param i the position of the column which this key is composed of
   * @return the BLOB value of the i-th column which this key is composed of as a byte array type
   */
  public byte[] getBlobAsBytes(int i) {
    return values.get(i).getAsBytes().orElse(null);
  }

  /**
   * Returns the value of the i-th column which this key is composed of as an Object type.
   *
   * <p>If the columns is a BOOLEAN type, it returns a {@code Boolean} object. If the columns is an
   * INT type, it returns an {@code Integer} object. If the columns is a BIGINT type, it returns a
   * {@code LONG} object. If the columns is a FLOAT type, it returns a {@code FLOAT} object. If the
   * columns is a DOUBLE type, it returns a {@code DOUBLE} object. If the columns is a TEXT type, it
   * returns a {@code String} object. If the columns is a BLOB type, it returns a {@code ByteBuffer}
   * object.
   *
   * @param i the position of the column which this key is composed of
   * @return the value of the i-th column which this key is composed of as an Object type
   */
  public Object getAsObject(int i) {
    if (values.get(i) instanceof BooleanValue) {
      return getBoolean(i);
    } else if (values.get(i) instanceof IntValue) {
      return getInt(i);
    } else if (values.get(i) instanceof BigIntValue) {
      return getBigInt(i);
    } else if (values.get(i) instanceof FloatValue) {
      return getFloat(i);
    } else if (values.get(i) instanceof DoubleValue) {
      return getDouble(i);
    } else if (values.get(i) instanceof TextValue) {
      return getText(i);
    } else if (values.get(i) instanceof BlobValue) {
      return getBlob(i);
    } else {
      throw new AssertionError();
    }
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
    return Ordering.<Value<?>>natural().lexicographical().compare(values, o.values);
  }

  /**
   * Creates a {@code Key} object with a single column with a BOOLEAN type
   *
   * @param columnName a column name
   * @param value a BOOLEAN value of the column
   * @return a {@code Key} object
   */
  public static Key ofBoolean(String columnName, boolean value) {
    return new Key(columnName, value);
  }

  /**
   * Creates a {@code Key} object with a single column with an INT type
   *
   * @param columnName a column name
   * @param value an INT value of the column
   * @return a {@code Key} object
   */
  public static Key ofInt(String columnName, int value) {
    return new Key(columnName, value);
  }

  /**
   * Creates a {@code Key} object with a single column with a BIGINT type
   *
   * @param columnName a column name
   * @param value a BIGINT value of the column
   * @return a {@code Key} object
   */
  public static Key ofBigInt(String columnName, long value) {
    return new Key(columnName, value);
  }

  /**
   * Creates a {@code Key} object with a single column with a FLOAT type
   *
   * @param columnName a column name
   * @param value a FLOAT value of the column
   * @return a {@code Key} object
   */
  public static Key ofFloat(String columnName, float value) {
    return new Key(columnName, value);
  }

  /**
   * Creates a {@code Key} object with a single column with a DOUBLE type
   *
   * @param columnName a column name
   * @param value a DOUBLE value of the column
   * @return a {@code Key} object
   */
  public static Key ofDouble(String columnName, double value) {
    return new Key(columnName, value);
  }

  /**
   * Creates a {@code Key} object with a single column with a TEXT type
   *
   * @param columnName a column name
   * @param value a TEXT value of the column
   * @return a {@code Key} object
   */
  public static Key ofText(String columnName, String value) {
    return new Key(columnName, value);
  }

  /**
   * Creates a {@code Key} object with a single column with a BLOB type from a byte array
   *
   * @param columnName a column name
   * @param value a TEXT value of the column
   * @return a {@code Key} object
   */
  public static Key ofBlob(String columnName, byte[] value) {
    return new Key(columnName, value);
  }

  /**
   * Creates a {@code Key} object with a single column with a BLOB type from a ByteBuffer
   *
   * @param columnName a column name
   * @param value a TEXT value of the column
   * @return a {@code Key} object
   */
  public static Key ofBlob(String columnName, ByteBuffer value) {
    return new Key(columnName, value);
  }

  /**
   * Create a {@code Key} object with two columns
   *
   * @param n1 a column name of the 1st column
   * @param v1 a value of the 1st column
   * @param n2 a column name of the 2nd column
   * @param v2 a value of the 2nd column
   * @return a {@code Key} object
   */
  public static Key of(String n1, Object v1, String n2, Object v2) {
    return new Key(n1, v1, n2, v2);
  }

  /**
   * Create a {@code Key} object with three columns
   *
   * @param n1 a column name of the 1st column
   * @param v1 a value of the 1st column
   * @param n2 a column name of the 2nd column
   * @param v2 a value of the 2nd column
   * @param n3 a column name of the 3rd column
   * @param v3 a value of the 3rd column
   * @return a {@code Key} object
   */
  public static Key of(String n1, Object v1, String n2, Object v2, String n3, Object v3) {
    return new Key(n1, v1, n2, v2, n3, v3);
  }

  /**
   * Create a {@code Key} object with four columns
   *
   * @param n1 a column name of the 1st column
   * @param v1 a value of the 1st column
   * @param n2 a column name of the 2nd column
   * @param v2 a value of the 2nd column
   * @param n3 a column name of the 3rd column
   * @param v3 a value of the 3rd column
   * @param n4 a column of the 4th column
   * @param v4 a value of the 4th column
   * @return a {@code Key} object
   */
  public static Key of(
      String n1, Object v1, String n2, Object v2, String n3, Object v3, String n4, Object v4) {
    return new Key(n1, v1, n2, v2, n3, v3, n4, v4);
  }

  /**
   * Create a {@code Key} object with five columns
   *
   * @param n1 a column name of the 1st column
   * @param v1 a value of the 1st column
   * @param n2 a column name of the 2nd column
   * @param v2 a value of the 2nd column
   * @param n3 a column name of the 3rd column
   * @param v3 a value of the 3rd column
   * @param n4 a column of the 4th column
   * @param v4 a value of the 4th column
   * @param n5 a column of the 5th column
   * @param v5 a value of the 5th column
   * @return a {@code Key} object
   */
  public static Key of(
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
    return new Key(n1, v1, n2, v2, n3, v3, n4, v4, n5, v5);
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
     * Adds BOOLEAN value as an element of Key.
     *
     * @param columnName a column name to add
     * @param value a BOOLEAN value to add
     * @return a builder object
     */
    public Builder addBoolean(String columnName, boolean value) {
      values.add(new BooleanValue(columnName, value));
      return this;
    }

    /**
     * Adds INT value as an element of Key.
     *
     * @param columnName a column name to add
     * @param value a INT value to add
     * @return a builder object
     */
    public Builder addInt(String columnName, int value) {
      values.add(new IntValue(columnName, value));
      return this;
    }

    /**
     * Adds BIGINT value as an element of Key.
     *
     * @param columnName a column name to add
     * @param value a BIGINT value to add
     * @return a builder object
     */
    public Builder addBigInt(String columnName, long value) {
      values.add(new BigIntValue(columnName, value));
      return this;
    }

    /**
     * Adds FLOAT value as an element of Key.
     *
     * @param columnName a column name to add
     * @param value a FLOAT value to add
     * @return a builder object
     */
    public Builder addFloat(String columnName, float value) {
      values.add(new FloatValue(columnName, value));
      return this;
    }

    /**
     * Adds DOUBLE value as an element of Key.
     *
     * @param columnName a column name to add
     * @param value a DOUBLE value to add
     * @return a builder object
     */
    public Builder addDouble(String columnName, double value) {
      values.add(new DoubleValue(columnName, value));
      return this;
    }

    /**
     * Adds TEXT value as an element of Key.
     *
     * @param columnName a column name to add
     * @param value a TEXT value to add
     * @return a builder object
     */
    public Builder addText(String columnName, String value) {
      values.add(new TextValue(columnName, value));
      return this;
    }

    /**
     * Adds BLOB value as an element of Key with a byte array.
     *
     * @param columnName a column name to add
     * @param value a BLOB value to add
     * @return a builder object
     */
    public Builder addBlob(String columnName, byte[] value) {
      values.add(new BlobValue(columnName, value));
      return this;
    }

    /**
     * Adds BLOB value as an element of Key with a ByteBuffer.
     *
     * @param columnName a column name to add
     * @param value a BLOB value to add
     * @return a builder object
     */
    public Builder addBlob(String columnName, ByteBuffer value) {
      values.add(new BlobValue(columnName, value));
      return this;
    }

    /**
     * @param value a value to add
     * @return a builder object
     * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
     */
    @Deprecated
    public Builder add(Value<?> value) {
      values.add(value);
      return this;
    }

    /**
     * @param values a list of values to add
     * @return a builder object
     * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
     */
    @Deprecated
    public Builder addAll(Collection<? extends Value<?>> values) {
      this.values.addAll(values);
      return this;
    }

    /**
     * Returns the current number of the elements of Key.
     *
     * @return the current number of the elements of Key
     */
    public int size() {
      return values.size();
    }

    public Key build() {
      return new Key(values);
    }
  }
}
