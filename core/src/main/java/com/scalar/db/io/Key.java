package com.scalar.db.io;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.util.ScalarDbUtils;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * An abstraction for keys
 *
 * @author Hiroyuki Yamada
 */
@Immutable
public final class Key implements Comparable<Key>, Iterable<Value<?>> {
  private final List<Column<?>> columns;

  /**
   * Constructs a {@code Key} with the specified {@link Value}s
   *
   * @param values one or more {@link Value}s which this key is composed of
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  public Key(Value<?>... values) {
    checkNotNull(values);
    this.columns = new ArrayList<>(values.length);
    Arrays.stream(values).map(ScalarDbUtils::toColumn).forEach(columns::add);
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
    this.columns = new ArrayList<>(values.size());
    values.stream().map(ScalarDbUtils::toColumn).forEach(columns::add);
  }

  /**
   * Constructs a {@code Key} with the specified list of {@link Column}s
   *
   * @param columns a list of {@link Column}s which this key is composed of
   */
  private Key(Collection<Column<?>> columns) {
    checkNotNull(columns);
    this.columns = new ArrayList<>(columns);
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
    columns = Collections.singletonList(BooleanColumn.of(columnName, booleanValue));
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
    columns = Collections.singletonList(IntColumn.of(columnName, intValue));
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
    columns = Collections.singletonList(BigIntColumn.of(columnName, bigIntValue));
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
    columns = Collections.singletonList(FloatColumn.of(columnName, floatValue));
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
    columns = Collections.singletonList(DoubleColumn.of(columnName, doubleValue));
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
    columns = Collections.singletonList(TextColumn.of(columnName, textValue));
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
    columns = Collections.singletonList(BlobColumn.of(columnName, blobValue));
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
    columns = Collections.singletonList(BlobColumn.of(columnName, blobValue));
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
    columns = Arrays.asList(toColumn(n1, v1), toColumn(n2, v2));
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
    columns = Arrays.asList(toColumn(n1, v1), toColumn(n2, v2), toColumn(n3, v3));
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
    columns = Arrays.asList(toColumn(n1, v1), toColumn(n2, v2), toColumn(n3, v3), toColumn(n4, v4));
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
    columns =
        Arrays.asList(
            toColumn(n1, v1),
            toColumn(n2, v2),
            toColumn(n3, v3),
            toColumn(n4, v4),
            toColumn(n5, v5));
  }

  private Column<?> toColumn(String name, Object value) {
    if (value instanceof Boolean) {
      return BooleanColumn.of(name, (Boolean) value);
    } else if (value instanceof Integer) {
      return IntColumn.of(name, (Integer) value);
    } else if (value instanceof Long) {
      return BigIntColumn.of(name, (Long) value);
    } else if (value instanceof Float) {
      return FloatColumn.of(name, (Float) value);
    } else if (value instanceof Double) {
      return DoubleColumn.of(name, (Double) value);
    } else if (value instanceof String) {
      return TextColumn.of(name, (String) value);
    } else if (value instanceof byte[]) {
      return BlobColumn.of(name, (byte[]) value);
    } else if (value instanceof ByteBuffer) {
      return BlobColumn.of(name, (ByteBuffer) value);
    } else if (value instanceof LocalDate) {
      return DateColumn.of(name, (LocalDate) value);
    } else if (value instanceof LocalTime) {
      return TimeColumn.of(name, (LocalTime) value);
    } else if (value instanceof LocalDateTime) {
      return TimestampColumn.of(name, (LocalDateTime) value);
    } else if (value instanceof Instant) {
      return TimestampTZColumn.of(name, (Instant) value);
    } else {
      throw new IllegalArgumentException(
          CoreError.KEY_BUILD_ERROR_UNSUPPORTED_TYPE.buildMessage(
              name, value.getClass().getName()));
    }
  }

  /**
   * Returns the list of {@code Value} which this key is composed of.
   *
   * <p>This method is primarily for internal use. Breaking changes can and will be introduced to
   * this method. Users should not depend on it.
   *
   * @return list of {@code Value} which this key is composed of
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  @Nonnull
  public List<Value<?>> get() {
    return columns.stream().map(ScalarDbUtils::toValue).collect(Collectors.toList());
  }

  /**
   * Returns the list of {@code Column} which this key is composed of.
   *
   * <p>This method is primarily for internal use. Breaking changes can and will be introduced to
   * this method. Users should not depend on it.
   *
   * @return list of {@code Column} which this key is composed of
   */
  public List<Column<?>> getColumns() {
    return ImmutableList.copyOf(columns);
  }

  /**
   * Returns the size of the list of columns which this key is composed of.
   *
   * @return the size of list of columns which this key is composed of
   */
  public int size() {
    return columns.size();
  }

  /**
   * Return the column name of the i-th column which this key is composed of.
   *
   * @param i the position of the column which this key is composed of
   * @return the column name of the i-th column which this key is composed of
   */
  public String getColumnName(int i) {
    return columns.get(i).getName();
  }

  /**
   * Returns the BOOLEAN value of the i-th column which this key is composed of.
   *
   * @param i the position of the column which this key is composed of
   * @return the BOOLEAN value of the i-th column which this key is composed of
   */
  public boolean getBooleanValue(int i) {
    return columns.get(i).getBooleanValue();
  }

  /**
   * Returns the INT value of the i-th column which this key is composed of.
   *
   * @param i the position of the column which this key is composed of
   * @return the INT value of the i-th column which this key is composed of
   */
  public int getIntValue(int i) {
    return columns.get(i).getIntValue();
  }

  /**
   * Returns the BIGINT value of the i-th column which this key is composed of.
   *
   * @param i the position of the column which this key is composed of
   * @return the BIGINT value of the i-th column which this key is composed of
   */
  public long getBigIntValue(int i) {
    return columns.get(i).getBigIntValue();
  }

  /**
   * Returns the FLOAT value of the i-th column which this key is composed of.
   *
   * @param i the position of the column which this key is composed of
   * @return the FLOAT value of the i-th column which this key is composed of
   */
  public float getFloatValue(int i) {
    return columns.get(i).getFloatValue();
  }

  /**
   * Returns the DOUBLE value of the i-th column which this key is composed of.
   *
   * @param i the position of the column which this key is composed of
   * @return the DOUBLE value of the i-th column which this key is composed of
   */
  public double getDoubleValue(int i) {
    return columns.get(i).getDoubleValue();
  }

  /**
   * Returns the TEXT value of the i-th column which this key is composed of.
   *
   * @param i the position of the column which this key is composed of
   * @return the TEXT value of the i-th column which this key is composed of
   */
  public String getTextValue(int i) {
    return columns.get(i).getTextValue();
  }

  /**
   * Returns the BLOB value of the i-th column which this key is composed of as a ByteBuffer type.
   *
   * @param i the position of the column which this key is composed of
   * @return the BLOB value of the i-th column which this key is composed of as a ByteBuffer type
   */
  public ByteBuffer getBlobValue(int i) {
    return getBlobValueAsByteBuffer(i);
  }

  /**
   * Returns the BLOB value of the i-th column which this key is composed of as a ByteBuffer type.
   *
   * @param i the position of the column which this key is composed of
   * @return the BLOB value of the i-th column which this key is composed of as a ByteBuffer type
   */
  public ByteBuffer getBlobValueAsByteBuffer(int i) {
    return columns.get(i).getBlobValueAsByteBuffer();
  }

  /**
   * Returns the BLOB value of the i-th column which this key is composed of as a byte array type.
   *
   * @param i the position of the column which this key is composed of
   * @return the BLOB value of the i-th column which this key is composed of as a byte array type
   */
  public byte[] getBlobValueAsBytes(int i) {
    return columns.get(i).getBlobValueAsBytes();
  }

  /**
   * Returns the DATE value of the i-th column which this key is composed of.
   *
   * @param i the position of the column which this key is composed of
   * @return the DATE value of the i-th column which this key is composed of as LocalDate type
   */
  public LocalDate getDateValue(int i) {
    return columns.get(i).getDateValue();
  }

  /**
   * Returns the TIME value of the i-th column which this key is composed of.
   *
   * @param i the position of the column which this key is composed of
   * @return the TIME value of the i-th column which this key is composed of as LocalTime type
   */
  public LocalTime getTimeValue(int i) {
    return columns.get(i).getTimeValue();
  }

  /**
   * Returns the TIMESTAMP value of the i-th column which this key is composed of.
   *
   * @param i the position of the column which this key is composed of
   * @return the TIMESTAMP value of the i-th column which this key is composed of as LocalDateTime
   *     type
   */
  public LocalDateTime getTimestampValue(int i) {
    return columns.get(i).getTimestampValue();
  }

  /**
   * Returns the TIMESTAMPTZ value of the i-th column which this key is composed of.
   *
   * @param i the position of the column which this key is composed of
   * @return the TIMESTAMPTZ value of the i-th column which this key is composed of as Instant type
   */
  public Instant getTimestampTZValue(int i) {
    return columns.get(i).getTimestampTZValue();
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
  public Object getValueAsObject(int i) {
    return columns.get(i).getValueAsObject();
  }

  @Override
  public int hashCode() {
    return Objects.hash(columns);
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
    return columns.equals(that.columns);
  }

  @Override
  public String toString() {
    MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
    columns.forEach(helper::addValue);
    return helper.toString();
  }

  /**
   * @return an iterator of the values which this key is composed of
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  @Nonnull
  @Override
  public Iterator<Value<?>> iterator() {
    return new Iterator<Value<?>>() {

      private final Iterator<Column<?>> iterator = columns.iterator();

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public Value<?> next() {
        return ScalarDbUtils.toValue(iterator.next());
      }
    };
  }

  @Override
  public int compareTo(Key o) {
    return Ordering.<Column<?>>natural().lexicographical().compare(columns, o.columns);
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
   * Creates a {@code Key} object with a single column with a DATE type
   *
   * @param columnName a column name
   * @param value a DATE value of the column as LocalDate type
   * @return a {@code Key} object
   */
  public static Key ofDate(String columnName, LocalDate value) {
    return new Key(Collections.singletonList(DateColumn.of(columnName, value)));
  }

  /**
   * Creates a {@code Key} object with a single column with a TIME type
   *
   * @param columnName a column name
   * @param value a TIME value of the column as LocalTime type
   * @return a {@code Key} object
   */
  public static Key ofTime(String columnName, LocalTime value) {
    return new Key(Collections.singletonList(TimeColumn.of(columnName, value)));
  }

  /**
   * Creates a {@code Key} object with a single column with a TIMESTAMP type
   *
   * @param columnName a column name
   * @param value a TIMESTAMP value of the column as LocalDateTime type
   * @return a {@code Key} object
   */
  public static Key ofTimestamp(String columnName, LocalDateTime value) {
    return new Key(Collections.singletonList(TimestampColumn.of(columnName, value)));
  }

  /**
   * Creates a {@code Key} object with a single column with a TIMESTAMPTZ type
   *
   * @param columnName a column name
   * @param value a TIMESTAMPTZ value of the column as LocalDateTime type
   * @return a {@code Key} object
   */
  public static Key ofTimestampTZ(String columnName, Instant value) {
    return new Key(Collections.singletonList(TimestampTZColumn.of(columnName, value)));
  }

  /**
   * Create an empty {@code Key} object
   *
   * @return an empty {@code Key} object
   */
  public static Key of() {
    return new Key();
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
    private final List<Column<?>> columns = new ArrayList<>();

    private Builder() {}

    /**
     * Adds BOOLEAN value as an element of Key.
     *
     * @param columnName a column name to add
     * @param value a BOOLEAN value to add
     * @return a builder object
     */
    public Builder addBoolean(String columnName, boolean value) {
      columns.add(BooleanColumn.of(columnName, value));
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
      columns.add(IntColumn.of(columnName, value));
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
      columns.add(BigIntColumn.of(columnName, value));
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
      columns.add(FloatColumn.of(columnName, value));
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
      columns.add(DoubleColumn.of(columnName, value));
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
      columns.add(TextColumn.of(columnName, value));
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
      columns.add(BlobColumn.of(columnName, value));
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
      columns.add(BlobColumn.of(columnName, value));
      return this;
    }

    /**
     * Adds DATE value as an element of Key.
     *
     * @param columnName a column name to add
     * @param value a DATE value to add as LocalDate type
     * @return a builder object
     */
    public Builder addDate(String columnName, LocalDate value) {
      columns.add(DateColumn.of(columnName, value));
      return this;
    }

    /**
     * Adds TIME value as an element of Key.
     *
     * @param columnName a column name to add
     * @param value a TIME value to add as LocalTime type
     * @return a builder object
     */
    public Builder addTime(String columnName, LocalTime value) {
      columns.add(TimeColumn.of(columnName, value));
      return this;
    }
    /**
     * Adds TIMESTAMP value as an element of Key.
     *
     * @param columnName a column name to add
     * @param value a TIMESTAMP value to add as LocalDateTime type
     * @return a builder object
     */
    public Builder addTimestamp(String columnName, LocalDateTime value) {
      columns.add(TimestampColumn.of(columnName, value));
      return this;
    }

    /**
     * Adds TIMESTAMPTZ value as an element of Key.
     *
     * @param columnName a column name to add
     * @param value a TIMESTAMPTZ value to add as Instant type
     * @return a builder object
     */
    public Builder addTimestampTZ(String columnName, Instant value) {
      columns.add(TimestampTZColumn.of(columnName, value));
      return this;
    }

    /**
     * @param value a value to add
     * @return a builder object
     * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
     */
    @Deprecated
    public Builder add(Value<?> value) {
      columns.add(ScalarDbUtils.toColumn(value));
      return this;
    }

    /**
     * @param values a list of values to add
     * @return a builder object
     * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
     */
    @Deprecated
    public Builder addAll(Collection<? extends Value<?>> values) {
      values.stream().map(ScalarDbUtils::toColumn).forEach(columns::add);
      return this;
    }

    /**
     * Adds a column as an element of Key.
     *
     * <p>This method is primarily for internal use. Breaking changes can and will be introduced to
     * this method. Users should not depend on it.
     *
     * @param column a column to add
     * @return a builder object
     */
    public Builder add(Column<?> column) {
      columns.add(column);
      return this;
    }

    /**
     * Returns the current number of the elements of Key.
     *
     * @return the current number of the elements of Key
     */
    public int size() {
      return columns.size();
    }

    public Key build() {
      return new Key(columns);
    }
  }
}
