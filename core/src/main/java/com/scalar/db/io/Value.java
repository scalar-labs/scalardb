package com.scalar.db.io;

import java.nio.ByteBuffer;
import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * An abstraction for storage entry's value (column).
 *
 * @author Hiroyuki Yamada
 * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
 */
@Deprecated
public interface Value<T> extends Comparable<Value<T>> {

  /**
   * Returns the name of the value (column).
   *
   * @return the name of this value (column)
   */
  String getName();

  /**
   * Creates a copy of the value (column) with the specified name.
   *
   * @param name name of a {@code Value}
   * @return a {@code Value} which has the same content of this value
   */
  Value<T> copyWith(String name);

  /**
   * Accepts a {@link ValueVisitor} to be able to be traversed.
   *
   * @param v a visitor class used for traversing {@code Value}s (columns)
   */
  void accept(ValueVisitor v);

  /**
   * Returns the value of this {@code Value} (column).
   *
   * @return the value of this {@code Value} (column)
   */
  @Nonnull
  T get();

  /**
   * Returns the data type of this {@code Value} (column).
   *
   * @return the data type of this {@code Value} (column)
   */
  DataType getDataType();

  /**
   * Returns the value of this {@code Value} (column) as a boolean type.
   *
   * @return the value of this {@code Value} (column)
   */
  default boolean getAsBoolean() {
    throw new UnsupportedOperationException("The data type of this column is " + getDataType());
  }

  /**
   * Returns the value of this {@code Value} (column) as an integer type.
   *
   * @return the value of this {@code Value} (column)
   */
  default int getAsInt() {
    throw new UnsupportedOperationException("The data type of this column is " + getDataType());
  }

  /**
   * Returns the value of this {@code Value} (column) as a long type.
   *
   * @return the value of this {@code Value} (column)
   */
  default long getAsLong() {
    throw new UnsupportedOperationException("The data type of this column is " + getDataType());
  }

  /**
   * Returns the value of this {@code Value} (column) as a float type.
   *
   * @return the value of this {@code Value} (column)
   */
  default float getAsFloat() {
    throw new UnsupportedOperationException("The data type of this column is " + getDataType());
  }

  /**
   * Returns the value of this {@code Value} (column) as a double type.
   *
   * @return the value of this {@code Value} (column)
   */
  default double getAsDouble() {
    throw new UnsupportedOperationException("The data type of this column is " + getDataType());
  }

  /**
   * Returns the value of this {@code Value} (column) as a string type.
   *
   * @return the value of this {@code Value} (column)
   */
  default Optional<String> getAsString() {
    throw new UnsupportedOperationException("The data type of this column is " + getDataType());
  }

  /**
   * Returns the value of this {@code Value} (column) as a byte array type.
   *
   * @return the value of this {@code Value} (column)
   */
  default Optional<byte[]> getAsBytes() {
    throw new UnsupportedOperationException("The data type of this column is " + getDataType());
  }

  /**
   * Returns the value of this {@code Value} (column) as a byte buffer.
   *
   * @return the value of this {@code Value} (column)
   */
  default Optional<ByteBuffer> getAsByteBuffer() {
    throw new UnsupportedOperationException("The data type of this column is " + getDataType());
  }
}
