package com.scalar.db.io;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * An abstraction for a column.
 *
 * <p>This class and its implementation classes of it are primarily for internal use. Breaking
 * changes can and will be introduced to them. Users should not depend on them.
 */
public interface Column<T> extends Comparable<Column<T>> {

  /**
   * Returns the name of this {@code Column}.
   *
   * @return the name of this {@code Column}
   */
  String getName();

  /**
   * Returns the value of this {@code Column}.
   *
   * @return the value of this {@code Column}
   */
  Optional<T> getValue();

  /**
   * Creates a copy of this {@code Column} with the specified name.
   *
   * @param name name of a {@code Column}
   * @return a {@code Column} which has the same value
   */
  Column<T> copyWith(String name);

  /**
   * Returns the data type of this {@code Column}.
   *
   * @return the data type of this {@code Column}
   */
  DataType getDataType();

  /**
   * Indicates whether the value of this {@code Column} is NULL.
   *
   * @return whether the value of this {@code Column} is NULL
   */
  boolean hasNullValue();

  /**
   * Returns the BOOLEAN value of this {@code Column} as a Java boolean type.
   *
   * <p>Note that, due to its signature, this method cannot return null. If the value is NULL, it
   * will return false. If this doesn't work for you, either call {@link #hasNullValue()} before
   * calling this method, or use {@link #getValueAsObject()} instead.
   *
   * @return the value of this {@code Column}
   */
  default boolean getBooleanValue() {
    throw new UnsupportedOperationException("The data type of this column is " + getDataType());
  }

  /**
   * Returns the INT value of this {@code Column} as a Java int type.
   *
   * <p>Note that, due to its signature, this method cannot return null. If the value is NULL, it
   * will return 0. If this doesn't work for you, either call {@link #hasNullValue()} before calling
   * this method, or use {@link #getValueAsObject()} instead.
   *
   * @return the value of this {@code Column}
   */
  default int getIntValue() {
    throw new UnsupportedOperationException("The data type of this column is " + getDataType());
  }

  /**
   * Returns the BIGINT value of this {@code Column} as a Java long type.
   *
   * <p>Note that, due to its signature, this method cannot return null. If the value is NULL, it
   * will return 0. If this doesn't work for you, either call {@link #hasNullValue()} before calling
   * this method, or use {@link #getValueAsObject()} instead.
   *
   * @return the value of this {@code Column}
   */
  default long getBigIntValue() {
    throw new UnsupportedOperationException("The data type of this column is " + getDataType());
  }

  /**
   * Returns the FLOAT value of this {@code Column} as a Java float type.
   *
   * <p>Note that, due to its signature, this method cannot return null. If the value is NULL, it
   * will return 0.0. If this doesn't work for you, either call {@link #hasNullValue()} before
   * calling this method, or use {@link #getValueAsObject()} instead.
   *
   * @return the value of this {@code Column}
   */
  default float getFloatValue() {
    throw new UnsupportedOperationException("The data type of this column is " + getDataType());
  }

  /**
   * Returns the DOUBLE value of this {@code Column} as a Java double type.
   *
   * <p>Note that, due to its signature, this method cannot return null. If the value is NULL, it
   * will return 0.0. If this doesn't work for you, either call {@link #hasNullValue()} before
   * calling this method, or use {@link #getValueAsObject()} instead.
   *
   * @return the value of this {@code Column}
   */
  default double getDoubleValue() {
    throw new UnsupportedOperationException("The data type of this column is " + getDataType());
  }

  /**
   * Returns the TEXT value of this {@code Column} as a Java string type.
   *
   * @return the value of this {@code Column}. if the value is NULL, null
   */
  @Nullable
  default String getTextValue() {
    throw new UnsupportedOperationException("The data type of this column is " + getDataType());
  }

  /**
   * Returns the BLOB value of this {@code Column} as a Java byte buffer type.
   *
   * @return the value of this {@code Column}. if the value is NULL, null
   */
  @Nullable
  default ByteBuffer getBlobValue() {
    return getBlobValueAsByteBuffer();
  }

  /**
   * Returns the BLOB value of this {@code Column} as a Java byte buffer type.
   *
   * @return the value of this {@code Column}. if the value is NULL, null
   */
  @Nullable
  default ByteBuffer getBlobValueAsByteBuffer() {
    throw new UnsupportedOperationException("The data type of this column is " + getDataType());
  }

  /**
   * Returns the BLOB value of this {@code Column} as a Java byte array type.
   *
   * @return the value of this {@code Column}. if the value is NULL, null
   */
  @Nullable
  default byte[] getBlobValueAsBytes() {
    throw new UnsupportedOperationException("The data type of this column is " + getDataType());
  }

  /**
   * Returns the DATE value of this {@code Column} as a Java LocalDate type.
   *
   * @return the value of this {@code Column}. if the value is NULL, null
   */
  @Nullable
  default LocalDate getDateValue() {
    throw new UnsupportedOperationException("The data type of this column is " + getDataType());
  }

  /**
   * Returns the TIME value of this {@code Column} as a Java LocalTime type.
   *
   * @return the value of this {@code Column}. if the value is NULL, null
   */
  @Nullable
  default LocalTime getTimeValue() {
    throw new UnsupportedOperationException("The data type of this column is " + getDataType());
  }

  /**
   * Returns the TIMESTAMP value of this {@code Column} as a Java LocalDateTime type.
   *
   * @return the value of this {@code Column}. if the value is NULL, null
   */
  @Nullable
  default LocalDateTime getTimestampValue() {
    throw new UnsupportedOperationException("The data type of this column is " + getDataType());
  }

  /**
   * Returns the TIMESTAMPTZ value of this {@code Column} as a Java Instant type.
   *
   * @return the value of this {@code Column}. if the value is NULL, null
   */
  @Nullable
  default Instant getTimestampTZValue() {
    throw new UnsupportedOperationException("The data type of this column is " + getDataType());
  }

  /**
   * Returns the value of this {@code Column} as a Java object type.
   *
   * <p>If the columns is a BOOLEAN type, it returns a {@code Boolean} object. If the columns is an
   * INT type, it returns an {@code Integer} object. If the columns is a BIGINT type, it returns a
   * {@code LONG} object. If the columns is a FLOAT type, it returns a {@code FLOAT} object. If the
   * columns is a DOUBLE type, it returns a {@code DOUBLE} object. If the columns is a TEXT type, it
   * returns a {@code String} object. If the columns is a BLOB type, it returns a {@code ByteBuffer}
   * object. If the columns is a DATE type, it returns a {@code LocalDate} object. If the columns is
   * a TIME type, it returns a {@code LocalTime} object. If the columns is a TIMESTAMP type, it
   * returns a {@code LocalDateTime} object. If the columns is a TIMESTAMPTZ type, it returns a
   * {@code Instant} object.
   *
   * @return the value of this {@code Column}. if the value is NULL, null
   */
  @Nullable
  Object getValueAsObject();

  /**
   * Accepts a {@link ColumnVisitor} to be able to be traversed.
   *
   * @param visitor a visitor class used for traversing {@code Column}s
   */
  void accept(ColumnVisitor visitor);
}
