package com.scalar.db.sql;

import java.nio.ByteBuffer;
import java.util.Set;
import javax.annotation.Nullable;

public interface Record {

  /**
   * Indicates whether the value of the specified column is NULL.
   *
   * @param columnName a column name of the value
   * @return whether the value of the specified column is NULL
   */
  boolean isNull(String columnName);

  /**
   * Indicates whether the value of the i-th column is NULL.
   *
   * @param i the position of the column
   * @return whether the value of the specified column is NULL
   */
  boolean isNull(int i);

  /**
   * Returns the BOOLEAN value of the specified column as a Java boolean type.
   *
   * <p>Note that, due to its signature, this method cannot return null. If the value is NULL, it
   * will return 0. If this doesn't work for you, either call {@link #isNull(String)} before calling
   * this method, or use {@link #getAsObject(String)} instead.
   *
   * @param columnName a column name of the value
   * @return the BOOLEAN value of the specified column as a Java boolean type
   */
  boolean getBoolean(String columnName);

  /**
   * Returns the BOOLEAN value of the i-th column as a Java boolean type.
   *
   * <p>Note that, due to its signature, this method cannot return null. If the value is NULL, it
   * will return 0. If this doesn't work for you, either call {@link #isNull(String)} before calling
   * this method, or use {@link #getAsObject(String)} instead.
   *
   * @param i the position of the column
   * @return the BOOLEAN value of the specified column as a Java boolean type
   */
  boolean getBoolean(int i);

  /**
   * Returns the INT value of the specified column as a Java int type.
   *
   * <p>Note that, due to its signature, this method cannot return null. If the value is NULL, it
   * will return 0. If this doesn't work for you, either call {@link #isNull(String)} before calling
   * this method, or use {@link #getAsObject(String)} instead.
   *
   * @param columnName a column name of the value
   * @return the INT value of the specified column as a Java int type
   */
  int getInt(String columnName);

  /**
   * Returns the INT value of the i-th column as a Java int type.
   *
   * <p>Note that, due to its signature, this method cannot return null. If the value is NULL, it
   * will return 0. If this doesn't work for you, either call {@link #isNull(String)} before calling
   * this method, or use {@link #getAsObject(String)} instead.
   *
   * @param i the position of the column
   * @return the INT value of the specified column as a Java int type
   */
  int getInt(int i);

  /**
   * Returns the BIGINT value of the specified column as a Java long type.
   *
   * <p>Note that, due to its signature, this method cannot return null. If the value is NULL, it
   * will return 0. If this doesn't work for you, either call {@link #isNull(String)} before calling
   * this method, or use {@link #getAsObject(String)} instead.
   *
   * @param columnName a column name of the value
   * @return the BIGINT value of the specified column as a Java long type
   */
  long getBigInt(String columnName);

  /**
   * Returns the BIGINT value of the i-th column as a Java long type.
   *
   * <p>Note that, due to its signature, this method cannot return null. If the value is NULL, it
   * will return 0. If this doesn't work for you, either call {@link #isNull(String)} before calling
   * this method, or use {@link #getAsObject(String)} instead.
   *
   * @param i the position of the column
   * @return the BIGINT value of the specified column as a Java long type
   */
  long getBigInt(int i);

  /**
   * Returns the FLOAT value of the specified column as a Java float type.
   *
   * <p>Note that, due to its signature, this method cannot return null. If the value is NULL, it
   * will return 0.0. If this doesn't work for you, either call {@link #isNull(String)} before
   * calling this method, or use {@link #getAsObject(String)} instead.
   *
   * @param columnName a column name of the value
   * @return the FLOAT value of the specified column as a Java float type
   */
  float getFloat(String columnName);

  /**
   * Returns the FLOAT value of the i-th column as a Java float type.
   *
   * <p>Note that, due to its signature, this method cannot return null. If the value is NULL, it
   * will return 0.0. If this doesn't work for you, either call {@link #isNull(String)} before
   * calling this method, or use {@link #getAsObject(String)} instead.
   *
   * @param i the position of the column
   * @return the FLOAT value of the specified column as a Java float type
   */
  float getFloat(int i);

  /**
   * Returns the DOUBLE value of the specified column as a Java double type.
   *
   * <p>Note that, due to its signature, this method cannot return null. If the value is NULL, it
   * will return 0.0. If this doesn't work for you, either call {@link #isNull(String)} before
   * calling this method, or use {@link #getAsObject(String)} instead.
   *
   * @param columnName a column name of the value
   * @return the DOUBLE value of the specified column as a Java double type
   */
  double getDouble(String columnName);

  /**
   * Returns the DOUBLE value of the i-th column as a Java double type.
   *
   * <p>Note that, due to its signature, this method cannot return null. If the value is NULL, it
   * will return 0.0. If this doesn't work for you, either call {@link #isNull(String)} before
   * calling this method, or use {@link #getAsObject(String)} instead.
   *
   * @param i the position of the column
   * @return the DOUBLE value of the specified column as a Java double type
   */
  double getDouble(int i);

  /**
   * Returns the TEXT value of the specified column as a Java String type.
   *
   * @param columnName a column name of the value
   * @return the TEXT value of the specified column as a Java String type. If the value is NULL,
   *     null
   */
  @Nullable
  String getText(String columnName);

  /**
   * Returns the TEXT value of the i-th column as a Java String type.
   *
   * @param i the position of the column
   * @return the TEXT value of the specified column as a Java String type. If the value is NULL,
   *     null
   */
  @Nullable
  String getText(int i);

  /**
   * Returns the BLOB value of the specified column as a Java ByteBuffer type.
   *
   * @param columnName a column name of the value
   * @return the BLOB value of the specified column as a Java ByteBuffer type. If the value is NULL,
   *     null
   */
  @Nullable
  default ByteBuffer getBlob(String columnName) {
    return getBlobAsByteBuffer(columnName);
  }

  /**
   * Returns the BLOB value of the i-th column as a Java ByteBuffer type.
   *
   * @param i the position of the column
   * @return the BLOB value of the specified column as a Java ByteBuffer type. If the value is NULL,
   *     null
   */
  @Nullable
  default ByteBuffer getBlob(int i) {
    return getBlobAsByteBuffer(i);
  }

  /**
   * Returns the BLOB value of the specified column as a Java ByteBuffer type.
   *
   * @param columnName a column name of the value
   * @return the BLOB value of the specified column as a Java ByteBuffer type. If the value is NULL,
   *     null
   */
  @Nullable
  ByteBuffer getBlobAsByteBuffer(String columnName);

  /**
   * Returns the BLOB value of the i-th column as a Java ByteBuffer type.
   *
   * @param i the position of the column
   * @return the BLOB value of the specified column as a Java ByteBuffer type. If the value is NULL,
   *     null
   */
  @Nullable
  ByteBuffer getBlobAsByteBuffer(int i);

  /**
   * Returns the BLOB value of the specified column as a Java byte array type.
   *
   * @param columnName a column name of the value
   * @return the BLOB value of the specified column as a Java byte array type. If the value is NULL,
   *     null
   */
  @Nullable
  byte[] getBlobAsBytes(String columnName);

  /**
   * Returns the BLOB value of the i-th column as a Java byte array type.
   *
   * @param i the position of the column
   * @return the BLOB value of the specified column as a Java byte array type. If the value is NULL,
   *     null
   */
  @Nullable
  byte[] getBlobAsBytes(int i);

  /**
   * Returns the value of the specified column as a Java Object type.
   *
   * <p>If the columns is a BOOLEAN type, it returns a {@code Boolean} object. If the columns is an
   * INT type, it returns an {@code Integer} object. If the columns is a BIGINT type, it returns a
   * {@code LONG} object. If the columns is a FLOAT type, it returns a {@code FLOAT} object. If the
   * columns is a DOUBLE type, it returns a {@code DOUBLE} object. If the columns is a TEXT type, it
   * returns a {@code String} object. If the columns is a BLOB type, it returns a {@code ByteBuffer}
   * object.
   *
   * @param columnName a column name of the value
   * @return the value of the specified column as a Java Object type. If the value is NULL, null
   */
  @Nullable
  Object getAsObject(String columnName);

  /**
   * Returns the value of the i-th column as a Java Object type.
   *
   * <p>If the columns is a BOOLEAN type, it returns a {@code Boolean} object. If the columns is an
   * INT type, it returns an {@code Integer} object. If the columns is a BIGINT type, it returns a
   * {@code LONG} object. If the columns is a FLOAT type, it returns a {@code FLOAT} object. If the
   * columns is a DOUBLE type, it returns a {@code DOUBLE} object. If the columns is a TEXT type, it
   * returns a {@code String} object. If the columns is a BLOB type, it returns a {@code ByteBuffer}
   * object.
   *
   * @param i the position of the column
   * @return the value of the specified column as a Java Object type. If the value is NULL, null
   */
  @Nullable
  Object getAsObject(int i);

  /**
   * Indicates whether it contains the specified column.
   *
   * @param columnName a column name of the value
   * @return whether the result contains the specified column name
   */
  boolean contains(String columnName);

  /**
   * Returns a set of the contained column names.
   *
   * @return a set of the contained column names
   */
  Set<String> getContainedColumnNames();

  /**
   * Returns a number of the columns contained in this record.
   *
   * @return a number of the columns contained in this record
   */
  int size();
}
