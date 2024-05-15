package com.scalar.db.api;

import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * A result retrieved from the underlying storage.
 *
 * @author Hiroyuki Yamada
 */
public interface Result {

  /**
   * Returns the partition {@link Key}
   *
   * @return an {@code Optional} with the partition {@code Key}
   * @deprecated As of release 3.8.0. Will be removed in release 5.0.0
   */
  @Deprecated
  Optional<Key> getPartitionKey();

  /**
   * Returns the clustering {@link Key}
   *
   * @return an {@code Optional} with the clustering {@code Key}
   * @deprecated As of release 3.8.0. Will be removed in release 5.0.0
   */
  @Deprecated
  Optional<Key> getClusteringKey();

  /**
   * Returns the {@link Value} which the specified column name is mapped to
   *
   * <p>Note that if the value is NULL, it will return a {@code Value} object with a default value.
   *
   * @param columnName a column name of the {@code Value}
   * @return an {@code Optional} with the {@code Value}
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  Optional<Value<?>> getValue(String columnName);

  /**
   * Returns a map of {@link Value}s
   *
   * <p>Note that if the value in the map is NULL, the value will be a {@code Value} object with a
   * default value.
   *
   * @return a map of {@code Value}s
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  Map<String, Value<?>> getValues();

  /**
   * Indicates whether the value of the specified column is NULL.
   *
   * @param columnName a column name of the value
   * @return whether the value of the specified column is NULL
   */
  boolean isNull(String columnName);

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
   * Returns the TEXT value of the specified column as a Java String type.
   *
   * @param columnName a column name of the value
   * @return the TEXT value of the specified column as a Java String type. If the value is NULL,
   *     null
   */
  @Nullable
  String getText(String columnName);

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
   * Returns the BLOB value of the specified column as a Java ByteBuffer type.
   *
   * @param columnName a column name of the value
   * @return the BLOB value of the specified column as a Java ByteBuffer type. If the value is NULL,
   *     null
   */
  @Nullable
  ByteBuffer getBlobAsByteBuffer(String columnName);

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
   * Returns a map of {@link Column}s in this result.
   *
   * <p>This method is primarily for internal use. Breaking changes can and will be introduced to
   * this method. Users should not depend on it.
   *
   * @return a map of {@code Column}s in this result
   */
  Map<String, Column<?>> getColumns();
}
