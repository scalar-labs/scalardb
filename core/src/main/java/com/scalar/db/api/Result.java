package com.scalar.db.api;

import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * A result retrieved from {@link DistributedStorage}.
 *
 * @author Hiroyuki Yamada
 */
public interface Result {

  /**
   * Returns the partition {@link Key}
   *
   * @return an {@code Optional} with the partition {@code Key}
   */
  Optional<Key> getPartitionKey();

  /**
   * Returns the clustering {@link Key}
   *
   * @return an {@code Optional} with the clustering {@code Key}
   */
  Optional<Key> getClusteringKey();

  /**
   * Returns the {@link Value} which the specified name is mapped to
   *
   * <p>Note that if the value is NULL, it will return a {@code Value} object with a default value.
   *
   * @param name name of the {@code Value}
   * @return an {@code Optional} with the {@code Value}
   */
  Optional<Value<?>> getValue(String name);

  /**
   * Returns a map of {@link Value}s
   *
   * <p>Note that if the value in the map is NULL, the value will be a {@code Value} object with a
   * default value.
   *
   * @return a map of {@code Value}s
   */
  Map<String, Value<?>> getValues();

  /**
   * Indicates whether the value for the specified name is NULL.
   *
   * @param name a name
   * @return whether the value for the specified name is NULL
   */
  boolean isNull(String name);

  /**
   * Returns the BOOLEAN value for the specified name as a Java boolean type.
   *
   * <p>Note that, due to its signature, this method cannot return null. If the value is NULL, it
   * will return 0. If this doesn't work for you, either call {@link #isNull(String)} before calling
   * this method, or use {@link #getObject(String)} instead.
   *
   * @param name a name
   * @return the BOOLEAN value for the specified name as a Java boolean type
   */
  boolean getBoolean(String name);

  /**
   * Returns the INT value for the specified name as a Java int type.
   *
   * <p>Note that, due to its signature, this method cannot return null. If the value is NULL, it
   * will return 0. If this doesn't work for you, either call {@link #isNull(String)} before calling
   * this method, or use {@link #getObject(String)} instead.
   *
   * @param name a name
   * @return the INT value for the specified name as a Java int type
   */
  int getInt(String name);

  /**
   * Returns the BIGINT value for the specified name as a Java long type.
   *
   * <p>Note that, due to its signature, this method cannot return null. If the value is NULL, it
   * will return 0. If this doesn't work for you, either call {@link #isNull(String)} before calling
   * this method, or use {@link #getObject(String)} instead.
   *
   * @param name a name
   * @return the BIGINT value for the specified name as a Java long type
   */
  long getBigInt(String name);

  /**
   * Returns the FLOAT value for the specified name as a Java float type.
   *
   * <p>Note that, due to its signature, this method cannot return null. If the value is NULL, it
   * will return 0.0. If this doesn't work for you, either call {@link #isNull(String)} before
   * calling this method, or use {@link #getObject(String)} instead.
   *
   * @param name a name
   * @return the FLOAT value for the specified name as a Java float type
   */
  float getFloat(String name);

  /**
   * Returns the DOUBLE value for the specified name as a Java double type.
   *
   * <p>Note that, due to its signature, this method cannot return null. If the value is NULL, it
   * will return 0.0. If this doesn't work for you, either call {@link #isNull(String)} before
   * calling this method, or use {@link #getObject(String)} instead.
   *
   * @param name a name
   * @return the DOUBLE value for the specified name as a Java double type
   */
  double getDouble(String name);

  /**
   * Returns the TEXT value for the specified name as a Java String type.
   *
   * @param name a name
   * @return the TEXT value for the specified name as a Java String type. If the value is NULL, null
   */
  @Nullable
  String getText(String name);

  /**
   * Returns the BLOB value for the specified name as a Java ByteBuffer type.
   *
   * @param name a name
   * @return the BLOB value for the specified name as a Java ByteBuffer type. If the value is NULL,
   *     null
   */
  @Nullable
  default ByteBuffer getBlob(String name) {
    return getBlobAsByteBuffer(name);
  }

  /**
   * Returns the BLOB value for the specified name as a Java ByteBuffer type.
   *
   * @param name a name
   * @return the BLOB value for the specified name as a Java ByteBuffer type. If the value is NULL,
   *     null
   */
  @Nullable
  ByteBuffer getBlobAsByteBuffer(String name);

  /**
   * Returns the BLOB value for the specified name as a Java byte array type.
   *
   * @param name a name
   * @return the BLOB value for the specified name as a Java byte array type. If the value is NULL,
   *     null
   */
  @Nullable
  byte[] getBlobAsBytes(String name);

  /**
   * Returns the value for the specified name as a Java Object type.
   *
   * <p>If the columns is a BOOLEAN type, it returns a {@code Boolean} object. If the columns is an
   * INT type, it returns an {@code Integer} object. If the columns is a BIGINT type, it returns a
   * {@code LONG} object. If the columns is a FLOAT type, it returns a {@code FLOAT} object. If the
   * columns is a DOUBLE type, it returns a {@code DOUBLE} object. If the columns is a TEXT type, it
   * returns a {@code String} object. If the columns is a BLOB type, it returns a {@code ByteBuffer}
   * object.
   *
   * @param name a name
   * @return the value for the specified name as a Java Object type. If the value is NULL, null
   */
  @Nullable
  Object getObject(String name);

  /**
   * Indicates whether it contains the specified column
   *
   * @param name a name
   * @return whether the result contains the specified column name
   */
  boolean contains(String name);

  /**
   * Returns a set of the contained column names
   *
   * @return a set of the contained column names
   */
  Set<String> getContainedColumnNames();
}
