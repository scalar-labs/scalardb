package com.scalar.db.api;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.PutBuilder.BuildableFromExisting;
import com.scalar.db.api.PutBuilder.Namespace;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.Value;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitOperationAttribute;
import com.scalar.db.util.ScalarDbUtils;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A command to put an entry in the underlying storage.
 *
 * @author Hiroyuki Yamada
 */
@NotThreadSafe
public class Put extends Mutation {

  private final Map<String, Column<?>> columns;

  Put(
      @Nullable String namespace,
      String tableName,
      Key partitionKey,
      @Nullable Key clusteringKey,
      @Nullable Consistency consistency,
      ImmutableMap<String, String> attributes,
      @Nullable MutationCondition condition,
      Map<String, Column<?>> columns) {
    super(namespace, tableName, partitionKey, clusteringKey, consistency, attributes, condition);
    this.columns = columns;
  }

  /**
   * Constructs a {@code Put} with the specified partition {@link Key}.
   *
   * @param partitionKey a partition {@code Key} (it might be composed of multiple values)
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link Put#newBuilder()}
   *     instead
   */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  public Put(Key partitionKey) {
    this(partitionKey, null);
  }

  /**
   * Constructs a {@code Put} with the specified partition {@link Key} and the clustering {@link
   * Key}.
   *
   * @param partitionKey a partition {@code Key} (it might be composed of multiple values)
   * @param clusteringKey a clustering {@code Key} (it might be composed of multiple values)
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link Put#newBuilder()}
   *     instead
   */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  public Put(Key partitionKey, Key clusteringKey) {
    super(partitionKey, clusteringKey);
    columns = new LinkedHashMap<>();
  }

  /**
   * Copy a Put.
   *
   * @param put a Put
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link
   *     Put#newBuilder(Put)} instead
   */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  public Put(Put put) {
    super(put);
    columns = new LinkedHashMap<>(put.columns);
  }

  /**
   * Build a {@code Put} operation using a builder.
   *
   * @return a {@code Put} operation builder
   */
  public static Namespace newBuilder() {
    return new Namespace();
  }

  /**
   * Build a {@code Put} operation from an existing {@code Put} object using a builder. The builder
   * will be parametrized by default with all the existing {@code Put} parameters.
   *
   * @param put an existing {@code Put} operation
   * @return a {@code Put} operation builder
   */
  public static BuildableFromExisting newBuilder(Put put) {
    checkNotNull(put);
    return new BuildableFromExisting(put);
  }

  /**
   * Adds the specified {@link Value} to the list of put values.
   *
   * @param value a {@code Value} to put
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Put builder instead; to create a Put builder, use {@link Put#newBuilder()}
   */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  public Put withValue(Value<?> value) {
    return withValue(ScalarDbUtils.toColumn(value));
  }

  /**
   * Adds the specified BOOLEAN value to the list of put values.
   *
   * @param columnName a column name of the value
   * @param booleanValue a BOOLEAN value to put
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Put builder instead; to create a Put builder, use {@link Put#newBuilder()}
   */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  public Put withValue(String columnName, boolean booleanValue) {
    return withBooleanValue(columnName, booleanValue);
  }

  /**
   * Adds the specified BOOLEAN value to the list of put values.
   *
   * @param columnName a column name of the value
   * @param value a BOOLEAN value to put
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Put builder instead; to create a Put builder, use {@link Put#newBuilder()}
   */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  public Put withBooleanValue(String columnName, boolean value) {
    columns.put(columnName, BooleanColumn.of(columnName, value));
    return this;
  }

  /**
   * Adds the specified BOOLEAN value to the list of put values.
   *
   * @param columnName a column name of the value
   * @param value a BOOLEAN value to put
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Put builder instead; to create a Put builder, use {@link Put#newBuilder()}
   */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  public Put withBooleanValue(String columnName, @Nullable Boolean value) {
    if (value != null) {
      return withBooleanValue(columnName, value.booleanValue());
    }
    columns.put(columnName, BooleanColumn.ofNull(columnName));
    return this;
  }

  /**
   * Adds the specified INT value to the list of put values.
   *
   * @param columnName a column name of the value
   * @param intValue a INT value to put
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Put builder instead; to create a Put builder, use {@link Put#newBuilder()}
   */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  public Put withValue(String columnName, int intValue) {
    return withIntValue(columnName, intValue);
  }

  /**
   * Adds the specified INT value to the list of put values.
   *
   * @param columnName a column name of the value
   * @param value a INT value to put
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Put builder instead; to create a Put builder, use {@link Put#newBuilder()}
   */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  public Put withIntValue(String columnName, int value) {
    columns.put(columnName, IntColumn.of(columnName, value));
    return this;
  }

  /**
   * Adds the specified INT value to the list of put values.
   *
   * @param columnName a column name of the value
   * @param value a INT value to put
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Put builder instead; to create a Put builder, use {@link Put#newBuilder()}
   */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  public Put withIntValue(String columnName, @Nullable Integer value) {
    if (value != null) {
      return withIntValue(columnName, value.intValue());
    }
    columns.put(columnName, IntColumn.ofNull(columnName));
    return this;
  }

  /**
   * Adds the specified BIGINT value to the list of put values.
   *
   * @param columnName a column name of the value
   * @param bigIntValue a BIGINT value to put
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Put builder instead; to create a Put builder, use {@link Put#newBuilder()}
   */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  public Put withValue(String columnName, long bigIntValue) {
    return withBigIntValue(columnName, bigIntValue);
  }

  /**
   * Adds the specified BIGINT value to the list of put values.
   *
   * @param columnName a column name of the value
   * @param value a BIGINT value to put
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Put builder instead; to create a Put builder, use {@link Put#newBuilder()}
   */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  public Put withBigIntValue(String columnName, long value) {
    columns.put(columnName, BigIntColumn.of(columnName, value));
    return this;
  }

  /**
   * Adds the specified BIGINT value to the list of put values.
   *
   * @param columnName a column name of the value
   * @param value a BIGINT value to put
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Put builder instead; to create a Put builder, use {@link Put#newBuilder()}
   */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  public Put withBigIntValue(String columnName, @Nullable Long value) {
    if (value != null) {
      return withBigIntValue(columnName, value.longValue());
    }
    columns.put(columnName, BigIntColumn.ofNull(columnName));
    return this;
  }

  /**
   * Adds the specified FLOAT value to the list of put values.
   *
   * @param columnName a column name of the value
   * @param floatValue a value to put
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Put builder instead; to create a Put builder, use {@link Put#newBuilder()}
   */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  public Put withValue(String columnName, float floatValue) {
    return withFloatValue(columnName, floatValue);
  }

  /**
   * Adds the specified FLOAT value to the list of put values.
   *
   * @param columnName a column name of the value
   * @param value a FLOAT value to put
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Put builder instead; to create a Put builder, use {@link Put#newBuilder()}
   */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  public Put withFloatValue(String columnName, float value) {
    columns.put(columnName, FloatColumn.of(columnName, value));
    return this;
  }

  /**
   * Adds the specified FLOAT value to the list of put values.
   *
   * @param columnName a column name of the value
   * @param value a FLOAT value to put
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Put builder instead; to create a Put builder, use {@link Put#newBuilder()}
   */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  public Put withFloatValue(String columnName, @Nullable Float value) {
    if (value != null) {
      return withFloatValue(columnName, value.floatValue());
    }
    columns.put(columnName, FloatColumn.ofNull(columnName));
    return this;
  }

  /**
   * Adds the specified DOUBLE value to the list of put values.
   *
   * @param columnName a column name of the value
   * @param doubleValue a DOUBLE value to put
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Put builder instead; to create a Put builder, use {@link Put#newBuilder()}
   */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  public Put withValue(String columnName, double doubleValue) {
    return withDoubleValue(columnName, doubleValue);
  }

  /**
   * Adds the specified DOUBLE value to the list of put values.
   *
   * @param columnName a column name of the value
   * @param value a DOUBLE value to put
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Put builder instead; to create a Put builder, use {@link Put#newBuilder()}
   */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  public Put withDoubleValue(String columnName, double value) {
    columns.put(columnName, DoubleColumn.of(columnName, value));
    return this;
  }

  /**
   * Adds the specified DOUBLE value to the list of put values.
   *
   * @param columnName a column name of the value
   * @param value a DOUBLE value to put
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Put builder instead; to create a Put builder, use {@link Put#newBuilder()}
   */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  public Put withDoubleValue(String columnName, @Nullable Double value) {
    if (value != null) {
      return withDoubleValue(columnName, value.doubleValue());
    }
    columns.put(columnName, DoubleColumn.ofNull(columnName));
    return this;
  }

  /**
   * Adds the specified TEXT value to the list of put values.
   *
   * @param columnName a column name of the value
   * @param textValue a TEXT value to put
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Put builder instead; to create a Put builder, use {@link Put#newBuilder()}
   */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  public Put withValue(String columnName, @Nullable String textValue) {
    return withTextValue(columnName, textValue);
  }

  /**
   * Adds the specified TEXT value to the list of put values.
   *
   * @param columnName a column name of the value
   * @param value a TEXT value to put
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Put builder instead; to create a Put builder, use {@link Put#newBuilder()}
   */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  public Put withTextValue(String columnName, @Nullable String value) {
    columns.put(columnName, TextColumn.of(columnName, value));
    return this;
  }

  /**
   * Adds the specified BLOB value as a byte array to the list of put values.
   *
   * @param columnName a column name of the value
   * @param blobValue a BLOB value to put
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Put builder instead; to create a Put builder, use {@link Put#newBuilder()}
   */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  public Put withValue(String columnName, @Nullable byte[] blobValue) {
    return withBlobValue(columnName, blobValue);
  }

  /**
   * Adds the specified BLOB value as a byte array to the list of put values.
   *
   * @param columnName a column name of the value
   * @param value a BLOB value to put
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Put builder instead; to create a Put builder, use {@link Put#newBuilder()}
   */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  public Put withBlobValue(String columnName, @Nullable byte[] value) {
    columns.put(columnName, BlobColumn.of(columnName, value));
    return this;
  }

  /**
   * Adds the specified Blob value as a ByteBuffer to the list of put values.
   *
   * @param columnName a column name of the value
   * @param blobValue a BLOB value to put
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Put builder instead; to create a Put builder, use {@link Put#newBuilder()}
   */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  public Put withValue(String columnName, @Nullable ByteBuffer blobValue) {
    return withBlobValue(columnName, blobValue);
  }

  /**
   * Adds the specified Blob value as a ByteBuffer to the list of put values.
   *
   * @param columnName a column name of the value
   * @param value a BLOB value to put
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Put builder instead; to create a Put builder, use {@link Put#newBuilder()}
   */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  public Put withBlobValue(String columnName, @Nullable ByteBuffer value) {
    columns.put(columnName, BlobColumn.of(columnName, value));
    return this;
  }

  /**
   * Adds the specified collection of {@link Value}s to the list of put values.
   *
   * @param values a collection of {@code Value}s to put
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  public Put withValues(Collection<Value<?>> values) {
    values.forEach(v -> this.columns.put(v.getName(), ScalarDbUtils.toColumn(v)));
    return this;
  }

  /**
   * Adds a column to the list of put values.
   *
   * <p>This method is primarily for internal use. Breaking changes can and will be introduced to
   * this method. Users should not depend on it.
   *
   * @param column a column to put
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Put builder instead; to create a Put builder, use {@link Put#newBuilder()}
   */
  @Deprecated
  public Put withValue(Column<?> column) {
    columns.put(column.getName(), column);
    return this;
  }

  /**
   * Returns a map of {@link Value}s.
   *
   * @return a map of {@code Value}s
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  public Map<String, Value<?>> getValues() {
    Map<String, Value<?>> ret = new HashMap<>();
    columns.forEach(
        (k, c) -> {
          Value<?> value;
          if (c.hasNullValue()
              && c.getDataType() != DataType.TEXT
              && c.getDataType() != DataType.BLOB) {
            value = null;
          } else {
            value = ScalarDbUtils.toValue(c);
          }
          ret.put(k, value);
        });
    return ret;
  }

  /**
   * Returns a map of {@link Column}s.
   *
   * <p>This method is primarily for internal use. Breaking changes can and will be introduced to
   * this method. Users should not depend on it.
   *
   * @return a map of {@code Column}s
   */
  public Map<String, Column<?>> getColumns() {
    return ImmutableMap.copyOf(columns);
  }

  /**
   * Indicates whether the value of the specified column added to the list of put values is NULL.
   *
   * @param columnName a column name of the value
   * @return whether the value of the specified column is NULL
   */
  public boolean isNullValue(String columnName) {
    checkIfExists(columnName);
    return columns.get(columnName).hasNullValue();
  }

  /**
   * Returns the BOOLEAN value of the specified column added to the list of put values.
   *
   * <p>Note that, due to its signature, this method cannot return null. If the value is NULL, it
   * will return 0. If this doesn't work for you, either call {@link #isNullValue(String)} before
   * calling this method, or use {@link #getValueAsObject(String)} instead.
   *
   * @param columnName a column name of the value
   * @return the BOOLEAN value of the specified column
   */
  public boolean getBooleanValue(String columnName) {
    checkIfExists(columnName);
    return columns.get(columnName).getBooleanValue();
  }

  /**
   * Returns the INT value of the specified column added to the list of put values.
   *
   * <p>Note that, due to its signature, this method cannot return null. If the value is NULL, it
   * will return 0. If this doesn't work for you, either call {@link #isNullValue(String)} before
   * calling this method, or use {@link #getValueAsObject(String)} instead.
   *
   * @param columnName a column name of the value
   * @return the INT value of the specified column
   */
  public int getIntValue(String columnName) {
    checkIfExists(columnName);
    return columns.get(columnName).getIntValue();
  }

  /**
   * Returns the BIGINT value of the specified column added to the list of put values.
   *
   * <p>Note that, due to its signature, this method cannot return null. If the value is NULL, it
   * will return 0. If this doesn't work for you, either call {@link #isNullValue(String)} before
   * calling this method, or use {@link #getValueAsObject(String)} instead.
   *
   * @param columnName a column name of the value
   * @return the BIGINT value of the specified column
   */
  public long getBigIntValue(String columnName) {
    checkIfExists(columnName);
    return columns.get(columnName).getBigIntValue();
  }

  /**
   * Returns the FLOAT value of the specified column added to the list of put values.
   *
   * <p>Note that, due to its signature, this method cannot return null. If the value is NULL, it
   * will return 0.0. If this doesn't work for you, either call {@link #isNullValue(String)} before
   * calling this method, or use {@link #getValueAsObject(String)} instead.
   *
   * @param columnName a column name of the value
   * @return the FLOAT value of the specified column
   */
  public float getFloatValue(String columnName) {
    checkIfExists(columnName);
    return columns.get(columnName).getFloatValue();
  }

  /**
   * Returns the DOUBLE value of the specified column added to the list of put values.
   *
   * <p>Note that, due to its signature, this method cannot return null. If the value is NULL, it
   * will return 0.0. If this doesn't work for you, either call {@link #isNullValue(String)} before
   * calling this method, or use {@link #getValueAsObject(String)} instead.
   *
   * @param columnName a column name of the value
   * @return the DOUBLE value of the specified column
   */
  public double getDoubleValue(String columnName) {
    checkIfExists(columnName);
    return columns.get(columnName).getDoubleValue();
  }

  /**
   * Returns the TEXT value of the specified column added to the list of put values.
   *
   * @param columnName a column name of the value
   * @return the TEXT value of the specified column. If the value is NULL, null
   */
  @Nullable
  public String getTextValue(String columnName) {
    checkIfExists(columnName);
    return columns.get(columnName).getTextValue();
  }

  /**
   * Returns the BLOB value of the specified column added to the list of put values as a ByteBuffer
   * type.
   *
   * @param columnName a column name of the value
   * @return the BLOB value of the specified column. If the value is NULL, null
   */
  @Nullable
  public ByteBuffer getBlobValue(String columnName) {
    return getBlobValueAsByteBuffer(columnName);
  }

  /**
   * Returns the BLOB value of the specified column added to the list of put values as a ByteBuffer
   * type.
   *
   * @param columnName a column name of the value
   * @return the BLOB value of the specified column. If the value is NULL, null
   */
  @Nullable
  public ByteBuffer getBlobValueAsByteBuffer(String columnName) {
    checkIfExists(columnName);
    return columns.get(columnName).getBlobValueAsByteBuffer();
  }

  /**
   * Returns the BLOB value of the specified column added to the list of put values as a byte array
   * type.
   *
   * @param columnName a column name of the value
   * @return the BLOB value of the specified column. If the value is NULL, null
   */
  @Nullable
  public byte[] getBlobValueAsBytes(String columnName) {
    checkIfExists(columnName);
    return columns.get(columnName).getBlobValueAsBytes();
  }

  /**
   * Returns the value of the specified column added to the list of put values as an Object type.
   *
   * <p>If the columns is a BOOLEAN type, it returns a {@code Boolean} object. If the columns is an
   * INT type, it returns an {@code Integer} object. If the columns is a BIGINT type, it returns a
   * {@code LONG} object. If the columns is a FLOAT type, it returns a {@code FLOAT} object. If the
   * columns is a DOUBLE type, it returns a {@code DOUBLE} object. If the columns is a TEXT type, it
   * returns a {@code String} object. If the columns is a BLOB type, it returns a {@code ByteBuffer}
   * object.
   *
   * @param columnName a column name of the value
   * @return the value of the specified column. If the value is NULL, null
   */
  @Nullable
  public Object getValueAsObject(String columnName) {
    checkIfExists(columnName);
    return columns.get(columnName).getValueAsObject();
  }

  /**
   * Indicates whether the list of put values contains the specified column.
   *
   * @param columnName a column name of the value
   * @return whether the result contains the specified column name
   */
  public boolean containsColumn(String columnName) {
    return columns.containsKey(columnName);
  }

  /**
   * Returns a set of the contained column names for the values added to the list of put values.
   *
   * @return a set of the contained column names
   */
  public Set<String> getContainedColumnNames() {
    return ImmutableSet.copyOf(columns.keySet());
  }

  private void checkIfExists(String name) {
    if (!containsColumn(name)) {
      throw new IllegalArgumentException(CoreError.COLUMN_NOT_FOUND.buildMessage(name));
    }
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Put builder instead; to create a Put builder, use {@link Put#newBuilder()}
   */
  @Deprecated
  @Override
  public Put forNamespace(String namespace) {
    return (Put) super.forNamespace(namespace);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Put builder instead; to create a Put builder, use {@link Put#newBuilder()}
   */
  @Deprecated
  @Override
  public Put forTable(String tableName) {
    return (Put) super.forTable(tableName);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Put builder instead; to create a Put builder, use {@link Put#newBuilder()}
   */
  @Deprecated
  @Override
  public Put withConsistency(Consistency consistency) {
    return (Put) super.withConsistency(consistency);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Put builder instead; to create a Put builder, use {@link Put#newBuilder()}
   */
  @Override
  @Deprecated
  public Put withCondition(MutationCondition condition) {
    return (Put) super.withCondition(condition);
  }

  @Nonnull
  @Override
  public Optional<MutationCondition> getCondition() {
    return super.getCondition();
  }

  /**
   * Returns whether implicit pre-read is enabled for this Put. This is a utility method for
   * Consensus Commit.
   *
   * @return whether implicit pre-read is enabled for this Put
   * @deprecated As of release 3.15.0. Will be removed in release 5.0.0. Use {@link
   *     ConsensusCommitOperationAttribute#isImplicitPreReadEnabled(Put)} instead
   */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  public boolean isImplicitPreReadEnabled() {
    return ConsensusCommitOperationAttribute.isImplicitPreReadEnabled(this);
  }

  /**
   * Returns whether the insert mode is enabled for this Put. This is a utility method for Consensus
   * Commit.
   *
   * @return whether the insert mode is enabled for this Put
   * @deprecated As of release 3.15.0. Will be removed in release 5.0.0. Use {@link
   *     ConsensusCommitOperationAttribute#isInsertModeEnabled(Put)} instead
   */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  public boolean isInsertModeEnabled() {
    return ConsensusCommitOperationAttribute.isInsertModeEnabled(this);
  }

  @Override
  public void accept(OperationVisitor v) {
    v.visit(this);
  }

  /**
   * Indicates whether some other object is "equal to" this object. The other object is considered
   * equal if:
   *
   * <ul>
   *   <li>both super class instances are equal and
   *   <li>it is also an {@code Put} and
   *   <li>both instances have the same values
   * </ul>
   *
   * @param o an object to be tested for equality
   * @return {@code true} if the other object is "equal to" this object otherwise {@code false}
   */
  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    if (o == this) {
      return true;
    }
    if (!(o instanceof Put)) {
      return false;
    }
    Put other = (Put) o;
    return columns.equals(other.columns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), columns);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("namespace", forNamespace())
        .add("table", forTable())
        .add("partitionKey", getPartitionKey())
        .add("clusteringKey", getClusteringKey())
        .add("consistency", getConsistency())
        .add("attributes", getAttributes())
        .add("condition", getCondition())
        .add("columns", getColumns())
        .toString();
  }
}
