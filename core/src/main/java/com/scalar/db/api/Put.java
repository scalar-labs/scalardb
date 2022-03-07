package com.scalar.db.api;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A command to put an entry to {@link DistributedStorage}.
 *
 * @author Hiroyuki Yamada
 */
@NotThreadSafe
public class Put extends Mutation {
  private final Map<String, Optional<Value<?>>> values;

  /**
   * Constructs a {@code Put} with the specified partition {@link Key}.
   *
   * @param partitionKey a partition {@code Key} (it might be composed of multiple values)
   */
  public Put(Key partitionKey) {
    this(partitionKey, null);
  }

  /**
   * Constructs a {@code Put} with the specified partition {@link Key} and the clustering {@link
   * Key}.
   *
   * @param partitionKey a partition {@code Key} (it might be composed of multiple values)
   * @param clusteringKey a clustering {@code Key} (it might be composed of multiple values)
   */
  public Put(Key partitionKey, Key clusteringKey) {
    super(partitionKey, clusteringKey);
    values = new LinkedHashMap<>();
  }

  public Put(Put put) {
    super(put);
    values = new LinkedHashMap<>(put.values);
  }

  /**
   * Adds the specified {@link Value} to the list of put values.
   *
   * @param value a {@code Value} to put
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  public Put withValue(Value<?> value) {
    values.put(value.getName(), Optional.of(value));
    return this;
  }

  /**
   * Adds the specified BOOLEAN value to the list of put values.
   *
   * @param columnName a column name of the value
   * @param booleanValue a BOOLEAN value to put
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link
   *     #withBooleanValue(String, boolean)} instead
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
   */
  public Put withBooleanValue(String columnName, boolean value) {
    values.put(columnName, Optional.of(new BooleanValue(columnName, value)));
    return this;
  }

  /**
   * Adds the specified INT value to the list of put values.
   *
   * @param columnName a column name of the value
   * @param intValue a INT value to put
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link
   *     #withIntValue(String, int)} instead
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
   */
  public Put withIntValue(String columnName, int value) {
    values.put(columnName, Optional.of(new IntValue(columnName, value)));
    return this;
  }

  /**
   * Adds the specified BIGINT value to the list of put values.
   *
   * @param columnName a column name of the value
   * @param bigIntValue a BIGINT value to put
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link
   *     #withBigIntValue(String, long)} instead
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
   */
  public Put withBigIntValue(String columnName, long value) {
    values.put(columnName, Optional.of(new BigIntValue(columnName, value)));
    return this;
  }

  /**
   * Adds the specified FLOAT value to the list of put values.
   *
   * @param columnName a column name of the value
   * @param floatValue a value to put
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link
   *     #withFloatValue(String, float)} instead
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
   * @param value a value to put
   * @return this object
   */
  public Put withFloatValue(String columnName, float value) {
    values.put(columnName, Optional.of(new FloatValue(columnName, value)));
    return this;
  }

  /**
   * Adds the specified DOUBLE value to the list of put values.
   *
   * @param columnName a column name of the value
   * @param doubleValue a DOUBLE value to put
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link
   *     #withDoubleValue(String, double)} instead
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
   */
  public Put withDoubleValue(String columnName, double value) {
    values.put(columnName, Optional.of(new DoubleValue(columnName, value)));
    return this;
  }

  /**
   * Adds the specified TEXT value to the list of put values.
   *
   * @param columnName a column name of the value
   * @param textValue a TEXT value to put
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link
   *     #withTextValue(String, String)} instead
   */
  @Deprecated
  public Put withValue(String columnName, @Nullable String textValue) {
    if (textValue == null) {
      return withNullValue(columnName);
    }
    return withTextValue(columnName, textValue);
  }

  /**
   * Adds the specified TEXT value to the list of put values.
   *
   * @param columnName a column name of the value
   * @param value a TEXT value to put
   * @return this object
   */
  public Put withTextValue(String columnName, String value) {
    values.put(columnName, Optional.of(new TextValue(columnName, Objects.requireNonNull(value))));
    return this;
  }

  /**
   * Adds the specified BLOB value as a byte array to the list of put values.
   *
   * @param columnName a column name of the value
   * @param blobValue a BLOB value to put
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link
   *     #withBlobValue(String, byte[])} instead
   */
  @Deprecated
  public Put withValue(String columnName, @Nullable byte[] blobValue) {
    if (blobValue == null) {
      return withNullValue(columnName);
    }
    return withBlobValue(columnName, blobValue);
  }

  /**
   * Adds the specified BLOB value as a byte array to the list of put values.
   *
   * @param columnName a column name of the value
   * @param value a BLOB value to put
   * @return this object
   */
  public Put withBlobValue(String columnName, byte[] value) {
    values.put(columnName, Optional.of(new BlobValue(columnName, Objects.requireNonNull(value))));
    return this;
  }

  /**
   * Adds the specified Blob value as a ByteBuffer to the list of put values.
   *
   * @param columnName a column name of the value
   * @param blobValue a BLOB value to put
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link
   *     #withBlobValue(String, ByteBuffer)} instead
   */
  @Deprecated
  public Put withValue(String columnName, @Nullable ByteBuffer blobValue) {
    if (blobValue == null) {
      return withNullValue(columnName);
    }
    return withBlobValue(columnName, blobValue);
  }

  /**
   * Adds the specified Blob value as a ByteBuffer to the list of put values.
   *
   * @param columnName a column name of the value
   * @param value a BLOB value to put
   * @return this object
   */
  public Put withBlobValue(String columnName, ByteBuffer value) {
    values.put(columnName, Optional.of(new BlobValue(columnName, Objects.requireNonNull(value))));
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
    values.forEach(v -> this.values.put(v.getName(), Optional.of(v)));
    return this;
  }

  /**
   * Adds NULL value to the list of put values.
   *
   * @param columnName a column name of the value
   * @return this object
   */
  public Put withNullValue(String columnName) {
    values.put(columnName, Optional.empty());
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
    values.forEach((k, v) -> ret.put(k, v.orElse(null)));
    return ret;
  }

  /**
   * Returns a map of {@link Value}s.
   *
   * <p>This method is primarily for internal use. Breaking changes can and will be introduced to
   * this method. Users should not depend on it.
   *
   * @return a map of {@code Value}s
   */
  public Map<String, Optional<Value<?>>> getNullableValues() {
    return ImmutableMap.copyOf(values);
  }

  /**
   * Indicates whether the value of the specified column added to the list of put values is NULL.
   *
   * @param columnName a column name of the value
   * @return whether the value of the specified column is NULL
   */
  public boolean isNullValue(String columnName) {
    checkIfExists(columnName);

    Optional<Value<?>> value = values.get(columnName);
    if (value.isPresent()) {
      if (value.get() instanceof TextValue) {
        return !value.get().getAsString().isPresent();
      } else if (value.get() instanceof BlobValue) {
        return !value.get().getAsBytes().isPresent();
      }
    }

    return !value.isPresent();
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

    if (isNullValue(columnName)) {
      // default value
      return false;
    }
    assert values.get(columnName).isPresent();
    return values.get(columnName).get().getAsBoolean();
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

    if (isNullValue(columnName)) {
      // default value
      return 0;
    }
    assert values.get(columnName).isPresent();
    return values.get(columnName).get().getAsInt();
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

    if (isNullValue(columnName)) {
      // default value
      return 0L;
    }
    assert values.get(columnName).isPresent();
    return values.get(columnName).get().getAsLong();
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

    if (isNullValue(columnName)) {
      // default value
      return 0.0F;
    }
    assert values.get(columnName).isPresent();
    return values.get(columnName).get().getAsFloat();
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

    if (isNullValue(columnName)) {
      // default value
      return 0.0D;
    }
    assert values.get(columnName).isPresent();
    return values.get(columnName).get().getAsDouble();
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

    if (isNullValue(columnName)) {
      // default value
      return null;
    }
    assert values.get(columnName).isPresent();
    return values.get(columnName).get().getAsString().orElse(null);
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

    if (isNullValue(columnName)) {
      // default value
      return null;
    }
    assert values.get(columnName).isPresent();
    return values.get(columnName).get().getAsByteBuffer().orElse(null);
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

    if (isNullValue(columnName)) {
      // default value
      return null;
    }
    assert values.get(columnName).isPresent();
    return values.get(columnName).get().getAsBytes().orElse(null);
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
    if (isNullValue(columnName)) {
      return null;
    }

    Optional<Value<?>> value = values.get(columnName);
    assert value.isPresent();
    if (value.get() instanceof BooleanValue) {
      return getBooleanValue(columnName);
    } else if (value.get() instanceof IntValue) {
      return getIntValue(columnName);
    } else if (value.get() instanceof BigIntValue) {
      return getBigIntValue(columnName);
    } else if (value.get() instanceof FloatValue) {
      return getFloatValue(columnName);
    } else if (value.get() instanceof DoubleValue) {
      return getDoubleValue(columnName);
    } else if (value.get() instanceof TextValue) {
      return getTextValue(columnName);
    } else if (value.get() instanceof BlobValue) {
      return getBlobValue(columnName);
    } else {
      throw new AssertionError();
    }
  }

  /**
   * Indicates whether the list of put values contains the specified column.
   *
   * @param columnName a column name of the value
   * @return whether the result contains the specified column name
   */
  public boolean containsColumn(String columnName) {
    return values.containsKey(columnName);
  }

  /**
   * Returns a set of the contained column names for the values added to the list of put values.
   *
   * @return a set of the contained column names
   */
  public Set<String> getContainedColumnNames() {
    return ImmutableSet.copyOf(values.keySet());
  }

  private void checkIfExists(String name) {
    if (!containsColumn(name)) {
      throw new IllegalArgumentException(name + " doesn't exist");
    }
  }

  @Override
  public Put forNamespace(String namespace) {
    return (Put) super.forNamespace(namespace);
  }

  @Override
  public Put forTable(String tableName) {
    return (Put) super.forTable(tableName);
  }

  @Override
  public Put withConsistency(Consistency consistency) {
    return (Put) super.withConsistency(consistency);
  }

  @Override
  public void accept(OperationVisitor v) {
    v.visit(this);
  }

  @Override
  public Put withCondition(MutationCondition condition) {
    return (Put) super.withCondition(condition);
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
    return values.equals(other.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), values);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("namespace", forNamespace())
        .add("table", forTable())
        .add("partitionKey", getPartitionKey())
        .add("clusteringKey", getClusteringKey())
        .add("values", getNullableValues())
        .add("consistency", getConsistency())
        .add("condition", getCondition())
        .toString();
  }
}
