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
   */
  public Put withValue(Value<?> value) {
    values.put(value.getName(), Optional.of(value));
    return this;
  }

  /**
   * Adds the specified Boolean value to the list of put values.
   *
   * @param name a name of the value
   * @param value a value to put
   * @return this object
   */
  public Put withValue(String name, boolean value) {
    values.put(name, Optional.of(new BooleanValue(name, value)));
    return this;
  }

  /**
   * Adds the specified Int value to the list of put values.
   *
   * @param name a name of the value
   * @param value a value to put
   * @return this object
   */
  public Put withValue(String name, int value) {
    values.put(name, Optional.of(new IntValue(name, value)));
    return this;
  }

  /**
   * Adds the specified BigInt value to the list of put values.
   *
   * @param name a name of the value
   * @param value a value to put
   * @return this object
   */
  public Put withValue(String name, long value) {
    values.put(name, Optional.of(new BigIntValue(name, value)));
    return this;
  }

  /**
   * Adds the specified Float value to the list of put values.
   *
   * @param name a name of the value
   * @param value a value to put
   * @return this object
   */
  public Put withValue(String name, float value) {
    values.put(name, Optional.of(new FloatValue(name, value)));
    return this;
  }

  /**
   * Adds the specified Double value to the list of put values.
   *
   * @param name a name of the value
   * @param value a value to put
   * @return this object
   */
  public Put withValue(String name, double value) {
    values.put(name, Optional.of(new DoubleValue(name, value)));
    return this;
  }

  /**
   * Adds the specified Text value to the list of put values.
   *
   * @param name a name of the value
   * @param value a value to put
   * @return this object
   */
  public Put withValue(String name, @Nullable String value) {
    if (value == null) {
      withNullValue(name);
    } else {
      values.put(name, Optional.of(new TextValue(name, value)));
    }
    return this;
  }

  /**
   * Adds the specified Blob value to the list of put values.
   *
   * @param name a name of the value
   * @param value a value to put
   * @return this object
   */
  public Put withValue(String name, @Nullable byte[] value) {
    if (value == null) {
      withNullValue(name);
    } else {
      values.put(name, Optional.of(new BlobValue(name, value)));
    }
    return this;
  }

  /**
   * Adds the specified Blob value to the list of put values.
   *
   * @param name a name of the value
   * @param value a value to put
   * @return this object
   */
  public Put withValue(String name, @Nullable ByteBuffer value) {
    if (value == null) {
      withNullValue(name);
    } else {
      values.put(name, Optional.of(new BlobValue(name, value)));
    }
    return this;
  }

  /**
   * Adds the specified collection of {@link Value}s to the list of put values.
   *
   * @param values a collection of {@code Value}s to put
   * @return this object
   */
  public Put withValues(Collection<Value<?>> values) {
    values.forEach(v -> this.values.put(v.getName(), Optional.of(v)));
    return this;
  }

  /**
   * Adds NULL value for the specified name of the value.
   *
   * @param name a name of the value
   * @return this object
   */
  public Put withNullValue(String name) {
    values.put(name, Optional.empty());
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
   * @return a map of {@code Value}s
   */
  public Map<String, Optional<Value<?>>> getNullableValues() {
    return ImmutableMap.copyOf(values);
  }

  /**
   * Indicates whether the value for the specified name added to the list of put values is NULL.
   *
   * @param name a name
   * @return whether the value for the specified name is NULL
   */
  public boolean isNullValue(String name) {
    checkIfExists(name);

    Optional<Value<?>> value = values.get(name);
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
   * Returns the BOOLEAN value for the specified name added to the list of put values.
   *
   * <p>Note that, due to its signature, this method cannot return null. If the value is NULL, it
   * will return 0. If this doesn't work for you, either call {@link #isNullValue(String)} before
   * calling this method, or use {@link #getValueAsObject(String)} instead.
   *
   * @param name a name
   * @return the BOOLEAN value for the specified name
   */
  public boolean getBooleanValue(String name) {
    checkIfExists(name);

    if (isNullValue(name)) {
      // default value
      return false;
    }
    assert values.get(name).isPresent();
    return values.get(name).get().getAsBoolean();
  }

  /**
   * Returns the INT value for the specified name added to the list of put values.
   *
   * <p>Note that, due to its signature, this method cannot return null. If the value is NULL, it
   * will return 0. If this doesn't work for you, either call {@link #isNullValue(String)} before
   * calling this method, or use {@link #getValueAsObject(String)} instead.
   *
   * @param name a name
   * @return the INT value for the specified name
   */
  public int getIntValue(String name) {
    checkIfExists(name);

    if (isNullValue(name)) {
      // default value
      return 0;
    }
    assert values.get(name).isPresent();
    return values.get(name).get().getAsInt();
  }

  /**
   * Returns the BIGINT value for the specified name added to the list of put values.
   *
   * <p>Note that, due to its signature, this method cannot return null. If the value is NULL, it
   * will return 0. If this doesn't work for you, either call {@link #isNullValue(String)} before
   * calling this method, or use {@link #getValueAsObject(String)} instead.
   *
   * @param name a name
   * @return the BIGINT value for the specified name
   */
  public long getBigIntValue(String name) {
    checkIfExists(name);

    if (isNullValue(name)) {
      // default value
      return 0L;
    }
    assert values.get(name).isPresent();
    return values.get(name).get().getAsLong();
  }

  /**
   * Returns the FLOAT value for the specified name added to the list of put values.
   *
   * <p>Note that, due to its signature, this method cannot return null. If the value is NULL, it
   * will return 0.0. If this doesn't work for you, either call {@link #isNullValue(String)} before
   * calling this method, or use {@link #getValueAsObject(String)} instead.
   *
   * @param name a name
   * @return the FLOAT value for the specified name
   */
  public float getFloatValue(String name) {
    checkIfExists(name);

    if (isNullValue(name)) {
      // default value
      return 0.0F;
    }
    assert values.get(name).isPresent();
    return values.get(name).get().getAsFloat();
  }

  /**
   * Returns the DOUBLE value for the specified name added to the list of put values.
   *
   * <p>Note that, due to its signature, this method cannot return null. If the value is NULL, it
   * will return 0.0. If this doesn't work for you, either call {@link #isNullValue(String)} before
   * calling this method, or use {@link #getValueAsObject(String)} instead.
   *
   * @param name a name
   * @return the DOUBLE value for the specified name
   */
  public double getDoubleValue(String name) {
    checkIfExists(name);

    if (isNullValue(name)) {
      // default value
      return 0.0D;
    }
    assert values.get(name).isPresent();
    return values.get(name).get().getAsDouble();
  }

  /**
   * Returns the TEXT value for the specified name added to the list of put values.
   *
   * @param name a name
   * @return the TEXT value for the specified name. If the value is NULL, null
   */
  @Nullable
  public String getTextValue(String name) {
    checkIfExists(name);

    if (isNullValue(name)) {
      // default value
      return null;
    }
    assert values.get(name).isPresent();
    return values.get(name).get().getAsString().orElse(null);
  }

  /**
   * Returns the BLOB value as a ByteBuffer type for the specified name added to the list of put
   * values.
   *
   * @param name a name
   * @return the BLOB value for the specified name. If the value is NULL, null
   */
  @Nullable
  public ByteBuffer getBlobValue(String name) {
    return getBlobValueAsByteBuffer(name);
  }

  /**
   * Returns the BLOB value as a ByteBuffer type for the specified name added to the list of put
   * values.
   *
   * @param name a name
   * @return the BLOB value for the specified name. If the value is NULL, null
   */
  @Nullable
  public ByteBuffer getBlobValueAsByteBuffer(String name) {
    checkIfExists(name);

    if (isNullValue(name)) {
      // default value
      return null;
    }
    assert values.get(name).isPresent();
    return values.get(name).get().getAsByteBuffer().orElse(null);
  }

  /**
   * Returns the BLOB value as a byte array type for the specified name added to the list of put
   * values.
   *
   * @param name a name
   * @return the BLOB value for the specified name. If the value is NULL, null
   */
  @Nullable
  public byte[] getBlobValueAsBytes(String name) {
    checkIfExists(name);

    if (isNullValue(name)) {
      // default value
      return null;
    }
    assert values.get(name).isPresent();
    return values.get(name).get().getAsBytes().orElse(null);
  }

  /**
   * Returns the value as an Object type for the specified name added to the list of put values.
   *
   * <p>If the columns is a BOOLEAN type, it returns a {@code Boolean} object. If the columns is an
   * INT type, it returns an {@code Integer} object. If the columns is a BIGINT type, it returns a
   * {@code LONG} object. If the columns is a FLOAT type, it returns a {@code FLOAT} object. If the
   * columns is a DOUBLE type, it returns a {@code DOUBLE} object. If the columns is a TEXT type, it
   * returns a {@code String} object. If the columns is a BLOB type, it returns a {@code ByteBuffer}
   * object.
   *
   * @param name a name
   * @return the value for the specified name. If the value is NULL, null
   */
  @Nullable
  public Object getValueAsObject(String name) {
    checkIfExists(name);
    if (isNullValue(name)) {
      return null;
    }

    Optional<Value<?>> value = values.get(name);
    assert value.isPresent();
    if (value.get() instanceof BooleanValue) {
      return getBooleanValue(name);
    } else if (value.get() instanceof IntValue) {
      return getIntValue(name);
    } else if (value.get() instanceof BigIntValue) {
      return getBigIntValue(name);
    } else if (value.get() instanceof FloatValue) {
      return getFloatValue(name);
    } else if (value.get() instanceof DoubleValue) {
      return getDoubleValue(name);
    } else if (value.get() instanceof TextValue) {
      return getTextValue(name);
    } else if (value.get() instanceof BlobValue) {
      return getBlobValue(name);
    } else {
      throw new AssertionError();
    }
  }

  /**
   * Indicates whether the list of put values contains the specified column.
   *
   * @param name a name
   * @return whether the result contains the specified column name
   */
  public boolean containsColumn(String name) {
    return values.containsKey(name);
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
