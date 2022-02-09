package com.scalar.db.api;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A command to put an entry to {@link DistributedStorage}.
 *
 * @author Hiroyuki Yamada
 */
@NotThreadSafe
public class Put extends Mutation {
  private final Map<String, Value<?>> values;

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
    values.put(value.getName(), value);
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
    values.put(name, new BooleanValue(name, value));
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
    values.put(name, new IntValue(name, value));
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
    values.put(name, new BigIntValue(name, value));
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
    values.put(name, new FloatValue(name, value));
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
    values.put(name, new DoubleValue(name, value));
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
    values.put(name, new TextValue(name, value));
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
    values.put(name, new BlobValue(name, value));
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
    values.put(name, new BlobValue(name, value));
    return this;
  }

  /**
   * Adds the specified collection of {@link Value}s to the list of put values.
   *
   * @param values a collection of {@code Value}s to put
   * @return this object
   */
  public Put withValues(Collection<Value<?>> values) {
    values.forEach(v -> this.values.put(v.getName(), v));
    return this;
  }

  /**
   * Returns a map of {@link Value}s
   *
   * @return a map of {@code Value}s
   */
  public Map<String, Value<?>> getValues() {
    return ImmutableMap.copyOf(values);
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
        .add("values", getValues())
        .add("consistency", getConsistency())
        .add("condition", getCondition())
        .toString();
  }
}
