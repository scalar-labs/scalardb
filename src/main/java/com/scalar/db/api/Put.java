package com.scalar.db.api;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A command to put an entry to {@link DistributedStorage}.
 *
 * @author Hiroyuki Yamada
 */
@NotThreadSafe
public class Put extends Mutation {
  private final Map<String, Value> values;

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

  /**
   * Adds the specified {@link Value} to the list of put values.
   *
   * @param value a {@code Value} to put
   * @return this object
   */
  public Put withValue(Value value) {
    values.put(value.getName(), value);
    return this;
  }

  /**
   * Adds the specified collection of {@link Value}s to the list of put values.
   *
   * @param values a collection of {@code Value}s to put
   * @return this object
   */
  public Put withValues(Collection<Value> values) {
    values.forEach(v -> this.values.put(v.getName(), v));
    return this;
  }

  /**
   * Returns a map of {@link Value}s
   *
   * @return a map of {@code Value}s
   */
  public Map<String, Value> getValues() {
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
