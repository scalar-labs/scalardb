package com.scalar.db.api;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.io.Key;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstraction for storage operations.
 *
 * @author Hiroyuki Yamada
 */
@NotThreadSafe
public abstract class Operation {
  private static final Logger logger = LoggerFactory.getLogger(Operation.class);

  private final Key partitionKey;
  @Nullable private final Key clusteringKey;
  @Nullable private String namespace;
  private String tableName;
  private Consistency consistency;
  private final ImmutableMap<String, String> attributes;

  Operation(
      @Nullable String namespace,
      String tableName,
      Key partitionKey,
      @Nullable Key clusteringKey,
      @Nullable Consistency consistency,
      ImmutableMap<String, String> attributes) {
    this.partitionKey = Objects.requireNonNull(partitionKey);
    this.clusteringKey = clusteringKey;
    this.namespace = namespace;
    this.tableName = Objects.requireNonNull(tableName);
    this.consistency = consistency != null ? consistency : Consistency.SEQUENTIAL;
    this.attributes = attributes;
  }

  /**
   * Constructs an {@code Operation}.
   *
   * @param partitionKey the partition key
   * @param clusteringKey the clustering key
   * @deprecated As of release 3.15.0. Will be removed in release 5.0.0
   */
  @Deprecated
  public Operation(Key partitionKey, @Nullable Key clusteringKey) {
    this.partitionKey = Objects.requireNonNull(partitionKey);
    this.clusteringKey = clusteringKey;
    namespace = null;
    tableName = null;
    consistency = Consistency.SEQUENTIAL;
    attributes = ImmutableMap.of();
  }

  /**
   * Constructs an {@code Operation}.
   *
   * @param namespace the namespace
   * @param tableName the table name
   * @param partitionKey the partition key
   * @param clusteringKey the clustering key
   * @deprecated As of release 3.15.0. Will be removed in release 5.0.0
   */
  @Deprecated
  public Operation(
      @Nullable String namespace, String tableName, Key partitionKey, @Nullable Key clusteringKey) {
    this.partitionKey = Objects.requireNonNull(partitionKey);
    this.clusteringKey = clusteringKey;
    this.namespace = namespace;
    this.tableName = tableName;
    consistency = Consistency.SEQUENTIAL;
    attributes = ImmutableMap.of();
  }

  /**
   * Constructs an {@code Operation}.
   *
   * @param operation the operation
   * @deprecated As of release 3.15.0. Will be removed in release 5.0.0
   */
  @Deprecated
  public Operation(Operation operation) {
    this.partitionKey = operation.partitionKey;
    this.clusteringKey = operation.clusteringKey;
    namespace = operation.namespace;
    tableName = operation.tableName;
    consistency = operation.consistency;
    attributes = operation.attributes;
  }

  /**
   * Returns the namespace for this operation
   *
   * @return an {@code Optional} with the returned namespace
   */
  @Nonnull
  public Optional<String> forNamespace() {
    return Optional.ofNullable(namespace);
  }

  /**
   * Returns the table name for this operation
   *
   * @return an {@code Optional} with the returned table name
   */
  @Nonnull
  public Optional<String> forTable() {
    return Optional.ofNullable(tableName);
  }

  /**
   * Returns the full table name with the full namespace for this operation
   *
   * @return an {@code Optional} with the returned the full table name
   */
  @Nonnull
  public Optional<String> forFullTableName() {
    if (namespace == null || tableName == null) {
      logger.warn("Namespace or table name isn't specified");
      return Optional.empty();
    }
    return Optional.of(namespace + "." + tableName);
  }

  /**
   * Sets the specified target namespace for this operation
   *
   * @param namespace target namespace for this operation
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  public Operation forNamespace(String namespace) {
    this.namespace = namespace;
    return this;
  }

  /**
   * Sets the specified target table for this operation
   *
   * @param tableName target table name for this operation
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  public Operation forTable(String tableName) {
    this.tableName = tableName;
    return this;
  }

  /**
   * Returns the partition key
   *
   * @return the partition {@code Key}
   */
  @Nonnull
  public Key getPartitionKey() {
    return partitionKey;
  }

  /**
   * Returns the clustering key
   *
   * @return the clustering {@code Key}
   */
  @Nonnull
  public Optional<Key> getClusteringKey() {
    return Optional.ofNullable(clusteringKey);
  }

  /**
   * Returns the consistency level for this operation
   *
   * @return the consistency level
   */
  public Consistency getConsistency() {
    return consistency;
  }

  /**
   * Sets the specified consistency level for this operation
   *
   * @param consistency consistency level to set
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  public Operation withConsistency(Consistency consistency) {
    this.consistency = consistency;
    return this;
  }

  /**
   * Returns the attributes for this operation.
   *
   * @return the attributes
   */
  public Map<String, String> getAttributes() {
    return attributes;
  }

  /**
   * Returns the value of the specified attribute.
   *
   * @param name the name of the attribute
   * @return the value of the specified attribute
   */
  public Optional<String> getAttribute(String name) {
    return Optional.ofNullable(attributes.get(name));
  }

  /**
   * Indicates whether some other object is "equal to" this object. The other object is considered
   * equal if:
   *
   * <ul>
   *   <li>it is also an {@code Operation} and
   *   <li>both instances have the same partition key, clustering key, namespace, table name and
   *       consistency
   * </ul>
   *
   * @param o an object to be tested for equality
   * @return {@code true} if the other object is "equal to" this object otherwise {@code false}
   */
  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof Operation)) {
      return false;
    }
    Operation other = (Operation) o;
    return Objects.equals(partitionKey, other.partitionKey)
        && Objects.equals(clusteringKey, other.clusteringKey)
        && Objects.equals(namespace, other.namespace)
        && Objects.equals(tableName, other.tableName)
        && Objects.equals(consistency, other.consistency)
        && Objects.equals(attributes, other.attributes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionKey, clusteringKey, namespace, tableName, consistency, attributes);
  }

  /**
   * Access the specified visitor
   *
   * @param v a visitor object to access
   */
  public abstract void accept(OperationVisitor v);
}
