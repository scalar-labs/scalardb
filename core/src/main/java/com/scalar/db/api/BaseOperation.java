package com.scalar.db.api;

import com.google.common.collect.ComparisonChain;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A base abstraction for storage operations. */
@NotThreadSafe
public abstract class BaseOperation implements BaseOperationInterface {
  private static final Logger logger = LoggerFactory.getLogger(BaseOperation.class);
  private Optional<String> namespace;
  private Optional<String> tableName;
  private Consistency consistency;

  BaseOperation() {
    namespace = Optional.empty();
    tableName = Optional.empty();
    consistency = Consistency.SEQUENTIAL;
  }

  BaseOperation(BaseOperation operation) {
    namespace = operation.namespace;
    tableName = operation.tableName;
    consistency = operation.consistency;
  }

  /**
   * Returns the namespace for this operation
   *
   * @return an {@code Optional} with the returned namespace
   */
  @Override
  @Nonnull
  public Optional<String> forNamespace() {
    return namespace;
  }

  /**
   * Returns the table name for this operation
   *
   * @return an {@code Optional} with the returned table name
   */
  @Override
  @Nonnull
  public Optional<String> forTable() {
    return tableName;
  }

  /**
   * Returns the full table name with the full namespace for this operation
   *
   * @return an {@code Optional} with the returned the full table name
   */
  @Nonnull
  public Optional<String> forFullTableName() {
    if (!namespace.isPresent() || !tableName.isPresent()) {
      logger.warn("namespace or table name isn't specified");
      return Optional.empty();
    }
    return Optional.of(namespace.get() + "." + tableName.get());
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
   * Sets the specified target namespace for this operation
   *
   * @param namespace target namespace for this operation
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  @Override
  public BaseOperation forNamespace(String namespace) {
    this.namespace = Optional.ofNullable(namespace);
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
  @Override
  public BaseOperation forTable(String tableName) {
    this.tableName = Optional.ofNullable(tableName);
    return this;
  }

  /**
   * Sets the specified consistency level for this operation
   *
   * @param consistency consistency level to set
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  BaseOperation withConsistency(Consistency consistency) {
    this.consistency = consistency;
    return this;
  }

  /**
   * Indicates whether some other object is "equal to" this object. The other object is considered
   * equal if:
   *
   * <ul>
   *   <li>it is also an {@code BaseOperation} and
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
    if (!(o instanceof BaseOperation)) {
      return false;
    }
    BaseOperation other = (BaseOperation) o;
    return ComparisonChain.start()
            .compare(
                namespace.orElse(null),
                other.namespace.orElse(null),
                Comparator.nullsFirst(Comparator.naturalOrder()))
            .compare(
                tableName.orElse(null),
                other.tableName.orElse(null),
                Comparator.nullsFirst(Comparator.naturalOrder()))
            .compare(consistency, other.consistency)
            .result()
        == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, tableName, consistency);
  }
}
