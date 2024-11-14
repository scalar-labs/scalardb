package com.scalar.db.api;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.DeleteBuilder.BuildableFromExisting;
import com.scalar.db.api.DeleteBuilder.Namespace;
import com.scalar.db.io.Key;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A command to delete an entry from the underlying storage.
 *
 * @author Hiroyuki Yamada
 */
@NotThreadSafe
public class Delete extends Mutation {

  Delete(
      @Nullable String namespace,
      String tableName,
      Key partitionKey,
      @Nullable Key clusteringKey,
      @Nullable Consistency consistency,
      ImmutableMap<String, String> attributes,
      @Nullable MutationCondition condition) {
    super(namespace, tableName, partitionKey, clusteringKey, consistency, attributes, condition);
  }

  /**
   * Constructs a {@code Delete} with the specified partition {@code Key}.
   *
   * @param partitionKey a partition key (it might be composed of multiple values)
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link
   *     Delete#newBuilder()} instead
   */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  public Delete(Key partitionKey) {
    this(partitionKey, null);
  }

  /**
   * Constructs a {@code Delete} with the specified partition {@link Key} and the clustering {@link
   * Key}.
   *
   * @param partitionKey a partition {@code Key} (it might be composed of multiple values)
   * @param clusteringKey a clustering {@code Key} (it might be composed of multiple values)
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link
   *     Delete#newBuilder()} instead
   */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  public Delete(Key partitionKey, Key clusteringKey) {
    super(partitionKey, clusteringKey);
  }

  /**
   * Copy a Delete.
   *
   * @param delete a Delete
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link
   *     Delete#newBuilder(Delete)} instead.
   */
  @Deprecated
  public Delete(Delete delete) {
    super(delete);
  }

  /**
   * Build a {@code Delete} operation using a builder.
   *
   * @return a {@code Delete} operation builder
   */
  public static Namespace newBuilder() {
    return new Namespace();
  }

  /**
   * Build a {@code Delete} operation from an existing {@code Delete} object using a builder. The
   * builder will be parametrized by default with all the existing {@code Delete} parameters.
   *
   * @param delete an existing {@code Delete} operation
   * @return a {@code Delete} operation builder
   */
  public static BuildableFromExisting newBuilder(Delete delete) {
    checkNotNull(delete);
    return new BuildableFromExisting(delete);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Delete builder instead; to create a Delete builder, use {@link Delete#newBuilder()}
   */
  @Override
  @Deprecated
  public Delete forNamespace(String namespace) {
    return (Delete) super.forNamespace(namespace);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Delete builder instead; to create a Delete builder, use {@link Delete#newBuilder()}
   */
  @Override
  @Deprecated
  public Delete forTable(String tableName) {
    return (Delete) super.forTable(tableName);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Delete builder instead; to create a Delete builder, use {@link Delete#newBuilder()}
   */
  @Override
  @Deprecated
  public Delete withConsistency(Consistency consistency) {
    return (Delete) super.withConsistency(consistency);
  }

  @Override
  public void accept(OperationVisitor v) {
    v.visit(this);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Delete builder instead; to create a Delete builder, use {@link Delete#newBuilder()}
   */
  @Override
  @Deprecated
  public Delete withCondition(MutationCondition condition) {
    return (Delete) super.withCondition(condition);
  }

  @Nonnull
  @Override
  public Optional<MutationCondition> getCondition() {
    return super.getCondition();
  }

  /**
   * Indicates whether some other object is "equal to" this object. The other object is considered
   * equal if:
   *
   * <ul>
   *   <li>both super class instances are equal and
   *   <li>it is also an {@code Delete}
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
    return o instanceof Delete;
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
        .toString();
  }
}
