package com.scalar.db.api;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
import com.scalar.db.api.GetBuilder.BuildableGetOrGetWithIndexFromExisting;
import com.scalar.db.api.GetBuilder.Namespace;
import com.scalar.db.io.Key;
import java.util.Collection;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A command to retrieve an entry from the underlying storage.
 *
 * @author Hiroyuki Yamada
 */
@NotThreadSafe
public class Get extends Selection {

  /**
   * Constructs a {@code Get} with the specified partition {@code Key}.
   *
   * @param partitionKey a partition key (it might be composed of multiple values)
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link Get#newBuilder()}
   *     instead
   */
  @Deprecated
  @SuppressWarnings("InlineMeSuggester")
  public Get(Key partitionKey) {
    this(partitionKey, null);
  }

  /**
   * Constructs a {@code Get} with the specified partition {@link Key} and the clustering {@link
   * Key}.
   *
   * @param partitionKey a partition {@code Key} (it might be composed of multiple values)
   * @param clusteringKey a clustering {@code Key} (it might be composed of multiple values)
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link Get#newBuilder()}
   *     instead
   */
  @Deprecated
  @SuppressWarnings("InlineMeSuggester")
  public Get(Key partitionKey, Key clusteringKey) {
    super(partitionKey, clusteringKey);
  }

  /**
   * Copy a Get.
   *
   * @param get a Get
   * @deprecated Use {@link Get#newBuilder(Get)} instead
   */
  @Deprecated
  @SuppressWarnings("InlineMeSuggester")
  public Get(Get get) {
    super(get);
  }

  /**
   * Build a {@code Get} operation using a builder.
   *
   * @return a {@code Get} operation builder
   */
  public static Namespace newBuilder() {
    return new Namespace();
  }

  /**
   * Build a {@code Get} operation from an existing {@code Get} object using a builder. The builder
   * will be parametrized by default with all the existing {@code Get} attributes
   *
   * @param get an existing {@code Get} operation
   * @return a {@code Get} operation builder
   */
  public static BuildableGetOrGetWithIndexFromExisting newBuilder(Get get) {
    checkNotNull(get);
    return new BuildableGetOrGetWithIndexFromExisting(get);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Get builder instead; to create a Get builder, use {@link Get#newBuilder()}
   */
  @Deprecated
  @Override
  public Get forNamespace(String namespace) {
    return (Get) super.forNamespace(namespace);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Get builder instead; to create a Get builder, use {@link Get#newBuilder()}
   */
  @Deprecated
  @Override
  public Get forTable(String tableName) {
    return (Get) super.forTable(tableName);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Get builder instead; to create a Get builder, use {@link Get#newBuilder()}
   */
  @Deprecated
  @Override
  public Get withConsistency(Consistency consistency) {
    return (Get) super.withConsistency(consistency);
  }

  @Override
  public void accept(OperationVisitor v) {
    v.visit(this);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Get builder instead; to create a Get builder, use {@link Get#newBuilder()}
   */
  @Deprecated
  @Override
  public Get withProjection(String projection) {
    return (Get) super.withProjection(projection);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Get builder instead; to create a Get builder, use {@link Get#newBuilder()}
   */
  @Deprecated
  @Override
  public Get withProjections(Collection<String> projections) {
    return (Get) super.withProjections(projections);
  }

  @Override
  Get withConjunctions(Collection<Conjunction> conjunctions) {
    return (Get) super.withConjunctions(conjunctions);
  }

  /**
   * Indicates whether some other object is "equal to" this object. The other object is considered
   * equal if:
   *
   * <ul>
   *   <li>both super class instances are equal and
   *   <li>it is also an {@code Get}
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
    return o instanceof Get;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("namespace", forNamespace())
        .add("table", forTable())
        .add("partitionKey", getPartitionKey())
        .add("clusteringKey", getClusteringKey())
        .add("projections", getProjections())
        .add("conjunctions", getConjunctions())
        .add("consistency", getConsistency())
        .toString();
  }
}
