package com.scalar.db.api;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/** A command to update an entry in the underlying storage. */
@NotThreadSafe
public class Update extends Mutation {

  private final ImmutableMap<String, Column<?>> columns;

  Update(
      @Nullable String namespace,
      String tableName,
      Key partitionKey,
      @Nullable Key clusteringKey,
      ImmutableMap<String, Column<?>> columns,
      @Nullable MutationCondition condition) {
    super(namespace, tableName, partitionKey, clusteringKey, null, condition);
    this.columns = columns;
  }

  public Map<String, Column<?>> getColumns() {
    return columns;
  }

  @Nonnull
  @Override
  public Optional<MutationCondition> getCondition() {
    return super.getCondition();
  }

  @Override
  public Consistency getConsistency() {
    throw new UnsupportedOperationException();
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public Operation withConsistency(Consistency consistency) {
    throw new UnsupportedOperationException();
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
   *   <li>it is also an {@code Update} and
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
    if (!(o instanceof Update)) {
      return false;
    }
    Update other = (Update) o;
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
        .add("columns", getColumns())
        .add("condition", getCondition())
        .toString();
  }

  /**
   * Build a {@code Update} operation using a builder.
   *
   * @return a {@code Update} operation builder
   */
  public static UpdateBuilder.Namespace newBuilder() {
    return new UpdateBuilder.Namespace();
  }

  /**
   * Build a {@code Update} operation from an existing {@code Update} object using a builder. The
   * builder will be parametrized by default with all the existing {@code Update} parameters.
   *
   * @param update an existing {@code Update} operation
   * @return a {@code Update} operation builder
   */
  public static UpdateBuilder.BuildableFromExisting newBuilder(Update update) {
    checkNotNull(update);
    return new UpdateBuilder.BuildableFromExisting(update);
  }
}
