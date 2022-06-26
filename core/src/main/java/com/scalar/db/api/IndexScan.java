package com.scalar.db.api;

import com.google.common.base.MoreObjects;
import com.scalar.db.io.Key;
import java.util.Collection;
import java.util.Objects;

public class IndexScan extends Scan {

  /**
   * @param indexKey an index key
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link
   *     Scan#newBuilder()} instead
   */
  @Deprecated
  @SuppressWarnings("InlineMeSuggester")
  public IndexScan(Key indexKey) {
    super(indexKey);
  }

  /**
   * Copy a IndexScan.
   *
   * @param indexScan a IndexScan
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link
   *     Scan#newBuilder(Scan)} instead
   */
  @Deprecated
  @SuppressWarnings("InlineMeSuggester")
  public IndexScan(IndexScan indexScan) {
    super(indexScan);
  }

  /**
   * Guaranteed to throw an exception.
   *
   * @throws UnsupportedOperationException always
   * @deprecated Unsupported operation.
   */
  @Deprecated
  @Override
  public IndexScan withStart(Key clusteringKey) {
    throw new UnsupportedOperationException();
  }

  /**
   * Guaranteed to throw an exception.
   *
   * @throws UnsupportedOperationException always
   * @deprecated Unsupported operation.
   */
  @Deprecated
  @Override
  public IndexScan withStart(Key clusteringKey, boolean inclusive) {
    throw new UnsupportedOperationException();
  }

  /**
   * Guaranteed to throw an exception.
   *
   * @throws UnsupportedOperationException always
   * @deprecated Unsupported operation.
   */
  @Deprecated
  @Override
  public IndexScan withEnd(Key clusteringKey) {
    throw new UnsupportedOperationException();
  }

  /**
   * Guaranteed to throw an exception.
   *
   * @throws UnsupportedOperationException always
   * @deprecated Unsupported operation.
   */
  @Deprecated
  @Override
  public IndexScan withEnd(Key clusteringKey, boolean inclusive) {
    throw new UnsupportedOperationException();
  }

  /**
   * Guaranteed to throw an exception.
   *
   * @throws UnsupportedOperationException always
   * @deprecated Unsupported operation.
   */
  @Deprecated
  @Override
  public IndexScan withOrdering(Ordering ordering) {
    throw new UnsupportedOperationException();
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Scan builder instead; to create a Scan builder, use {@link Scan#newBuilder()}
   */
  @Override
  @Deprecated
  public IndexScan withLimit(int limit) {
    return (IndexScan) super.withLimit(limit);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Scan builder instead; to create a Scan builder, use {@link Scan#newBuilder()}
   */
  @Override
  @Deprecated
  public IndexScan forNamespace(String namespace) {
    return (IndexScan) super.forNamespace(namespace);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Scan builder instead; to create a Scan builder, use {@link Scan#newBuilder()}
   */
  @Override
  @Deprecated
  public IndexScan forTable(String tableName) {
    return (IndexScan) super.forTable(tableName);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Scan builder instead; to create a Scan builder, use {@link Scan#newBuilder()}
   */
  @Override
  @Deprecated
  public IndexScan withConsistency(Consistency consistency) {
    return (IndexScan) super.withConsistency(consistency);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Scan builder instead; to create a Scan builder, use {@link Scan#newBuilder()}
   */
  @Override
  @Deprecated
  public IndexScan withProjection(String projection) {
    return (IndexScan) super.withProjection(projection);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Scan builder instead; to create a Scan builder, use {@link Scan#newBuilder()}
   */
  @Override
  @Deprecated
  public IndexScan withProjections(Collection<String> projections) {
    return (IndexScan) super.withProjections(projections);
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    if (o == this) {
      return true;
    }
    return o instanceof IndexScan;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode());
  }

  @Override
  public String toString() {
    return super.toString() + MoreObjects.toStringHelper(this);
  }
}
