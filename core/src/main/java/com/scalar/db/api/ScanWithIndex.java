package com.scalar.db.api;

import com.scalar.db.io.Key;
import java.util.Collection;
import java.util.Objects;

public class ScanWithIndex extends Scan {

  /**
   * @param indexKey an index key
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link
   *     Scan#newBuilder()} instead
   */
  @Deprecated
  @SuppressWarnings("InlineMeSuggester")
  public ScanWithIndex(Key indexKey) {
    super(indexKey);
  }

  /**
   * Copy a ScanWithIndex.
   *
   * @param scanWithIndex a ScanWithIndex
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link
   *     Scan#newBuilder(Scan)} instead
   */
  @Deprecated
  @SuppressWarnings("InlineMeSuggester")
  public ScanWithIndex(ScanWithIndex scanWithIndex) {
    super(scanWithIndex);
  }

  /**
   * Guaranteed to throw an exception.
   *
   * @throws UnsupportedOperationException always
   * @deprecated Unsupported operation.
   */
  @Deprecated
  @Override
  public ScanWithIndex withStart(Key clusteringKey) {
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
  public ScanWithIndex withStart(Key clusteringKey, boolean inclusive) {
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
  public ScanWithIndex withEnd(Key clusteringKey) {
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
  public ScanWithIndex withEnd(Key clusteringKey, boolean inclusive) {
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
  public ScanWithIndex withOrdering(Ordering ordering) {
    throw new UnsupportedOperationException();
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Scan builder instead; to create a Scan builder, use {@link Scan#newBuilder()}
   */
  @Override
  @Deprecated
  public ScanWithIndex withLimit(int limit) {
    return (ScanWithIndex) super.withLimit(limit);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Scan builder instead; to create a Scan builder, use {@link Scan#newBuilder()}
   */
  @Override
  @Deprecated
  public ScanWithIndex forNamespace(String namespace) {
    return (ScanWithIndex) super.forNamespace(namespace);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Scan builder instead; to create a Scan builder, use {@link Scan#newBuilder()}
   */
  @Override
  @Deprecated
  public ScanWithIndex forTable(String tableName) {
    return (ScanWithIndex) super.forTable(tableName);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Scan builder instead; to create a Scan builder, use {@link Scan#newBuilder()}
   */
  @Override
  @Deprecated
  public ScanWithIndex withConsistency(Consistency consistency) {
    return (ScanWithIndex) super.withConsistency(consistency);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Scan builder instead; to create a Scan builder, use {@link Scan#newBuilder()}
   */
  @Override
  @Deprecated
  public ScanWithIndex withProjection(String projection) {
    return (ScanWithIndex) super.withProjection(projection);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Scan builder instead; to create a Scan builder, use {@link Scan#newBuilder()}
   */
  @Override
  @Deprecated
  public ScanWithIndex withProjections(Collection<String> projections) {
    return (ScanWithIndex) super.withProjections(projections);
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    if (o == this) {
      return true;
    }
    return o instanceof ScanWithIndex;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode());
  }
}
