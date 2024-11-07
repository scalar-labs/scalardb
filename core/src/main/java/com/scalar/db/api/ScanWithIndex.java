package com.scalar.db.api;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.io.Key;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A command to retrieve entries from the underlying storage by using an index. The number of {@link
 * Result} can be limited.
 */
@NotThreadSafe
public class ScanWithIndex extends Scan {

  ScanWithIndex(
      @Nullable String namespace,
      String tableName,
      Key indexKey,
      @Nullable Consistency consistency,
      List<String> projections,
      ImmutableSet<Conjunction> conjunctions,
      int limit) {
    super(
        namespace,
        tableName,
        indexKey,
        consistency,
        projections,
        conjunctions,
        null,
        false,
        null,
        false,
        ImmutableList.of(),
        limit);
  }

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
