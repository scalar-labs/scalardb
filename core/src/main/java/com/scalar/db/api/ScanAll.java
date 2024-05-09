package com.scalar.db.api;

import com.scalar.db.io.Key;
import java.util.Collection;
import java.util.Objects;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A command to retrieve all the entries from the underlying storage. The scan range of clustering
 * key and {@link Ordering} cannot be specified for this command. The number of {@link Result} can
 * be limited.
 */
@NotThreadSafe
public class ScanAll extends Scan {

  private static final Key DUMMY_PARTITION_KEY = Key.of();

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link
   *     Scan#newBuilder()} instead
   */
  @Deprecated
  @SuppressWarnings("InlineMeSuggester")
  public ScanAll() {
    super(DUMMY_PARTITION_KEY);
  }

  /**
   * Copy a ScanAll.
   *
   * @param scanAll a ScanAll
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link
   *     Scan#newBuilder(Scan)} instead
   */
  @Deprecated
  @SuppressWarnings("InlineMeSuggester")
  public ScanAll(ScanAll scanAll) {
    super(scanAll);
  }

  /**
   * Guaranteed to throw an exception.
   *
   * @throws UnsupportedOperationException always
   * @deprecated Unsupported operation.
   */
  @Deprecated
  @Override
  public ScanAll withStart(Key clusteringKey) {
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
  public ScanAll withStart(Key clusteringKey, boolean inclusive) {
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
  public ScanAll withEnd(Key clusteringKey) {
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
  public ScanAll withEnd(Key clusteringKey, boolean inclusive) {
    throw new UnsupportedOperationException();
  }

  /**
   * Sets the specified scan ordering. Ordering can only be specified with arbitrary columns. To
   * sort results by multiple columns, call this method multiple times in the order of sorting.
   *
   * @param ordering a scan ordering
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Scan builder instead; to create a Scan builder, use {@link Scan#newBuilder()}
   */
  @Deprecated
  @Override
  public ScanAll withOrdering(Ordering ordering) {
    return (ScanAll) super.withOrdering(ordering);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Scan builder instead; to create a Scan builder, use {@link Scan#newBuilder()}
   */
  @Override
  @Deprecated
  public ScanAll withLimit(int limit) {
    return (ScanAll) super.withLimit(limit);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Scan builder instead; to create a Scan builder, use {@link Scan#newBuilder()}
   */
  @Override
  @Deprecated
  public ScanAll forNamespace(String namespace) {
    return (ScanAll) super.forNamespace(namespace);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Scan builder instead; to create a Scan builder, use {@link Scan#newBuilder()}
   */
  @Override
  @Deprecated
  public ScanAll forTable(String tableName) {
    return (ScanAll) super.forTable(tableName);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Scan builder instead; to create a Scan builder, use {@link Scan#newBuilder()}
   */
  @Override
  @Deprecated
  public ScanAll withConsistency(Consistency consistency) {
    return (ScanAll) super.withConsistency(consistency);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Scan builder instead; to create a Scan builder, use {@link Scan#newBuilder()}
   */
  @Override
  @Deprecated
  public ScanAll withProjection(String projection) {
    return (ScanAll) super.withProjection(projection);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Scan builder instead; to create a Scan builder, use {@link Scan#newBuilder()}
   */
  @Override
  @Deprecated
  public ScanAll withProjections(Collection<String> projections) {
    return (ScanAll) super.withProjections(projections);
  }

  @Override
  ScanAll withConjunctions(Collection<Conjunction> conjunctions) {
    return (ScanAll) super.withConjunctions(conjunctions);
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    if (o == this) {
      return true;
    }
    return o instanceof ScanAll;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode());
  }
}
