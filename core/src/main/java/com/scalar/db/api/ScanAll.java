package com.scalar.db.api;

import com.google.common.base.MoreObjects;
import com.scalar.db.io.Key;
import java.util.Collection;
import java.util.Objects;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A command to retrieve all the entries of the database. The scan range of clustering key and
 * {@link Ordering} cannot be specified for this command. The number of {@link Result} can be
 * limited.
 */
@NotThreadSafe
public class ScanAll extends Scan {

  private static final Key DUMMY_PARTITION_KEY = Key.of();

  public ScanAll() {
    super(DUMMY_PARTITION_KEY);
  }

  public ScanAll(ScanAll scanAll) {
    super(scanAll);
  }

  @Override
  public ScanAll withStart(Key clusteringKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ScanAll withStart(Key clusteringKey, boolean inclusive) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Scan withEnd(Key clusteringKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Scan withEnd(Key clusteringKey, boolean inclusive) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ScanAll withOrdering(Ordering ordering) {
    throw new UnsupportedOperationException();
  }

  public ScanAll withLimit(int limit) {
    return (ScanAll) super.withLimit(limit);
  }

  @Override
  public ScanAll forNamespace(String namespace) {
    return (ScanAll) super.forNamespace(namespace);
  }

  @Override
  public ScanAll forTable(String tableName) {
    return (ScanAll) super.forTable(tableName);
  }

  @Override
  public ScanAll withConsistency(Consistency consistency) {
    return (ScanAll) super.withConsistency(consistency);
  }

  @Override
  public ScanAll withProjection(String projection) {
    return (ScanAll) super.withProjection(projection);
  }

  @Override
  public ScanAll withProjections(Collection<String> projections) {
    return (ScanAll) super.withProjections(projections);
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

  @Override
  public String toString() {
    return super.toString() + MoreObjects.toStringHelper(this);
  }
}
