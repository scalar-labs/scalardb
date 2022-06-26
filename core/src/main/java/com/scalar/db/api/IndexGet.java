package com.scalar.db.api;

import com.google.common.base.MoreObjects;
import com.scalar.db.io.Key;
import java.util.Collection;
import java.util.Objects;

public class IndexGet extends Get {

  /**
   * Constructs an {@code IndexGet} with the specified index {@code Key}.
   *
   * @param indexKey an index key
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link Get#newBuilder()}
   *     instead
   */
  @Deprecated
  @SuppressWarnings("InlineMeSuggester")
  public IndexGet(Key indexKey) {
    super(indexKey);
  }

  /**
   * Copy a IndexGet.
   *
   * @param indexGet an IndexGet
   * @deprecated Use {@link Get#newBuilder(Get)} instead
   */
  @Deprecated
  @SuppressWarnings("InlineMeSuggester")
  public IndexGet(IndexGet indexGet) {
    super(indexGet);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Get builder instead; to create a Get builder, use {@link Get#newBuilder()}
   */
  @Deprecated
  @Override
  public IndexGet forNamespace(String namespace) {
    return (IndexGet) super.forNamespace(namespace);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Get builder instead; to create a Get builder, use {@link Get#newBuilder()}
   */
  @Deprecated
  @Override
  public IndexGet forTable(String tableName) {
    return (IndexGet) super.forTable(tableName);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Get builder instead; to create a Get builder, use {@link Get#newBuilder()}
   */
  @Deprecated
  @Override
  public IndexGet withConsistency(Consistency consistency) {
    return (IndexGet) super.withConsistency(consistency);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Get builder instead; to create a Get builder, use {@link Get#newBuilder()}
   */
  @Deprecated
  @Override
  public IndexGet withProjection(String projection) {
    return (IndexGet) super.withProjection(projection);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Get builder instead; to create a Get builder, use {@link Get#newBuilder()}
   */
  @Deprecated
  @Override
  public IndexGet withProjections(Collection<String> projections) {
    return (IndexGet) super.withProjections(projections);
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    if (o == this) {
      return true;
    }
    return o instanceof IndexGet;
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
