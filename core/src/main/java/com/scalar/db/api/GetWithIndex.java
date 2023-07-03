package com.scalar.db.api;

import com.scalar.db.io.Key;
import java.util.Collection;
import java.util.Objects;

public class GetWithIndex extends Get {

  /**
   * Constructs an {@code GetWithIndex} with the specified index {@code Key}.
   *
   * @param indexKey an index key
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link Get#newBuilder()}
   *     instead
   */
  @Deprecated
  @SuppressWarnings("InlineMeSuggester")
  public GetWithIndex(Key indexKey) {
    super(indexKey);
  }

  /**
   * Copy a GetWithIndex.
   *
   * @param getWithIndex a GetWithIndex
   * @deprecated Use {@link Get#newBuilder(Get)} instead
   */
  @Deprecated
  @SuppressWarnings("InlineMeSuggester")
  public GetWithIndex(GetWithIndex getWithIndex) {
    super(getWithIndex);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Get builder instead; to create a Get builder, use {@link Get#newBuilder()}
   */
  @Deprecated
  @Override
  public GetWithIndex forNamespace(String namespace) {
    return (GetWithIndex) super.forNamespace(namespace);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Get builder instead; to create a Get builder, use {@link Get#newBuilder()}
   */
  @Deprecated
  @Override
  public GetWithIndex forTable(String tableName) {
    return (GetWithIndex) super.forTable(tableName);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Get builder instead; to create a Get builder, use {@link Get#newBuilder()}
   */
  @Deprecated
  @Override
  public GetWithIndex withConsistency(Consistency consistency) {
    return (GetWithIndex) super.withConsistency(consistency);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Get builder instead; to create a Get builder, use {@link Get#newBuilder()}
   */
  @Deprecated
  @Override
  public GetWithIndex withProjection(String projection) {
    return (GetWithIndex) super.withProjection(projection);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Get builder instead; to create a Get builder, use {@link Get#newBuilder()}
   */
  @Deprecated
  @Override
  public GetWithIndex withProjections(Collection<String> projections) {
    return (GetWithIndex) super.withProjections(projections);
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    if (o == this) {
      return true;
    }
    return o instanceof GetWithIndex;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode());
  }
}
