package com.scalar.db.transaction.consensuscommit;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.scalar.db.api.Mutation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

@Immutable
public class PartitionedMutations {
  private final ImmutableListMultimap<Key, Mutation> partitions;

  @SafeVarargs
  public PartitionedMutations(Collection<? extends Mutation>... collections) {
    ImmutableListMultimap.Builder<Key, Mutation> builder = ImmutableListMultimap.builder();
    for (Collection<? extends Mutation> collection : collections) {
      collection.forEach(m -> builder.put(new Key(m), m));
    }
    partitions = builder.build();
  }

  @Nonnull
  public ImmutableList<Key> getOrderedKeys() {
    List<Key> keys = new ArrayList<>(partitions.keySet());
    Collections.sort(keys);
    return ImmutableList.copyOf(keys);
  }

  @Nonnull
  public ImmutableList<Mutation> get(Key key) {
    return partitions.get(key);
  }

  @Immutable
  public static final class Key implements Comparable<Key> {
    private final String namespace;
    private final String table;
    private final com.scalar.db.io.Key partitionKey;

    public Key(Mutation mutation) {
      namespace = mutation.forNamespace().get();
      table = mutation.forTable().get();
      partitionKey = mutation.getPartitionKey();
    }

    @Override
    public int hashCode() {
      return Objects.hash(namespace, table, partitionKey);
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (!(o instanceof Key)) {
        return false;
      }
      Key another = (Key) o;
      return this.namespace.equals(another.namespace)
          && this.table.equals(another.table)
          && this.partitionKey.equals(another.partitionKey);
    }

    @Override
    public int compareTo(Key o) {
      return ComparisonChain.start()
          .compare(this.namespace, o.namespace)
          .compare(this.table, o.table)
          .compare(this.partitionKey, o.partitionKey)
          .result();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof PartitionedMutations)) {
      return false;
    }
    PartitionedMutations other = (PartitionedMutations) o;
    return partitions.equals(other.partitions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitions);
  }
}
