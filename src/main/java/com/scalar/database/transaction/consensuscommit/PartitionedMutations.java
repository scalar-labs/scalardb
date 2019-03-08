package com.scalar.database.transaction.consensuscommit;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.scalar.database.api.Mutation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/** Partition mutations based on their partition key */
@Immutable
public class PartitionedMutations {
  private final ImmutableListMultimap<Key, Mutation> partitions;

  /**
   * Constructs a {@code PartitionedMutations} from the specified {@link Collection} of {@link
   * Mutation}s
   *
   * @param collections a {@code Collection} of {@link Mutation}s
   */
  public PartitionedMutations(Collection<? extends Mutation>... collections) {
    ImmutableListMultimap.Builder<Key, Mutation> builder = ImmutableListMultimap.builder();
    for (Collection<? extends Mutation> collection : collections) {
      collection.forEach(m -> builder.put(new Key(m), m));
    }
    partitions = builder.build();
  }

  /**
   * Returns the list of {@link Key}s contained in the {@code PartitionedMutations}
   *
   * @return the list of {@link Key}s contained in the {@code PartitionedMutations}
   */
  @Nonnull
  public ImmutableList<Key> getOrderedKeys() {
    List<Key> keys = new ArrayList<>(partitions.keySet());
    Collections.sort(keys);
    return ImmutableList.copyOf(keys);
  }

  /**
   * Returns an {@link ImmutableList} of {@link Mutation}s associated with the specified {@link Key}
   *
   * @param key a {@link Key}
   * @return the list of {@link Key}s contained in the {@code PartitionedMutations}
   */
  @Nonnull
  public ImmutableList<Mutation> get(Key key) {
    return partitions.get(key);
  }

  @Immutable
  public static final class Key implements Comparable<Key> {
    private final String namespace;
    private final String table;
    private final com.scalar.database.io.Key partitionKey;

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

  /**
   * Indicates whether some other object is "equal to" this object. The other object is considered
   * equal if it is the same instance or if:
   *
   * <ul>
   *   <li>it is also a {@code PartitionedMutations} and
   *   <li>both instances have the same partitions.
   * </ul>
   *
   * @param o an object to be tested for equality
   * @return {@code true} if the other object is "equal to" this object otherwise {@code false}
   */
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
}
