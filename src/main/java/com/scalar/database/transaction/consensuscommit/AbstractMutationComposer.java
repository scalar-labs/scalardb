package com.scalar.database.transaction.consensuscommit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.scalar.database.api.Mutation;
import com.scalar.database.api.Operation;
import com.scalar.database.api.Scan;
import com.scalar.database.io.Key;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** An abstraction for a mutation composer */
public abstract class AbstractMutationComposer implements MutationComposer {
  protected final String id;
  protected final List<Mutation> mutations;
  protected final long current;

  /**
   * Constructs an {@code AbstractMutationComposer} with the specified {@code id}
   *
   * @param id a {@code String}
   */
  public AbstractMutationComposer(String id) {
    this.id = id;
    this.mutations = new ArrayList<>();
    this.current = System.currentTimeMillis();
  }

  @VisibleForTesting
  AbstractMutationComposer(String id, List<Mutation> mutations, long current) {
    this.id = id;
    this.mutations = mutations;
    this.current = current;
  }

  /**
   * Returns an immutable list of all the {@link Mutation}s
   *
   * @return an immutable list of all the {@link Mutation}s
   */
  @Override
  public List<Mutation> get() {
    return ImmutableList.copyOf(mutations);
  }

  /**
   * Returns the clustering {@link Key}
   *
   * @param base an {@link Operation}
   * @param result a {@link TransactionResult}
   * @return an {@link Optional} with the clustering {@link Key}
   */
  protected Optional<Key> getClusteringKey(Operation base, TransactionResult result) {
    if (base instanceof Scan) {
      return result.getClusteringKey();
    } else {
      return base.getClusteringKey();
    }
  }
}
