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

public abstract class AbstractMutationComposer implements MutationComposer {
  protected final String id;
  protected final List<Mutation> mutations;
  protected final long current;

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

  @Override
  public List<Mutation> get() {
    return ImmutableList.copyOf(mutations);
  }

  protected Optional<Key> getClusteringKey(Operation base, TransactionResult result) {
    if (base instanceof Scan) {
      return result.getClusteringKey();
    } else {
      return base.getClusteringKey();
    }
  }
}
