package com.scalar.db.transaction.consensuscommit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.scalar.db.api.Mutation;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public abstract class AbstractMutationComposer implements MutationComposer {
  protected final String id;
  protected final List<Mutation> mutations;
  protected final long current;
  protected final TransactionTableMetadataManager tableMetadataManager;

  public AbstractMutationComposer(String id, TransactionTableMetadataManager tableMetadataManager) {
    this.id = id;
    this.mutations = new ArrayList<>();
    this.current = System.currentTimeMillis();
    this.tableMetadataManager = tableMetadataManager;
  }

  @VisibleForTesting
  AbstractMutationComposer(
      String id, long current, TransactionTableMetadataManager tableMetadataManager) {
    this.id = id;
    this.mutations = new ArrayList<>();
    this.current = current;
    this.tableMetadataManager = tableMetadataManager;
  }

  @Override
  public List<Mutation> get() {
    return ImmutableList.copyOf(mutations);
  }
}
