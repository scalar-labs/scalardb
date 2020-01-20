package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import java.util.List;

public interface MutationComposer {

  void add(Operation base, TransactionResult result);

  List<Mutation> get();
}
