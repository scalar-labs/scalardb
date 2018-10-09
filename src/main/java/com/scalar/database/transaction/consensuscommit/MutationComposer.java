package com.scalar.database.transaction.consensuscommit;

import com.scalar.database.api.Mutation;
import com.scalar.database.api.Operation;
import java.util.List;

public interface MutationComposer {

  void add(Operation base, TransactionResult result);

  List<Mutation> get();
}
