package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.List;

public interface MutationComposer {

  void add(Operation base, TransactionResult result) throws ExecutionException;

  List<Mutation> get();
}
