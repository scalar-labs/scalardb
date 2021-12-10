package com.scalar.db.graphql.datafetcher;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Mutation;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.TransactionException;
import graphql.VisibleForTesting;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MutationMutateDataFetcher implements DataFetcher<Boolean> {
  private final DistributedStorage storage;
  private final DataFetcherHelper helper;

  public MutationMutateDataFetcher(DistributedStorage storage, DataFetcherHelper helper) {
    this.storage = storage;
    this.helper = helper;
  }

  @Override
  public Boolean get(DataFetchingEnvironment environment) throws Exception {
    List<Map<String, Object>> putInput = environment.getArgument("put");
    List<Map<String, Object>> deleteInput = environment.getArgument("delete");
    List<Mutation> mutations = new ArrayList<>();
    if (putInput != null) {
      mutations.addAll(putInput.stream().map(helper::createPut).collect(Collectors.toList()));
    }
    if (deleteInput != null) {
      mutations.addAll(deleteInput.stream().map(helper::createDelete).collect(Collectors.toList()));
    }

    performMutate(environment, mutations);

    return true;
  }

  @VisibleForTesting
  void performMutate(DataFetchingEnvironment environment, List<Mutation> mutations)
      throws TransactionException, ExecutionException {
    DistributedTransaction transaction = helper.getTransactionIfEnabled(environment);
    if (transaction != null) {
      transaction.mutate(mutations);
    } else {
      storage.mutate(mutations);
    }
  }
}
