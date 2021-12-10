package com.scalar.db.graphql.datafetcher;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Put;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.TransactionException;
import graphql.VisibleForTesting;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MutationBulkPutDataFetcher implements DataFetcher<Boolean> {
  private final DistributedStorage storage;
  private final DataFetcherHelper helper;

  public MutationBulkPutDataFetcher(DistributedStorage storage, DataFetcherHelper helper) {
    this.storage = storage;
    this.helper = helper;
  }

  @Override
  public Boolean get(DataFetchingEnvironment environment) throws Exception {
    List<Map<String, Object>> putInput = environment.getArgument("put");
    List<Put> puts = putInput.stream().map(helper::createPut).collect(Collectors.toList());

    performPut(environment, puts);

    return true;
  }

  @VisibleForTesting
  void performPut(DataFetchingEnvironment environment, List<Put> puts)
      throws TransactionException, ExecutionException {
    DistributedTransaction transaction = helper.getTransactionIfEnabled(environment);
    if (transaction != null) {
      transaction.put(puts);
    } else {
      storage.put(puts);
    }
  }
}
