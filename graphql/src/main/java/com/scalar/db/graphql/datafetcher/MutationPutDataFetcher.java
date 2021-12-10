package com.scalar.db.graphql.datafetcher;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Put;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.TransactionException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Map;

public class MutationPutDataFetcher implements DataFetcher<Boolean> {
  private final DistributedStorage storage;
  private final DataFetcherHelper helper;

  public MutationPutDataFetcher(DistributedStorage storage, DataFetcherHelper helper) {
    this.storage = storage;
    this.helper = helper;
  }

  @Override
  public Boolean get(DataFetchingEnvironment environment) throws Exception {
    Map<String, Object> putInput = environment.getArgument("put");
    performPut(environment, helper.createPut(putInput));

    return true;
  }

  @VisibleForTesting
  void performPut(DataFetchingEnvironment environment, Put put)
      throws TransactionException, ExecutionException {
    DistributedTransaction transaction = helper.getTransactionIfEnabled(environment);
    if (transaction != null) {
      transaction.put(put);
    } else {
      storage.put(put);
    }
  }
}
