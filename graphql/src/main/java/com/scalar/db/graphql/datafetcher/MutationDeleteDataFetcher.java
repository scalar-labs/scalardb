package com.scalar.db.graphql.datafetcher;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.TransactionException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Map;

public class MutationDeleteDataFetcher implements DataFetcher<Boolean> {
  private final DistributedStorage storage;
  private final DataFetcherHelper helper;

  public MutationDeleteDataFetcher(DistributedStorage storage, DataFetcherHelper helper) {
    this.storage = storage;
    this.helper = helper;
  }

  @Override
  public Boolean get(DataFetchingEnvironment environment) throws Exception {
    Map<String, Object> deleteInput = environment.getArgument("delete");
    performDelete(environment, helper.createDelete(deleteInput));

    return true;
  }

  @VisibleForTesting
  void performDelete(DataFetchingEnvironment environment, Delete delete)
      throws TransactionException, ExecutionException {
    DistributedTransaction transaction = helper.getTransactionIfEnabled(environment);
    if (transaction != null) {
      transaction.delete(delete);
    } else {
      storage.delete(delete);
    }
  }
}
