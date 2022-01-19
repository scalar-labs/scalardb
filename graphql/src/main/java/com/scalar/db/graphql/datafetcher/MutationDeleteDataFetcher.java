package com.scalar.db.graphql.datafetcher;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.TransactionException;
import graphql.execution.AbortExecutionException;
import graphql.execution.DataFetcherResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Map;

public class MutationDeleteDataFetcher implements DataFetcher<DataFetcherResult<Boolean>> {
  private final DistributedStorage storage;
  private final DataFetcherHelper helper;

  public MutationDeleteDataFetcher(DistributedStorage storage, DataFetcherHelper helper) {
    this.storage = storage;
    this.helper = helper;
  }

  @Override
  public DataFetcherResult<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    Map<String, Object> deleteInput = environment.getArgument("delete");
    Delete delete = helper.createDelete(deleteInput);

    DataFetcherResult.Builder<Boolean> result = DataFetcherResult.newResult();
    try {
      performDelete(environment, delete);
      result.data(true);
    } catch (TransactionException | ExecutionException e) {
      result.data(false).error(new AbortExecutionException(e));
    }

    return result.build();
  }

  @VisibleForTesting
  void performDelete(DataFetchingEnvironment environment, Delete delete)
      throws TransactionException, ExecutionException {
    DistributedTransaction transaction = DataFetcherHelper.getCurrentTransaction(environment);
    if (transaction != null) {
      transaction.delete(delete);
    } else {
      storage.delete(delete);
    }
  }
}
