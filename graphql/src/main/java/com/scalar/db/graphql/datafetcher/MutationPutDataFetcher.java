package com.scalar.db.graphql.datafetcher;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Put;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.TransactionException;
import graphql.execution.AbortExecutionException;
import graphql.execution.DataFetcherResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Map;

public class MutationPutDataFetcher implements DataFetcher<DataFetcherResult<Boolean>> {
  private final DistributedStorage storage;
  private final DataFetcherHelper helper;

  public MutationPutDataFetcher(DistributedStorage storage, DataFetcherHelper helper) {
    this.storage = storage;
    this.helper = helper;
  }

  @Override
  public DataFetcherResult<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    Map<String, Object> putInput = environment.getArgument("put");
    Put put = helper.createPut(putInput);

    DataFetcherResult.Builder<Boolean> result = DataFetcherResult.newResult();
    try {
      performPut(environment, put);
      result.data(true);
    } catch (TransactionException | ExecutionException e) {
      result.data(false).error(new AbortExecutionException(e));
    }

    return result.build();
  }

  @VisibleForTesting
  void performPut(DataFetchingEnvironment environment, Put put)
      throws TransactionException, ExecutionException {
    DistributedTransaction transaction = DataFetcherHelper.getCurrentTransaction(environment);
    if (transaction != null) {
      transaction.put(put);
    } else {
      storage.put(put);
    }
  }
}
