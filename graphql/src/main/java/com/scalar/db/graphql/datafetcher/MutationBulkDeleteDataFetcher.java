package com.scalar.db.graphql.datafetcher;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.TransactionException;
import graphql.VisibleForTesting;
import graphql.execution.AbortExecutionException;
import graphql.execution.DataFetcherResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MutationBulkDeleteDataFetcher implements DataFetcher<DataFetcherResult<Boolean>> {
  private final DistributedStorage storage;
  private final DataFetcherHelper helper;

  public MutationBulkDeleteDataFetcher(DistributedStorage storage, DataFetcherHelper helper) {
    this.storage = storage;
    this.helper = helper;
  }

  @Override
  public DataFetcherResult<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    List<Map<String, Object>> deleteInput = environment.getArgument("delete");
    List<Delete> deletes =
        deleteInput.stream().map(helper::createDelete).collect(Collectors.toList());

    DataFetcherResult.Builder<Boolean> result = DataFetcherResult.newResult();
    try {
      performDelete(environment, deletes);
      result.data(true);
    } catch (TransactionException | ExecutionException e) {
      result.data(false).error(new AbortExecutionException(e));
    }

    return result.build();
  }

  @VisibleForTesting
  void performDelete(DataFetchingEnvironment environment, List<Delete> deletes)
      throws TransactionException, ExecutionException {
    DistributedTransaction transaction = DataFetcherHelper.getCurrentTransaction(environment);
    if (transaction != null) {
      transaction.delete(deletes);
    } else {
      storage.delete(deletes);
    }
  }
}
