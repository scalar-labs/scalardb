package com.scalar.db.graphql.datafetcher;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.TransactionException;
import graphql.VisibleForTesting;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MutationBulkDeleteDataFetcher implements DataFetcher<Boolean> {
  private final DistributedStorage storage;
  private final DataFetcherHelper helper;

  public MutationBulkDeleteDataFetcher(DistributedStorage storage, DataFetcherHelper helper) {
    this.storage = storage;
    this.helper = helper;
  }

  @Override
  public Boolean get(DataFetchingEnvironment environment) throws Exception {
    List<Map<String, Object>> deleteInput = environment.getArgument("delete");
    List<Delete> deletes =
        deleteInput.stream().map(helper::createDelete).collect(Collectors.toList());

    performDelete(environment, deletes);

    return true;
  }

  @VisibleForTesting
  void performDelete(DataFetchingEnvironment environment, List<Delete> deletes)
      throws TransactionException, ExecutionException {
    DistributedTransaction transaction = helper.getTransactionIfEnabled(environment);
    if (transaction != null) {
      transaction.delete(deletes);
    } else {
      storage.delete(deletes);
    }
  }
}
