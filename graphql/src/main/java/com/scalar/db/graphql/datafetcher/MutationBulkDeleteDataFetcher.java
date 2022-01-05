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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MutationBulkDeleteDataFetcher implements DataFetcher<DataFetcherResult<Boolean>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MutationBulkDeleteDataFetcher.class);
  private final DistributedStorage storage;
  private final DataFetcherHelper helper;

  public MutationBulkDeleteDataFetcher(DistributedStorage storage, DataFetcherHelper helper) {
    this.storage = storage;
    this.helper = helper;
  }

  @Override
  public DataFetcherResult<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    List<Map<String, Object>> deleteInput = environment.getArgument("delete");
    LOGGER.debug("got delete argument: " + deleteInput);
    List<Delete> deletes =
        deleteInput.stream().map(helper::createDelete).collect(Collectors.toList());

    DataFetcherResult.Builder<Boolean> result = DataFetcherResult.newResult();
    try {
      performDelete(environment, deletes);
      result.data(true);
    } catch (TransactionException | ExecutionException e) {
      LOGGER.warn("Scalar DB delete operation failed", e);
      result.data(false).error(new AbortExecutionException(e));
    }

    return result.build();
  }

  @VisibleForTesting
  void performDelete(DataFetchingEnvironment environment, List<Delete> deletes)
      throws TransactionException, ExecutionException {
    DistributedTransaction transaction = DataFetcherHelper.getCurrentTransaction(environment);
    if (transaction != null) {
      LOGGER.debug("running Delete operations with transaction: " + deletes);
      transaction.delete(deletes);
    } else {
      LOGGER.debug("running Delete operations with storage: " + deletes);
      storage.delete(deletes);
    }
  }
}
