package com.scalar.db.graphql.datafetcher;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CrudException;
import graphql.execution.DataFetcherResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MutationDeleteDataFetcher implements DataFetcher<DataFetcherResult<Boolean>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MutationDeleteDataFetcher.class);
  private final DistributedStorage storage;
  private final DataFetcherHelper helper;

  public MutationDeleteDataFetcher(DistributedStorage storage, DataFetcherHelper helper) {
    this.storage = storage;
    this.helper = helper;
  }

  @Override
  public DataFetcherResult<Boolean> get(DataFetchingEnvironment environment) {
    Map<String, Object> deleteInput = environment.getArgument("delete");
    LOGGER.debug("got delete argument: {}", deleteInput);
    Delete delete = helper.createDelete(deleteInput);

    DataFetcherResult.Builder<Boolean> result = DataFetcherResult.newResult();
    try {
      performDelete(environment, delete);
      result.data(true);
    } catch (CrudException | ExecutionException e) {
      LOGGER.warn("Scalar DB delete operation failed", e);
      result.data(false).error(DataFetcherHelper.getGraphQLError(e, environment));
    }

    return result.build();
  }

  @VisibleForTesting
  void performDelete(DataFetchingEnvironment environment, Delete delete)
      throws CrudException, ExecutionException {
    DistributedTransaction transaction = DataFetcherHelper.getCurrentTransaction(environment);
    if (transaction != null) {
      LOGGER.debug("running Delete operation with transaction: {}", delete);
      transaction.delete(delete);
    } else {
      LOGGER.debug("running Delete operation with storage: {}", delete);
      helper.failIfConsensusCommitTransactionalTable();
      storage.delete(delete);
    }
  }
}
