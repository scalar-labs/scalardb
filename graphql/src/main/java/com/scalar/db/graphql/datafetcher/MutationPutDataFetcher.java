package com.scalar.db.graphql.datafetcher;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Put;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CrudException;
import graphql.execution.DataFetcherResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MutationPutDataFetcher implements DataFetcher<DataFetcherResult<Boolean>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MutationPutDataFetcher.class);
  private final DistributedStorage storage;
  private final DataFetcherHelper helper;

  public MutationPutDataFetcher(DistributedStorage storage, DataFetcherHelper helper) {
    this.storage = storage;
    this.helper = helper;
  }

  @Override
  public DataFetcherResult<Boolean> get(DataFetchingEnvironment environment) {
    Map<String, Object> putInput = environment.getArgument("put");
    LOGGER.debug("got put argument: {}", putInput);
    Put put = helper.createPut(putInput);

    DataFetcherResult.Builder<Boolean> result = DataFetcherResult.newResult();
    try {
      performPut(environment, put);
      result.data(true);
    } catch (CrudException | ExecutionException e) {
      LOGGER.warn("Scalar DB put operation failed", e);
      result.data(false).error(DataFetcherHelper.getGraphQLError(e, environment));
    }

    return result.build();
  }

  @VisibleForTesting
  void performPut(DataFetchingEnvironment environment, Put put)
      throws CrudException, ExecutionException {
    DistributedTransaction transaction = DataFetcherHelper.getCurrentTransaction(environment);
    if (transaction != null) {
      LOGGER.debug("running Put operation with transaction: {}", put);
      transaction.put(put);
    } else {
      LOGGER.debug("running Put operation with storage: {}", put);
      storage.put(put);
    }
  }
}
