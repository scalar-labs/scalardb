package com.scalar.db.graphql.datafetcher;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Put;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CrudException;
import graphql.VisibleForTesting;
import graphql.execution.DataFetcherResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MutationBulkPutDataFetcher implements DataFetcher<DataFetcherResult<Boolean>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MutationBulkPutDataFetcher.class);
  private final DistributedStorage storage;
  private final DataFetcherHelper helper;

  public MutationBulkPutDataFetcher(DistributedStorage storage, DataFetcherHelper helper) {
    this.storage = storage;
    this.helper = helper;
  }

  @Override
  public DataFetcherResult<Boolean> get(DataFetchingEnvironment environment) {
    List<Map<String, Object>> putInput = environment.getArgument("put");
    LOGGER.debug("got put argument: {}", putInput);
    List<Put> puts = putInput.stream().map(helper::createPut).collect(Collectors.toList());

    DataFetcherResult.Builder<Boolean> result = DataFetcherResult.newResult();
    try {
      performPut(environment, puts);
      result.data(true);
    } catch (CrudException | ExecutionException e) {
      LOGGER.warn("Scalar DB put operation failed", e);
      result.data(false).error(DataFetcherHelper.getGraphQLError(e, environment));
    }

    return result.build();
  }

  @VisibleForTesting
  void performPut(DataFetchingEnvironment environment, List<Put> puts)
      throws CrudException, ExecutionException {
    DistributedTransaction transaction = DataFetcherHelper.getCurrentTransaction(environment);
    if (transaction != null) {
      LOGGER.debug("running Put operations with transaction: {}", puts);
      transaction.put(puts);
    } else {
      LOGGER.debug("running Put operations with storage: {}", puts);
      storage.put(puts);
    }
  }
}
