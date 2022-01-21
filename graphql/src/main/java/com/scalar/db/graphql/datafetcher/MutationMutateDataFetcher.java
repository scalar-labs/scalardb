package com.scalar.db.graphql.datafetcher;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Mutation;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CrudException;
import graphql.VisibleForTesting;
import graphql.execution.DataFetcherResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MutationMutateDataFetcher implements DataFetcher<DataFetcherResult<Boolean>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MutationMutateDataFetcher.class);
  private final DistributedStorage storage;
  private final DataFetcherHelper helper;

  public MutationMutateDataFetcher(DistributedStorage storage, DataFetcherHelper helper) {
    this.storage = storage;
    this.helper = helper;
  }

  @Override
  public DataFetcherResult<Boolean> get(DataFetchingEnvironment environment) {
    List<Map<String, Object>> putInput = environment.getArgument("put");
    List<Map<String, Object>> deleteInput = environment.getArgument("delete");
    List<Mutation> mutations = new ArrayList<>();
    if (putInput != null) {
      LOGGER.debug("got put argument: {}", putInput);
      mutations.addAll(putInput.stream().map(helper::createPut).collect(Collectors.toList()));
    }
    if (deleteInput != null) {
      LOGGER.debug("got delete argument: {}", deleteInput);
      mutations.addAll(deleteInput.stream().map(helper::createDelete).collect(Collectors.toList()));
    }

    DataFetcherResult.Builder<Boolean> result = DataFetcherResult.newResult();
    try {
      performMutate(environment, mutations);
      result.data(true);
    } catch (CrudException | ExecutionException e) {
      LOGGER.warn("Scalar DB mutate operation failed", e);
      result.data(false).error(DataFetcherHelper.getGraphQLError(e, environment));
    }

    return result.build();
  }

  @VisibleForTesting
  void performMutate(DataFetchingEnvironment environment, List<Mutation> mutations)
      throws CrudException, ExecutionException {
    DistributedTransaction transaction = DataFetcherHelper.getCurrentTransaction(environment);
    if (transaction != null) {
      LOGGER.debug("running Mutation operations with transaction: {}", mutations);
      transaction.mutate(mutations);
    } else {
      LOGGER.debug("running Mutation operations with storage: {}", mutations);
      helper.failIfConsensusCommitTransactionalTable();
      storage.mutate(mutations);
    }
  }
}
