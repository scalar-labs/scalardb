package com.scalar.db.graphql.datafetcher;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.graphql.schema.Constants;
import graphql.GraphQLContext;
import graphql.execution.AbortExecutionException;
import graphql.execution.DataFetcherResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MutationAbortDataFetcher implements DataFetcher<DataFetcherResult<Boolean>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MutationAbortDataFetcher.class);

  @Override
  public DataFetcherResult<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    GraphQLContext graphQLContext = environment.getGraphQlContext();
    DistributedTransaction transaction = graphQLContext.get(Constants.CONTEXT_TRANSACTION_KEY);
    if (transaction == null) {
      LOGGER.warn("got abort mutation, but transaction was not found");
      return DataFetcherResult.<Boolean>newResult()
          .data(false)
          .error(new AbortExecutionException("the transaction to abort is not found"))
          .build();
    }

    LOGGER.debug("aborting transaction {}", transaction.getId());
    DataFetcherResult.Builder<Boolean> result = DataFetcherResult.newResult();
    try {
      transaction.abort();
      result.data(true);
    } catch (AbortException e) {
      LOGGER.warn("abort failed", e);
      result.data(false).error(new AbortExecutionException(e));
    }

    return result.build();
  }
}
