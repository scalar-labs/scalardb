package com.scalar.db.graphql.datafetcher;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.graphql.schema.Constants;
import graphql.GraphQLContext;
import graphql.execution.AbortExecutionException;
import graphql.execution.DataFetcherResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;

public class MutationAbortDataFetcher implements DataFetcher<DataFetcherResult<Boolean>> {
  @Override
  public DataFetcherResult<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    GraphQLContext graphQLContext = environment.getGraphQlContext();
    DistributedTransaction transaction = graphQLContext.get(Constants.CONTEXT_TRANSACTION_KEY);
    if (transaction == null) {
      return DataFetcherResult.<Boolean>newResult()
          .data(false)
          .error(new AbortExecutionException("Transaction not found."))
          .build();
    }
    graphQLContext.delete(Constants.CONTEXT_TRANSACTION_KEY);

    DataFetcherResult.Builder<Boolean> result = DataFetcherResult.newResult();
    try {
      transaction.abort();
      result.data(true);
    } catch (AbortException e) {
      result.data(false).error(new AbortExecutionException(e));
    }

    return result.build();
  }
}
