package com.scalar.db.graphql.datafetcher;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.graphql.schema.Constants;
import graphql.GraphQLContext;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;

public class MutationCommitDataFetcher implements DataFetcher<Boolean> {
  @Override
  public Boolean get(DataFetchingEnvironment environment) throws Exception {
    GraphQLContext graphQLContext = environment.getGraphQlContext();
    DistributedTransaction transaction = graphQLContext.get(Constants.CONTEXT_TRANSACTION_KEY);
    if (transaction == null) {
      throw new IllegalStateException("Transaction not found.");
    }
    graphQLContext.delete(Constants.CONTEXT_TRANSACTION_KEY);
    transaction.commit();
    return true;
  }
}
