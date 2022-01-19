package com.scalar.db.graphql.datafetcher;

import static org.mockito.Mockito.when;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.graphql.schema.Constants;
import graphql.GraphQLContext;
import graphql.schema.DataFetchingEnvironment;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public abstract class DataFetcherTestBase {
  protected static final String ANY_NAMESPACE = "namespace1";
  protected static final String ANY_TABLE = "table1";

  @Mock protected DataFetchingEnvironment environment;
  @Mock protected GraphQLContext graphQlContext;
  @Mock protected DistributedStorage storage;
  @Mock protected DistributedTransaction transaction;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    when(environment.getGraphQlContext()).thenReturn(graphQlContext);

    doSetUp();
  }

  protected abstract void doSetUp() throws Exception;

  protected void setTransactionStarted() {
    when(graphQlContext.get(Constants.CONTEXT_TRANSACTION_KEY)).thenReturn(transaction);
  }
}
