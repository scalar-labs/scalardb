package com.scalar.db.graphql.datafetcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.graphql.schema.Constants;
import graphql.GraphQLContext;
import graphql.GraphQLError;
import graphql.Scalars;
import graphql.execution.DataFetcherResult;
import graphql.execution.ExecutionStepInfo;
import graphql.execution.ResultPath;
import graphql.language.Field;
import graphql.language.SourceLocation;
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
    // Stub environment methods for errors
    when(environment.getField())
        .thenReturn(Field.newField("test").sourceLocation(SourceLocation.EMPTY).build());
    when(environment.getExecutionStepInfo())
        .thenReturn(
            ExecutionStepInfo.newExecutionStepInfo()
                .type(Scalars.GraphQLString)
                .path(ResultPath.parse(""))
                .build());

    doSetUp();
  }

  protected abstract void doSetUp() throws Exception;

  protected void setTransactionStarted() {
    when(graphQlContext.get(Constants.CONTEXT_TRANSACTION_KEY)).thenReturn(transaction);
  }

  protected void assertThatDataFetcherResultHasErrorForException(
      DataFetcherResult<?> result, Exception exception) {
    String exName = exception.getClass().getSimpleName();
    GraphQLError error =
        result.getErrors().stream()
            .filter(
                e ->
                    exName.equals(e.getExtensions().get(Constants.ERRORS_EXTENSIONS_EXCEPTION_KEY)))
            .findFirst()
            .orElse(null);
    assertThat(error).isNotNull();
    assertThat(error.getMessage()).contains(exception.getMessage());
    assertThat(error.getMessage()).contains(exName);
  }
}
