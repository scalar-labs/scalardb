package com.scalar.db.graphql.datafetcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.graphql.GraphQlConstants;
import graphql.GraphQLContext;
import graphql.GraphQLError;
import graphql.Scalars;
import graphql.execution.DataFetcherResult;
import graphql.execution.ExecutionStepInfo;
import graphql.execution.ResultPath;
import graphql.language.Field;
import graphql.language.SourceLocation;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.DataFetchingFieldSelectionSet;
import graphql.schema.SelectedField;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public abstract class DataFetcherTestBase {
  protected static final String ANY_NAMESPACE = "namespace1";
  protected static final String ANY_TABLE = "table1";

  @Mock protected DataFetchingEnvironment dataFetchingEnvironment;
  @Mock protected DataFetchingFieldSelectionSet selectionSet;
  @Mock protected GraphQLContext graphQlContext;
  @Mock protected DistributedStorage storage;
  @Mock protected DistributedTransaction transaction;

  @Before
  public void setUpBase() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    when(dataFetchingEnvironment.getGraphQlContext()).thenReturn(graphQlContext);

    // Set empty selection set as default
    when(selectionSet.getFields(anyString())).thenReturn(Collections.emptyList());
    when(dataFetchingEnvironment.getSelectionSet()).thenReturn(selectionSet);

    // Stub environment methods for errors
    when(dataFetchingEnvironment.getField())
        .thenReturn(Field.newField("test").sourceLocation(SourceLocation.EMPTY).build());
    when(dataFetchingEnvironment.getExecutionStepInfo())
        .thenReturn(
            ExecutionStepInfo.newExecutionStepInfo()
                .type(Scalars.GraphQLString)
                .path(ResultPath.parse(""))
                .build());
  }

  protected void addSelectionSetToEnvironment(
      DataFetchingEnvironment environment, String... fields) {
    List<SelectedField> selectedFieldList =
        Arrays.stream(fields)
            .map(
                field -> {
                  SelectedField selectedField = mock(SelectedField.class);
                  when(selectedField.getName()).thenReturn(field);
                  return selectedField;
                })
            .collect(Collectors.toList());
    when(selectionSet.getFields(anyString())).thenReturn(selectedFieldList);
    when(environment.getSelectionSet()).thenReturn(selectionSet);
  }

  protected void setTransactionStarted() {
    when(graphQlContext.get(GraphQlConstants.CONTEXT_TRANSACTION_KEY)).thenReturn(transaction);
  }

  protected void assertThatDataFetcherResultHasErrorForException(
      DataFetcherResult<?> result, Exception exception) {
    String exName = exception.getClass().getSimpleName();
    GraphQLError error =
        result.getErrors().stream()
            .filter(e -> exName.equals(e.getExtensions().get("exception")))
            .findFirst()
            .orElse(null);
    assertThat(error).isNotNull();
    assertThat(error.getMessage()).contains(exception.getMessage());
    assertThat(error.getMessage()).contains(exName);
  }
}
