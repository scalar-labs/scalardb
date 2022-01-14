package com.scalar.db.graphql.datafetcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.graphql.schema.Constants;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.GraphQLContext;
import graphql.GraphQLError;
import graphql.execution.AbortExecutionException;
import graphql.execution.ExecutionContext;
import graphql.execution.instrumentation.InstrumentationContext;
import graphql.execution.instrumentation.SimpleInstrumentationContext;
import graphql.execution.instrumentation.parameters.InstrumentationExecuteOperationParameters;
import graphql.execution.instrumentation.parameters.InstrumentationExecutionParameters;
import graphql.language.Argument;
import graphql.language.BooleanValue;
import graphql.language.Directive;
import graphql.language.OperationDefinition;
import graphql.language.StringValue;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TransactionInstrumentationTest {
  private static final String ANY_TX_ID = UUID.randomUUID().toString();

  @Mock private ExecutionContext executionContext;
  @Mock private OperationDefinition operationDefinition;
  @Mock private DistributedTransactionManager transactionManager;
  @Mock private DistributedTransaction transaction;
  @Mock private ExecutionResult executionResult;
  @Mock private InstrumentationExecuteOperationParameters instrumentationExecuteOperationParameters;
  @Mock private InstrumentationExecutionParameters instrumentationExecutionParameters;
  private GraphQLContext graphQlContext;
  private TransactionInstrumentation instrumentation;
  private ExecutionResultCaptor executionResultCaptor;
  private GraphQL graphQl;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    graphQlContext = spy(new GraphQLContext.Builder().build());
    when(transaction.getId()).thenReturn(ANY_TX_ID);
    when(transactionManager.start()).thenReturn(transaction);

    when(executionContext.getGraphQLContext()).thenReturn(graphQlContext);
    when(executionContext.getOperationDefinition()).thenReturn(operationDefinition);

    instrumentation = spy(new TransactionInstrumentation(transactionManager));
  }

  private void prepareDirective(Directive transactionDirective) {
    when(operationDefinition.getDirectives(Constants.TRANSACTION_DIRECTIVE_NAME))
        .thenReturn(Collections.singletonList(transactionDirective));
  }

  private void prepareForBeginExecuteOperationTests() {
    when(instrumentationExecuteOperationParameters.getExecutionContext())
        .thenReturn(executionContext);
  }

  @Test
  public void beginExecuteOperation_DirectiveWithNoArgumentGiven_ShouldStartTransaction()
      throws Exception {
    // Arrange
    prepareForBeginExecuteOperationTests();
    prepareDirective(new Directive(Constants.TRANSACTION_DIRECTIVE_NAME));

    // Act
    InstrumentationContext<ExecutionResult> context =
        instrumentation.beginExecuteOperation(instrumentationExecuteOperationParameters);

    // Assert
    verify(transactionManager, times(1)).start();
    verify(graphQlContext).put(Constants.CONTEXT_TRANSACTION_KEY, transaction);
    assertThat(context).isInstanceOf(SimpleInstrumentationContext.class);
  }

  @Test
  public void
      beginExecuteOperation_DirectiveWithNoTxIdAndCommitTrueGiven_ShouldStartTransactionAndCommit()
          throws Exception {
    // Arrange
    prepareForBeginExecuteOperationTests();
    prepareDirective(
        Directive.newDirective()
            .name(Constants.TRANSACTION_DIRECTIVE_NAME)
            .argument(
                new Argument(
                    Constants.TRANSACTION_DIRECTIVE_COMMIT_ARGUMENT_NAME, new BooleanValue(true)))
            .build());

    // Act
    InstrumentationContext<ExecutionResult> context =
        instrumentation.beginExecuteOperation(instrumentationExecuteOperationParameters);

    // Assert
    verify(transactionManager, times(1)).start();
    verify(graphQlContext).put(Constants.CONTEXT_TRANSACTION_KEY, transaction);
    assertThat(context).isInstanceOf(SimpleInstrumentationContext.class);
  }

  @Test
  public void beginExecuteOperation_TransactionFailsToStart_ShouldThrowAbortExecutionException()
      throws Exception {
    // Arrange
    prepareForBeginExecuteOperationTests();
    prepareDirective(new Directive(Constants.TRANSACTION_DIRECTIVE_NAME));
    when(transactionManager.start()).thenThrow(TransactionException.class);

    // Act Assert
    assertThatThrownBy(
            () -> instrumentation.beginExecuteOperation(instrumentationExecuteOperationParameters))
        .isInstanceOf(AbortExecutionException.class);
    verify(transactionManager, times(1)).start();
    verify(graphQlContext, never()).put(any(), any());
  }

  @Test
  public void beginExecuteOperation_DirectiveWithTxIdGiven_ShouldLoadExistingTransaction()
      throws Exception {
    // Arrange
    prepareForBeginExecuteOperationTests();
    prepareDirective(
        Directive.newDirective()
            .name(Constants.TRANSACTION_DIRECTIVE_NAME)
            .argument(
                new Argument(
                    Constants.TRANSACTION_DIRECTIVE_TX_ID_ARGUMENT_NAME,
                    new StringValue(ANY_TX_ID)))
            .build());
    instrumentation.activeTransactions.put(ANY_TX_ID, transaction);

    // Act
    InstrumentationContext<ExecutionResult> context =
        instrumentation.beginExecuteOperation(instrumentationExecuteOperationParameters);

    // Assert
    verify(transactionManager, never()).start();
    verify(graphQlContext).put(Constants.CONTEXT_TRANSACTION_KEY, transaction);
    assertThat(context).isInstanceOf(SimpleInstrumentationContext.class);
  }

  @Test
  public void
      beginExecuteOperation_DirectiveWithTxIdAndCommitTrueGiven_ShouldLoadExistingTransactionAndCommit()
          throws Exception {
    // Arrange
    prepareForBeginExecuteOperationTests();
    prepareDirective(
        Directive.newDirective()
            .name(Constants.TRANSACTION_DIRECTIVE_NAME)
            .argument(
                new Argument(
                    Constants.TRANSACTION_DIRECTIVE_TX_ID_ARGUMENT_NAME,
                    new StringValue(ANY_TX_ID)))
            .argument(
                new Argument(
                    Constants.TRANSACTION_DIRECTIVE_COMMIT_ARGUMENT_NAME, new BooleanValue(true)))
            .build());
    instrumentation.activeTransactions.put(ANY_TX_ID, transaction);

    // Act
    InstrumentationContext<ExecutionResult> context =
        instrumentation.beginExecuteOperation(instrumentationExecuteOperationParameters);

    // Assert
    verify(transactionManager, never()).start();
    verify(graphQlContext).put(Constants.CONTEXT_TRANSACTION_KEY, transaction);
    assertThat(context).isInstanceOf(SimpleInstrumentationContext.class);
  }

  @Test
  public void beginExecuteOperation_TransactionIsNotFound_ShouldThrowAbortExecutionException()
      throws TransactionException {
    // Arrange
    prepareForBeginExecuteOperationTests();
    prepareDirective(
        Directive.newDirective()
            .name(Constants.TRANSACTION_DIRECTIVE_NAME)
            .argument(
                new Argument(
                    Constants.TRANSACTION_DIRECTIVE_TX_ID_ARGUMENT_NAME,
                    new StringValue(ANY_TX_ID)))
            .build());

    // Act Assert
    assertThatThrownBy(
            () -> instrumentation.beginExecuteOperation(instrumentationExecuteOperationParameters))
        .isInstanceOf(AbortExecutionException.class);
    verify(transactionManager, never()).start();
    verify(graphQlContext, never()).put(any(), any());
  }

  private void prepareForInstrumentExecutionResultTests() {
    when(instrumentationExecutionParameters.getGraphQLContext()).thenReturn(graphQlContext);
  }

  @Test
  public void instrumentExecutionResult_TransactionNotFound_ShouldReturnTheSameExecutionResult()
      throws Exception {
    // Arrange
    prepareForInstrumentExecutionResultTests();

    // Act
    CompletableFuture<ExecutionResult> future =
        instrumentation.instrumentExecutionResult(
            executionResult, instrumentationExecutionParameters);

    // Assert
    assertThat(future.get()).isSameAs(executionResult);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void instrumentExecutionResult_TransactionFound_ShouldAddExtensionToExecutionResult()
      throws Exception {
    // Arrange
    prepareForInstrumentExecutionResultTests();
    graphQlContext.put(Constants.CONTEXT_TRANSACTION_KEY, transaction);
    Object originalData = ImmutableMap.of("data", 1);
    List<GraphQLError> originalErrors = ImmutableList.of(new AbortExecutionException());
    Map<Object, Object> originalExt = ImmutableMap.of("ext1", 1, "ext2", 2);
    when(executionResult.getData()).thenReturn(originalData);
    when(executionResult.getErrors()).thenReturn(originalErrors);
    when(executionResult.getExtensions()).thenReturn(originalExt);

    // Act
    CompletableFuture<ExecutionResult> future =
        instrumentation.instrumentExecutionResult(
            executionResult, instrumentationExecutionParameters);
    ExecutionResult instrumentedExecutionResult = future.get();

    // Assert
    assertThat(instrumentedExecutionResult.<Map<Object, Object>>getData()).isEqualTo(originalData);
    assertThat(instrumentedExecutionResult.getErrors()).isEqualTo(originalErrors);
    Map<Object, Object> ext = instrumentedExecutionResult.getExtensions();
    assertThat(ext).containsAllEntriesOf(originalExt);
    assertThat(ext).containsKey("transaction");
    assertThat((Map<Object, Object>) ext.get("transaction"))
        .containsOnly(entry("txId", transaction.getId()));
  }

  private void prepareForInstrumentationContextTests() {
    // In tests, we create an executable schema with an SDL-first approach
    String helloSchema =
        "directive @transaction(commit: Boolean, txId: String) on QUERY | MUTATION\n"
            + "type Query { hello: String }";
    TypeDefinitionRegistry typeRegistry = new SchemaParser().parse(helloSchema);
    RuntimeWiring wiring =
        RuntimeWiring.newRuntimeWiring()
            .type("Query", typeWiring -> typeWiring.dataFetcher("hello", env -> "Hello!"))
            .build();
    GraphQLSchema schema = new SchemaGenerator().makeExecutableSchema(typeRegistry, wiring);

    // Spy an ExecutionContext object passed to instrumentExecutionContext()
    when(instrumentation.instrumentExecutionContext(
            any(ExecutionContext.class), any(InstrumentationExecutionParameters.class)))
        .thenAnswer(
            invocation -> {
              ExecutionContext executionContext = spy((ExecutionContext) invocation.getArgument(0));
              when(executionContext.getInstrumentation()).thenReturn(instrumentation);
              when(executionContext.getGraphQLSchema()).thenReturn(schema);
              return executionContext;
            });
    graphQl = GraphQL.newGraphQL(schema).instrumentation(instrumentation).build();

    // Capture the ExecutionResult object that is returned from instrumentExecutionResult()
    executionResultCaptor = new ExecutionResultCaptor();
    doAnswer(executionResultCaptor).when(instrumentation).instrumentExecutionResult(any(), any());
  }

  @Test
  public void instrumentationContext_CommitTrueGiven_ShouldCommitAndDeleteTransaction()
      throws Exception {
    // Arrange
    prepareForInstrumentationContextTests();

    // Act
    graphQl.execute("query @transaction(commit: true) { hello }");

    // Assert
    verify(transactionManager, times(1)).start();
    verify(transaction, times(1)).commit();
    assertThat(instrumentation.activeTransactions.get(transaction.getId())).isEmpty();

    ArgumentCaptor<InstrumentationExecutionParameters> captor =
        ArgumentCaptor.forClass(InstrumentationExecutionParameters.class);
    verify(instrumentation).instrumentExecutionResult(any(ExecutionResult.class), captor.capture());
    assertThat(captor.getValue().getGraphQLContext().hasKey(Constants.CONTEXT_TRANSACTION_KEY))
        .isFalse();
  }

  @Test
  public void instrumentationContext_NoCommitArgGiven_ShouldNotCommitAndShouldKeepTransaction()
      throws Exception {
    // Arrange
    prepareForInstrumentationContextTests();

    // Act
    graphQl.execute("query @transaction { hello }");

    // Assert
    verify(transactionManager, times(1)).start();
    verify(transaction, never()).commit();
    assertThat(instrumentation.activeTransactions.get(transaction.getId())).isPresent();

    ArgumentCaptor<InstrumentationExecutionParameters> captor =
        ArgumentCaptor.forClass(InstrumentationExecutionParameters.class);
    verify(instrumentation).instrumentExecutionResult(any(ExecutionResult.class), captor.capture());
    assertThat(captor.getValue().getGraphQLContext().hasKey(Constants.CONTEXT_TRANSACTION_KEY))
        .isTrue();
  }

  @Test
  public void
      instrumentationContext_UnknownTransactionStatusExceptionThrown_ShouldAddErrorToResult()
          throws Exception {
    // Arrange
    prepareForInstrumentationContextTests();
    doThrow(UnknownTransactionStatusException.class).when(transaction).commit();

    // Act
    graphQl.execute("query @transaction(commit: true) { hello }");

    // Assert
    assertThatTransactionWasAbortedAndResultContainsTransactionError(
        UnknownTransactionStatusException.class);
  }

  @Test
  public void instrumentationContext_CommitConflictExceptionThrown_ShouldAbortTransaction()
      throws Exception {
    // Arrange
    prepareForInstrumentationContextTests();
    doThrow(CommitConflictException.class).when(transaction).commit();

    // Act
    graphQl.execute("query @transaction(commit: true) { hello }");

    // Assert
    assertThatTransactionWasAbortedAndResultContainsTransactionError(CommitConflictException.class);
  }

  @Test
  public void instrumentationContext_CommitExceptionThrown_ShouldAddErrorToResult()
      throws Exception {
    // Arrange
    prepareForInstrumentationContextTests();
    doThrow(CommitException.class).when(transaction).commit();

    // Act
    graphQl.execute("query @transaction(commit: true) { hello }");

    // Assert
    assertThatTransactionWasAbortedAndResultContainsTransactionError(CommitException.class);
  }

  private void assertThatTransactionWasAbortedAndResultContainsTransactionError(
      Class<? extends TransactionException> transactionExceptionClass) throws TransactionException {
    verify(transactionManager, times(1)).start();
    verify(transaction, times(1)).commit();
    verify(transaction, times(1)).abort();
    assertThat(instrumentation.activeTransactions.get(transaction.getId())).isEmpty();
    ExecutionResult result = executionResultCaptor.getExecutionResult();
    assertThat(result.getErrors()).element(0).isExactlyInstanceOf(ScalarDbTransactionError.class);
    GraphQLError error = result.getErrors().get(0);
    assertThat(error.getExtensions().get("exception"))
        .isEqualTo(transactionExceptionClass.getSimpleName());
    assertThat(error.getMessage()).contains(transactionExceptionClass.getSimpleName());
    assertThat((Object) result.getData()).isNull();
  }

  public static class ExecutionResultCaptor implements Answer<CompletableFuture<ExecutionResult>> {
    private ExecutionResult executionResult = null;

    public ExecutionResult getExecutionResult() {
      return executionResult;
    }

    @SuppressWarnings("unchecked")
    @Override
    public CompletableFuture<ExecutionResult> answer(InvocationOnMock invocationOnMock)
        throws Throwable {
      CompletableFuture<ExecutionResult> future =
          (CompletableFuture<ExecutionResult>) invocationOnMock.callRealMethod();
      executionResult = future.get();
      return future;
    }
  }
}
