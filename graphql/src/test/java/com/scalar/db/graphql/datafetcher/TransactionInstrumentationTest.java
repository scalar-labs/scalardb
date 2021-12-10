package com.scalar.db.graphql.datafetcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.graphql.datafetcher.TransactionInstrumentation.TransactionCommitInstrumentationContext;
import com.scalar.db.graphql.schema.Constants;
import graphql.ExecutionResult;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TransactionInstrumentationTest {
  private static final String ANY_TX_ID = UUID.randomUUID().toString();

  @Mock private InstrumentationExecuteOperationParameters instrumentationExecuteOperationParameters;
  @Mock private ExecutionContext executionContext;
  @Mock private OperationDefinition operationDefinition;
  @Mock private DistributedTransactionManager transactionManager;
  @Mock private DistributedTransaction transaction;
  @Mock private InstrumentationExecutionParameters instrumentationExecutionParameters;
  @Mock private ExecutionResult executionResult;
  private GraphQLContext graphQlContext;
  private TransactionInstrumentation instrumentation;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    graphQlContext = spy(new GraphQLContext.Builder().build());
    when(transaction.getId()).thenReturn(ANY_TX_ID);
    when(transactionManager.start()).thenReturn(transaction);

    when(executionContext.getGraphQLContext()).thenReturn(graphQlContext);
    when(executionContext.getOperationDefinition()).thenReturn(operationDefinition);
    when(instrumentationExecuteOperationParameters.getExecutionContext())
        .thenReturn(executionContext);

    when(instrumentationExecutionParameters.getGraphQLContext()).thenReturn(graphQlContext);

    instrumentation = new TransactionInstrumentation(transactionManager);
  }

  private void prepareDirective(Directive transactionDirective) {
    when(operationDefinition.getDirectives(Constants.TRANSACTION_DIRECTIVE_NAME))
        .thenReturn(Collections.singletonList(transactionDirective));
  }

  @Test
  public void beginExecuteOperation_DirectiveWithNoArgumentGiven_ShouldStartTransaction()
      throws Exception {
    // Arrange
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
    assertThat(context).isInstanceOf(TransactionCommitInstrumentationContext.class);
  }

  @Test
  public void beginExecuteOperation_TransactionFailsToStart_ShouldThrowAbortExecutionException()
      throws Exception {
    // Arrange
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
    prepareDirective(
        Directive.newDirective()
            .name(Constants.TRANSACTION_DIRECTIVE_NAME)
            .argument(
                new Argument(
                    Constants.TRANSACTION_DIRECTIVE_TX_ID_ARGUMENT_NAME,
                    new StringValue(ANY_TX_ID)))
            .build());
    instrumentation.transactionMap.put(ANY_TX_ID, transaction);

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
    instrumentation.transactionMap.put(ANY_TX_ID, transaction);

    // Act
    InstrumentationContext<ExecutionResult> context =
        instrumentation.beginExecuteOperation(instrumentationExecuteOperationParameters);

    // Assert
    verify(transactionManager, never()).start();
    verify(graphQlContext).put(Constants.CONTEXT_TRANSACTION_KEY, transaction);
    assertThat(context).isInstanceOf(TransactionCommitInstrumentationContext.class);
  }

  @Test
  public void beginExecuteOperation_TransactionIsNotFound_ShouldThrowAbortExecutionException()
      throws TransactionException {
    // Arrange
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

  @Test
  public void instrumentExecutionResult_TransactionNotFound_ShouldReturnTheSameExecutionResult()
      throws Exception {
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
    assertThat(ext).containsKey(TransactionInstrumentation.EXTENSIONS_TRANSACTION_KEY);
    assertThat((Map<Object, Object>) ext.get(TransactionInstrumentation.EXTENSIONS_TRANSACTION_KEY))
        .containsOnly(entry(TransactionInstrumentation.EXTENSIONS_TX_ID_KEY, transaction.getId()));
  }
}
