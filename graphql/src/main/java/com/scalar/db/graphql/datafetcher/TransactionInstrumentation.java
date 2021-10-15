package com.scalar.db.graphql.datafetcher;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.graphql.schema.Constants;
import graphql.ExecutionResult;
import graphql.ExecutionResultImpl;
import graphql.GraphQLContext;
import graphql.execution.AbortExecutionException;
import graphql.execution.ExecutionContext;
import graphql.execution.instrumentation.InstrumentationContext;
import graphql.execution.instrumentation.SimpleInstrumentation;
import graphql.execution.instrumentation.SimpleInstrumentationContext;
import graphql.execution.instrumentation.parameters.InstrumentationExecuteOperationParameters;
import graphql.execution.instrumentation.parameters.InstrumentationExecutionParameters;
import graphql.language.Argument;
import graphql.language.Directive;
import graphql.language.StringValue;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class TransactionInstrumentation extends SimpleInstrumentation {
  private static final String EXTENSIONS_TRANSACTION_KEY = "transaction";
  private static final String EXTENSIONS_TX_ID_KEY = "txId";

  private final DistributedTransactionManager transactionManager;
  private final Map<String, DistributedTransaction> transactionMap;

  public TransactionInstrumentation(DistributedTransactionManager transactionManager) {
    this.transactionManager = transactionManager;
    this.transactionMap = new HashMap<>();
  }

  @Override
  public InstrumentationContext<ExecutionResult> beginExecuteOperation(
      InstrumentationExecuteOperationParameters parameters) {
    ExecutionContext executionContext = parameters.getExecutionContext();
    GraphQLContext graphQLContext = executionContext.getGraphQLContext();

    List<Directive> transactionDirectives =
        executionContext
            .getOperationDefinition()
            .getDirectives(Constants.TRANSACTION_DIRECTIVE_NAME);
    if (transactionDirectives.size() > 0) {
      Argument txIdArg =
          transactionDirectives
              .get(0)
              .getArgument(Constants.TRANSACTION_DIRECTIVE_TX_ID_ARGUMENT_NAME);

      if (txIdArg == null) {
        try {
          DistributedTransaction transaction = transactionManager.start();
          transactionMap.put(transaction.getId(), transaction);
          graphQLContext.put(Constants.CONTEXT_TRANSACTION_KEY, transaction);
        } catch (TransactionException e) {
          throw new AbortExecutionException("Failed to start transaction.", e);
        }
      } else {
        String txId = ((StringValue) txIdArg.getValue()).getValue();
        if (txId != null) {
          DistributedTransaction transaction = transactionMap.get(txId);
          if (transaction == null) {
            throw new AbortExecutionException(
                "The specified "
                    + Constants.TRANSACTION_DIRECTIVE_TX_ID_ARGUMENT_NAME
                    + " "
                    + txId
                    + " does not exist.");
          }
          graphQLContext.put(Constants.CONTEXT_TRANSACTION_KEY, transaction);
        }
      }
    }
    return new SimpleInstrumentationContext<>();
  }

  @Override
  public CompletableFuture<ExecutionResult> instrumentExecutionResult(
      ExecutionResult executionResult, InstrumentationExecutionParameters parameters) {
    GraphQLContext graphQLContext = parameters.getGraphQLContext();
    DistributedTransaction transaction = graphQLContext.get(Constants.CONTEXT_TRANSACTION_KEY);
    if (transaction == null) {
      return CompletableFuture.completedFuture(executionResult);
    }

    Map<Object, Object> currentExt = executionResult.getExtensions();
    Map<Object, Object> withTxExt =
        new LinkedHashMap<>(currentExt == null ? Collections.emptyMap() : currentExt);
    withTxExt.put(
        EXTENSIONS_TRANSACTION_KEY, ImmutableMap.of(EXTENSIONS_TX_ID_KEY, transaction.getId()));

    return CompletableFuture.completedFuture(
        new ExecutionResultImpl(executionResult.getData(), executionResult.getErrors(), withTxExt));
  }
}
