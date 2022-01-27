package com.scalar.db.graphql.instrumentation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.graphql.schema.Constants;
import com.scalar.db.util.ActiveExpiringMap;
import graphql.ExecutionResult;
import graphql.ExecutionResultImpl;
import graphql.GraphQLContext;
import graphql.GraphQLError;
import graphql.execution.AbortExecutionException;
import graphql.execution.ExecutionContext;
import graphql.execution.instrumentation.InstrumentationContext;
import graphql.execution.instrumentation.SimpleInstrumentation;
import graphql.execution.instrumentation.SimpleInstrumentationContext;
import graphql.execution.instrumentation.parameters.InstrumentationExecuteOperationParameters;
import graphql.execution.instrumentation.parameters.InstrumentationExecutionParameters;
import graphql.language.Argument;
import graphql.language.BooleanValue;
import graphql.language.Directive;
import graphql.language.StringValue;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionInstrumentation extends SimpleInstrumentation {
  private static final Logger LOGGER = LoggerFactory.getLogger(TransactionInstrumentation.class);
  private static final String RESULT_EXTENSIONS_TRANSACTION_KEY = "transaction";
  private static final String RESULT_EXTENSIONS_TRANSACTION_TX_ID_KEY = "txId";
  private static final String CONTEXT_TRANSACTION_ERROR_KEY = "transactionError";
  private static final long TRANSACTION_LIFETIME_MILLIS = 60000;
  private static final long TRANSACTION_EXPIRATION_INTERVAL_MILLIS = 1000;

  @VisibleForTesting final ActiveExpiringMap<String, DistributedTransaction> activeTransactions;
  private final DistributedTransactionManager transactionManager;

  public TransactionInstrumentation(DistributedTransactionManager transactionManager) {
    this.transactionManager = transactionManager;
    this.activeTransactions =
        new ActiveExpiringMap<>(
            TRANSACTION_LIFETIME_MILLIS,
            TRANSACTION_EXPIRATION_INTERVAL_MILLIS,
            t -> {
              LOGGER.warn("the transaction {} is expired", t.getId());
              try {
                t.abort();
              } catch (AbortException e) {
                LOGGER.warn("an error happened when aborting the transaction", e);
              }
            });
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

    if (transactionDirectives.isEmpty()) {
      return SimpleInstrumentationContext.noOp();
    }

    Directive directive = transactionDirectives.get(0); // @transaction is not repeatable
    Argument txIdArg = directive.getArgument(Constants.TRANSACTION_DIRECTIVE_TX_ID_ARGUMENT_NAME);
    Argument commitArg =
        directive.getArgument(Constants.TRANSACTION_DIRECTIVE_COMMIT_ARGUMENT_NAME);

    DistributedTransaction transaction;
    if (txIdArg == null) {
      // start a new transaction
      try {
        transaction = transactionManager.start();
        activeTransactions.put(transaction.getId(), transaction);
      } catch (TransactionException e) {
        LOGGER.warn("failed to start transaction", e);
        throw new AbortExecutionException(e);
      }
    } else {
      // continue an existing transaction
      String txId = ((StringValue) txIdArg.getValue()).getValue();
      transaction =
          activeTransactions
              .get(txId)
              .orElseThrow(
                  () -> {
                    String message =
                        String.format(
                            "The specified transaction %s is not found."
                                + " It might have been expired",
                            txId);
                    LOGGER.warn(message);
                    return new AbortExecutionException(message);
                  });
    }
    graphQLContext.put(Constants.CONTEXT_TRANSACTION_KEY, transaction);

    boolean isCommitTrue =
        commitArg != null
            && commitArg.getValue() != null
            && ((BooleanValue) commitArg.getValue()).isValue();
    if (isCommitTrue) {
      return SimpleInstrumentationContext.whenCompleted(
          (executionResult, throwable) -> {
            activeTransactions.remove(transaction.getId());
            try {
              if (throwable != null) {
                LOGGER.warn(
                    "aborting the transaction {} since an error happened during execution",
                    transaction.getId(),
                    throwable);
                transaction.abort();
              } else {
                try {
                  LOGGER.info("committing the transaction {}", transaction.getId());
                  transaction.commit();
                } catch (CommitException | UnknownTransactionStatusException e) {
                  LOGGER.warn(
                      "an error happened when committing the transaction {}",
                      transaction.getId(),
                      e);
                  graphQLContext.put(
                      CONTEXT_TRANSACTION_ERROR_KEY,
                      new ScalarDbTransactionError(
                          transaction.getId(), e, directive.getSourceLocation()));
                  transaction.abort();
                }
              }
            } catch (AbortException e) {
              LOGGER.warn(
                  "an error happened when aborting the transaction {}", transaction.getId(), e);
            }
          });
    } else {
      return SimpleInstrumentationContext.noOp();
    }
  }

  @Override
  public CompletableFuture<ExecutionResult> instrumentExecutionResult(
      ExecutionResult executionResult, InstrumentationExecutionParameters parameters) {
    GraphQLContext graphQLContext = parameters.getGraphQLContext();

    // If a transaction error has occurred, only the "errors" key should be included in the result.
    ScalarDbTransactionError txError = graphQLContext.get(CONTEXT_TRANSACTION_ERROR_KEY);
    if (txError != null) {
      List<GraphQLError> currentErrors = executionResult.getErrors();
      List<GraphQLError> errorsWithTxError =
          ImmutableList.<GraphQLError>builder()
              .addAll(currentErrors != null ? currentErrors : Collections.emptyList())
              .add(txError)
              .build();
      return CompletableFuture.completedFuture(new ExecutionResultImpl(errorsWithTxError));
    }

    // If a transaction is ongoing, transaction ID should be included in the "extensions" key of the
    // result.
    DistributedTransaction transaction = graphQLContext.get(Constants.CONTEXT_TRANSACTION_KEY);
    if (transaction == null) {
      return CompletableFuture.completedFuture(executionResult);
    }

    Map<Object, Object> currentExt = executionResult.getExtensions();
    Map<Object, Object> extWithTxId =
        ImmutableMap.builder()
            .putAll(currentExt != null ? currentExt : Collections.emptyMap())
            .put(
                RESULT_EXTENSIONS_TRANSACTION_KEY,
                ImmutableMap.of(RESULT_EXTENSIONS_TRANSACTION_TX_ID_KEY, transaction.getId()))
            .build();

    return CompletableFuture.completedFuture(
        new ExecutionResultImpl(
            executionResult.getData(), executionResult.getErrors(), extWithTxId));
  }
}
