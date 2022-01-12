package com.scalar.db.graphql.datafetcher;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.exception.transaction.TransactionException;
import graphql.ErrorClassification;
import graphql.ErrorType;
import graphql.GraphQLError;
import graphql.GraphqlErrorHelper;
import graphql.language.SourceLocation;
import java.util.Collections;
import java.util.List;
import java.util.Map;

class ScalarDbTransactionError implements GraphQLError {
  private static final String EXTENSIONS_EXCEPTION_KEY = "exception";
  private final String txId;
  private final TransactionException transactionException;
  private final List<SourceLocation> locations;
  private final String message;
  private final Map<String, Object> extensions;

  public ScalarDbTransactionError(
      String txId, TransactionException transactionException, SourceLocation sourceLocation) {
    this.txId = txId;
    this.transactionException = transactionException;
    this.locations = sourceLocation == null ? null : Collections.singletonList(sourceLocation);
    String simpleExName = transactionException.getClass().getSimpleName();
    this.message =
        String.format(
            "Scalar DB %s happened in the transaction %s: %s",
            simpleExName, txId, transactionException.getMessage());
    this.extensions = ImmutableMap.of(EXTENSIONS_EXCEPTION_KEY, simpleExName);
  }

  @Override
  public String getMessage() {
    return message;
  }

  @Override
  public List<SourceLocation> getLocations() {
    return locations;
  }

  @Override
  public ErrorClassification getErrorType() {
    return ErrorType.ExecutionAborted;
  }

  @Override
  public Map<String, Object> getExtensions() {
    return extensions;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("transactionException", transactionException)
        .add("txId", txId)
        .add("message", message)
        .add("locations", locations)
        .toString();
  }

  @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
  @Override
  public boolean equals(Object o) {
    return GraphqlErrorHelper.equals(this, o);
  }

  @Override
  public int hashCode() {
    return GraphqlErrorHelper.hashCode(this);
  }
}
