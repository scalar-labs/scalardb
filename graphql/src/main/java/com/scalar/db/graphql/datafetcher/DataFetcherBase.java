package com.scalar.db.graphql.datafetcher;

import static java.util.stream.Collectors.toList;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.graphql.schema.Constants;
import com.scalar.db.graphql.schema.TableGraphQlModel;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

abstract class DataFetcherBase<T> implements DataFetcher<T> {

  protected final TableGraphQlModel tableModel;
  protected final DistributedStorage storage;

  protected DataFetcherBase(DistributedStorage storage, TableGraphQlModel tableModel) {
    this.tableModel = tableModel;
    this.storage = storage;
  }

  protected Key createPartitionKeyFromKeyArgument(Map<String, Object> keyArg) {
    List<Value<?>> partitionKeyValues =
        tableModel.getPartitionKeyNames().stream()
            .map(name -> createValue(name, keyArg.get(name)))
            .collect(toList());
    return new Key(partitionKeyValues);
  }

  protected Key createClusteringKeyFromKeyArgument(Map<String, Object> keyArg) {
    List<Value<?>> clusteringKeyValues =
        tableModel.getClusteringKeyNames().stream()
            .map(name -> createValue(name, keyArg.get(name)))
            .collect(toList());
    return new Key(clusteringKeyValues);
  }

  private Value<?> createValue(String columnName, Object inputValue) {
    switch (tableModel.getColumnDataType(columnName)) {
      case BOOLEAN:
        return new BooleanValue(columnName, (Boolean) inputValue);
      case INT:
        return new IntValue(columnName, (Integer) inputValue);
      case BIGINT:
        return new BigIntValue(columnName, (Long) inputValue);
      case FLOAT:
        return new FloatValue(columnName, (Float) inputValue);
      case DOUBLE:
        return new DoubleValue(columnName, (Double) inputValue);
      case TEXT:
        return new TextValue(columnName, (String) inputValue);
      case BLOB:
      default:
        throw new IllegalArgumentException("Invalid column");
    }
  }

  protected Optional<Result> performGet(DataFetchingEnvironment environment, Get get)
      throws TransactionException, ExecutionException {
    DistributedTransaction transaction = getTransactionIfEnabled(environment);
    if (transaction != null) {
      return transaction.get(get);
    } else {
      return storage.get(get);
    }
  }

  protected void performPut(DataFetchingEnvironment environment, Put put)
      throws TransactionException, ExecutionException {
    DistributedTransaction transaction = getTransactionIfEnabled(environment);
    if (transaction != null) {
      transaction.put(put);
    } else {
      storage.put(put);
    }
  }

  protected void performDelete(DataFetchingEnvironment environment, Delete delete)
      throws TransactionException, ExecutionException {
    DistributedTransaction transaction = getTransactionIfEnabled(environment);
    if (transaction != null) {
      transaction.delete(delete.withConsistency(Consistency.LINEARIZABLE));
    } else {
      storage.delete(delete);
    }
  }

  private DistributedTransaction getTransactionIfEnabled(DataFetchingEnvironment environment) {
    DistributedTransaction transaction = null;
    if (tableModel.getTransactionEnabled()) {
      transaction = environment.getGraphQlContext().get(Constants.CONTEXT_TRANSACTION_KEY);
    }
    return transaction;
  }

  /**
   * Create a list of Scalar DB {@link ConditionalExpression} objects from a list of
   * ConditionalExpression in GraphQL request.
   *
   * @param graphQlConditionalExpressions expressions field input in PutCondition or DeleteCondition
   * @return A list of {@link ConditionalExpression}
   */
  protected List<ConditionalExpression> getConditionalExpressions(
      List<Map<String, Object>> graphQlConditionalExpressions) {
    return graphQlConditionalExpressions.stream()
        .map(
            ex -> {
              List<?> expressionValues =
                  Stream.of(
                          (Integer) ex.get("intValue"),
                          (Long) ex.get("bigIntValue"),
                          (Float) ex.get("floatValue"),
                          (Double) ex.get("doubleValue"),
                          (String) ex.get("stringValue"),
                          (Boolean) ex.get("booleanValue"))
                      .filter(Objects::nonNull)
                      .collect(toList());
              if (expressionValues.size() != 1) {
                throw new IllegalArgumentException(
                    "One and only one of intValue, bigIntValue, floatValue, doubleValue, stringValue, and booleanValue must be specified.");
              }
              Object v = expressionValues.get(0);
              Value<?> value;
              if (v instanceof Integer) {
                value = new IntValue((Integer) v);
              } else if (v instanceof Long) {
                value = new BigIntValue((Long) v);
              } else if (v instanceof Float) {
                value = new FloatValue((Float) v);
              } else if (v instanceof Double) {
                value = new DoubleValue((Double) v);
              } else if (v instanceof String) {
                value = new TextValue((String) v);
              } else if (v instanceof Boolean) {
                value = new BooleanValue((Boolean) v);
              } else {
                value = null;
              }
              return new ConditionalExpression(
                  (String) ex.get("name"),
                  value,
                  ConditionalExpression.Operator.valueOf((String) ex.get("operator")));
            })
        .collect(toList());
  }
}
