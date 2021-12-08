package com.scalar.db.graphql.datafetcher;

import static java.util.stream.Collectors.toList;

import com.google.common.collect.ImmutableList;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
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

  protected List<Result> performScan(DataFetchingEnvironment environment, Scan scan)
      throws TransactionException, ExecutionException {
    DistributedTransaction transaction = getTransactionIfEnabled(environment);
    if (transaction != null) {
      return transaction.scan(scan);
    } else {
      return ImmutableList.copyOf(storage.scan(scan));
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
      transaction.delete(delete);
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
            ex ->
                new ConditionalExpression(
                    (String) ex.get("name"),
                    createAnonymousValueFromMap(ex),
                    ConditionalExpression.Operator.valueOf((String) ex.get("operator"))))
        .collect(toList());
  }

  protected Value<?> createAnonymousValueFromMap(Map<String, Object> map) {
    Object v = getOneScalarValue(map);
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
      throw new IllegalArgumentException("Unexpected value: " + v.getClass());
    }
    return value;
  }

  protected Value<?> createNamedValueFromMap(String name, Map<String, Object> map) {
    Object v = getOneScalarValue(map);
    Value<?> value;
    if (v instanceof Integer) {
      value = new IntValue(name, (Integer) v);
    } else if (v instanceof Long) {
      value = new BigIntValue(name, (Long) v);
    } else if (v instanceof Float) {
      value = new FloatValue(name, (Float) v);
    } else if (v instanceof Double) {
      value = new DoubleValue(name, (Double) v);
    } else if (v instanceof String) {
      value = new TextValue(name, (String) v);
    } else if (v instanceof Boolean) {
      value = new BooleanValue(name, (Boolean) v);
    } else {
      throw new IllegalArgumentException("Unexpected value: " + v.getClass());
    }
    return value;
  }

  private Object getOneScalarValue(Map<String, Object> map) {
    List<?> values =
        Stream.of(
                (Integer) map.get("intValue"),
                (Long) map.get("bigIntValue"),
                (Float) map.get("floatValue"),
                (Double) map.get("doubleValue"),
                (String) map.get("stringValue"),
                (Boolean) map.get("booleanValue"))
            .filter(Objects::nonNull)
            .collect(toList());
    if (values.size() != 1) {
      throw new IllegalArgumentException(
          "One and only one of intValue, bigIntValue, floatValue, doubleValue, stringValue, and booleanValue must be specified.");
    }
    return values.get(0);
  }
}
