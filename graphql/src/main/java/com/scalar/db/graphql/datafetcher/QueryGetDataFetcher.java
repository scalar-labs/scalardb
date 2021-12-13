package com.scalar.db.graphql.datafetcher;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Get;
import com.scalar.db.api.Result;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.TransactionException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Map;
import java.util.Optional;

public class QueryGetDataFetcher implements DataFetcher<Map<String, Map<String, Object>>> {
  private final DistributedStorage storage;
  private final DataFetcherHelper helper;

  public QueryGetDataFetcher(DistributedStorage storage, DataFetcherHelper helper) {
    this.storage = storage;
    this.helper = helper;
  }

  @Override
  public Map<String, Map<String, Object>> get(DataFetchingEnvironment environment)
      throws Exception {
    Map<String, Object> getInput = environment.getArgument("get");
    Get get = createGet(getInput);

    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    performGet(environment, get)
        .ifPresent(
            result -> {
              for (String fieldName : helper.getFieldNames()) {
                result.getValue(fieldName).ifPresent(value -> builder.put(fieldName, value.get()));
              }
            });

    return ImmutableMap.of(helper.getObjectTypeName(), builder.build());
  }

  @SuppressWarnings("unchecked")
  private Get createGet(Map<String, Object> getInput) {
    Map<String, Object> key = (Map<String, Object>) getInput.get("key");
    Get get =
        new Get(
                helper.createPartitionKeyFromKeyArgument(key),
                helper.createClusteringKeyFromKeyArgument(key))
            .forNamespace(helper.getNamespaceName())
            .forTable(helper.getTableName());
    String consistency = (String) getInput.get("consistency");
    if (consistency != null) {
      get.withConsistency(Consistency.valueOf(consistency));
    }

    return get;
  }

  @VisibleForTesting
  Optional<Result> performGet(DataFetchingEnvironment environment, Get get)
      throws TransactionException, ExecutionException {
    DistributedTransaction transaction = helper.getTransactionIfEnabled(environment);
    if (transaction != null) {
      return transaction.get(get);
    } else {
      return storage.get(get);
    }
  }
}
