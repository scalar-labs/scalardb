package com.scalar.db.graphql.datafetcher;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Get;
import com.scalar.db.api.Result;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CrudException;
import graphql.execution.DataFetcherResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryGetDataFetcher
    implements DataFetcher<DataFetcherResult<Map<String, Map<String, Object>>>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryGetDataFetcher.class);
  private final DistributedStorage storage;
  private final DataFetcherHelper helper;

  public QueryGetDataFetcher(DistributedStorage storage, DataFetcherHelper helper) {
    this.storage = storage;
    this.helper = helper;
  }

  @Override
  public DataFetcherResult<Map<String, Map<String, Object>>> get(
      DataFetchingEnvironment environment) {
    Map<String, Object> getInput = environment.getArgument("get");
    LOGGER.debug("got get argument: {}", getInput);
    Get get = createGet(getInput, environment);
    LOGGER.debug("running get: {}", get);

    DataFetcherResult.Builder<Map<String, Map<String, Object>>> result =
        DataFetcherResult.newResult();
    ImmutableMap.Builder<String, Object> data = ImmutableMap.builder();
    try {
      performGet(environment, get)
          .ifPresent(
              dbResult -> {
                for (String fieldName : helper.getFieldNames()) {
                  dbResult.getValue(fieldName).ifPresent(value -> data.put(fieldName, value.get()));
                }
              });
      result.data(ImmutableMap.of(helper.getObjectTypeName(), data.build()));
    } catch (CrudException | ExecutionException e) {
      LOGGER.warn("Scalar DB get operation failed", e);
      result.error(DataFetcherHelper.getGraphQLError(e, environment));
    }

    return result.build();
  }

  @VisibleForTesting
  @SuppressWarnings("unchecked")
  Get createGet(Map<String, Object> getInput, DataFetchingEnvironment environment) {
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
    helper.addProjections(get, environment);

    return get;
  }

  @VisibleForTesting
  Optional<Result> performGet(DataFetchingEnvironment environment, Get get)
      throws CrudException, ExecutionException {
    DistributedTransaction transaction = DataFetcherHelper.getCurrentTransaction(environment);
    if (transaction != null) {
      LOGGER.debug("running Get operation with transaction: {}", get);
      return transaction.get(get);
    } else {
      LOGGER.debug("running Get operation with storage: {}", get);
      return storage.get(get);
    }
  }
}
