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
import com.scalar.db.graphql.schema.TableGraphQlModel;
import com.scalar.db.io.Key;
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

  private final TableGraphQlModel tableGraphQlModel;
  private final DistributedStorage storage;

  public QueryGetDataFetcher(TableGraphQlModel tableGraphQlModel, DistributedStorage storage) {
    this.tableGraphQlModel = tableGraphQlModel;
    this.storage = storage;
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
                for (String fieldName : get.getProjections()) {
                  dbResult.getValue(fieldName).ifPresent(value -> data.put(fieldName, value.get()));
                }
              });
      result.data(ImmutableMap.of(tableGraphQlModel.getObjectType().getName(), data.build()));
    } catch (CrudException | ExecutionException e) {
      LOGGER.warn("Scalar DB get operation failed", e);
      result.error(DataFetcherUtils.createGraphQLError(e, environment));
    }

    return result.build();
  }

  @VisibleForTesting
  @SuppressWarnings("unchecked")
  Get createGet(Map<String, Object> getInput, DataFetchingEnvironment environment) {
    Map<String, Object> key = (Map<String, Object>) getInput.get("key");
    Key partitionKey = DataFetcherUtils.createPartitionKeyFromKeyArgument(tableGraphQlModel, key);
    Key clusteringKey = DataFetcherUtils.createClusteringKeyFromKeyArgument(tableGraphQlModel, key);
    Get get =
        new Get(partitionKey, clusteringKey)
            .forNamespace(tableGraphQlModel.getNamespaceName())
            .forTable(tableGraphQlModel.getTableName())
            .withProjections(DataFetcherUtils.getProjections(environment));
    String consistency = (String) getInput.get("consistency");
    if (consistency != null) {
      get.withConsistency(Consistency.valueOf(consistency));
    }

    return get;
  }

  @VisibleForTesting
  Optional<Result> performGet(DataFetchingEnvironment environment, Get get)
      throws CrudException, ExecutionException {
    DistributedTransaction transaction = DataFetcherUtils.getCurrentTransaction(environment);
    if (transaction != null) {
      LOGGER.debug("running Get operation with transaction: {}", get);
      return transaction.get(get);
    } else {
      LOGGER.debug("running Get operation with storage: {}", get);
      DataFetcherUtils.failIfConsensusCommitTransactionalTable(tableGraphQlModel);
      return storage.get(get);
    }
  }
}
