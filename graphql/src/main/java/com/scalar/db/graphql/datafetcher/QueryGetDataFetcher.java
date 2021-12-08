package com.scalar.db.graphql.datafetcher;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.graphql.schema.TableGraphQlModel;
import com.scalar.db.io.TextValue;
import graphql.schema.DataFetchingEnvironment;
import java.util.Map;

public class QueryGetDataFetcher extends DataFetcherBase<Map<String, Map<String, Object>>> {

  public QueryGetDataFetcher(DistributedStorage storage, TableGraphQlModel tableModel) {
    super(storage, tableModel);
  }

  @Override
  public Map<String, Map<String, Object>> get(DataFetchingEnvironment environment)
      throws Exception {
    Map<String, Object> getInput = environment.getArgument("get");
    Map<String, Object> key = (Map<String, Object>) getInput.get("key");
    Get get =
        new Get(createPartitionKeyFromKeyArgument(key), createClusteringKeyFromKeyArgument(key))
            .forNamespace(tableModel.getNamespaceName())
            .forTable(tableModel.getTableName());
    String consistency = (String) getInput.get("consistency");
    if (consistency != null) {
      get.withConsistency(Consistency.valueOf(consistency));
    }

    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    performGet(environment, get)
        .ifPresent(
            result -> {
              tableModel
                  .getFieldNames()
                  .forEach(
                      fieldName ->
                          result
                              .getValue(fieldName)
                              .ifPresent(
                                  value -> {
                                    if (value instanceof TextValue) {
                                      builder.put(fieldName, value.getAsString().get());
                                    } else {
                                      builder.put(fieldName, value.get());
                                    }
                                  }));
            });

    return ImmutableMap.of(tableModel.getObjectType().getName(), builder.build());
  }
}
