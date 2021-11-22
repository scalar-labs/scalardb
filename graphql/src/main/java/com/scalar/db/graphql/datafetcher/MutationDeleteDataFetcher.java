package com.scalar.db.graphql.datafetcher;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.graphql.schema.DeleteConditionType;
import com.scalar.db.graphql.schema.TableGraphQlModel;
import com.scalar.db.io.Key;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MutationDeleteDataFetcher extends DataFetcherBase<List<Map<String, Object>>> {

  public MutationDeleteDataFetcher(DistributedStorage storage, TableGraphQlModel tableModel) {
    super(storage, tableModel);
  }

  @Override
  public List<Map<String, Object>> get(DataFetchingEnvironment environment) throws Exception {
    List<Map<String, Object>> deleteInputs = environment.getArgument("delete");
    List<Map<String, Object>> result = new ArrayList<>();
    for (Map<String, Object> deleteInput : deleteInputs) {
      result.add(delete(environment, deleteInput));
    }
    return result;
  }

  private Map<String, Object> delete(
      DataFetchingEnvironment environment, Map<String, Object> deleteInput) throws Exception {
    Map<String, Object> keyArg = (Map<String, Object>) deleteInput.get("key"); // table_Key
    Map<String, Object> conditionArg =
        (Map<String, Object>) deleteInput.get("condition"); // DeleteCondition

    Key partitionKey = createPartitionKeyFromKeyArgument(keyArg);
    Key clusteringKey = createClusteringKeyFromKeyArgument(keyArg);
    Delete delete =
        new Delete(partitionKey, clusteringKey)
            .forNamespace(tableModel.getNamespaceName())
            .forTable(tableModel.getTableName());
    String consistency = (String) deleteInput.get("consistency");
    if (consistency != null) {
      delete.withConsistency(Consistency.valueOf(consistency));
    }

    if (conditionArg != null) {
      DeleteConditionType deleteConditionType =
          DeleteConditionType.valueOf((String) conditionArg.get("type"));
      switch (deleteConditionType) {
        case DeleteIf:
          List<Map<String, Object>> expressionsArg =
              (List<Map<String, Object>>) conditionArg.get("expressions");
          if (expressionsArg == null || expressionsArg.isEmpty()) {
            throw new IllegalStateException("Empty expressions passed to PutIf condition");
          }
          delete.withCondition(new DeleteIf(getConditionalExpressions(expressionsArg)));
          break;
        case DeleteIfExists:
          delete.withCondition(new DeleteIfExists());
          break;
      }
    }

    performDelete(environment, delete);

    return ImmutableMap.of("applied", true, "key", keyArg);
  }
}
