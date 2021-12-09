package com.scalar.db.graphql.datafetcher;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.graphql.schema.CommonSchema;
import com.scalar.db.graphql.schema.PutConditionType;
import com.scalar.db.graphql.schema.TableGraphQlModel;
import com.scalar.db.io.Key;
import graphql.Scalars;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLScalarType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MutationPutDataFetcher extends DataFetcherBase<List<Map<String, Object>>> {

  public MutationPutDataFetcher(DistributedStorage storage, TableGraphQlModel tableModel) {
    super(storage, tableModel);
  }

  @Override
  public List<Map<String, Object>> get(DataFetchingEnvironment environment) throws Exception {
    List<Map<String, Object>> putInputs = environment.getArgument("put");
    List<Map<String, Object>> result = new ArrayList<>();
    for (Map<String, Object> putInput : putInputs) {
      result.add(put(environment, putInput));
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> put(DataFetchingEnvironment environment, Map<String, Object> putInput)
      throws TransactionException, ExecutionException {
    Map<String, Object> keyArg = (Map<String, Object>) putInput.get("key"); // table_Key
    Map<String, Object> valuesArg = (Map<String, Object>) putInput.get("values"); // table_PutValues
    Map<String, Object> conditionArg =
        (Map<String, Object>) putInput.get("condition"); // PutCondition

    Key partitionKey = createPartitionKeyFromKeyArgument(keyArg);
    Key clusteringKey = createClusteringKeyFromKeyArgument(keyArg);
    Put put =
        new Put(partitionKey, clusteringKey)
            .forNamespace(tableModel.getNamespaceName())
            .forTable(tableModel.getTableName());
    String consistency = (String) putInput.get("consistency");
    if (consistency != null) {
      put.withConsistency(Consistency.valueOf(consistency));
    }

    Map<String, GraphQLScalarType> inputNameToGraphQLScalarTypeMap =
        tableModel.getPutValuesObjectType().getFields().stream()
            .collect(
                Collectors.toMap(
                    GraphQLInputObjectField::getName, f -> (GraphQLScalarType) f.getType()));

    valuesArg.forEach(
        (name, value) -> {
          GraphQLScalarType graphQLScalarType = inputNameToGraphQLScalarTypeMap.get(name);
          if (Scalars.GraphQLInt.equals(graphQLScalarType)) {
            put.withValue(name, (Integer) value);
          } else if (CommonSchema.BIG_INT_SCALAR.equals(graphQLScalarType)) {
            put.withValue(name, (Long) value);
          } else if (CommonSchema.FLOAT_32_SCALAR.equals(graphQLScalarType)) {
            put.withValue(name, (Float) value);
          } else if (Scalars.GraphQLFloat.equals(graphQLScalarType)) {
            put.withValue(name, (Double) value);
          } else if (Scalars.GraphQLString.equals(graphQLScalarType)) {
            put.withValue(name, (String) value);
          } else if (Scalars.GraphQLBoolean.equals(graphQLScalarType)) {
            put.withValue(name, (Boolean) value);
          } else {
            throw new IllegalArgumentException("Invalid type");
          }
        });

    if (conditionArg != null) {
      PutConditionType putConditionType =
          PutConditionType.valueOf((String) conditionArg.get("type"));
      switch (putConditionType) {
        case PutIf:
          List<Map<String, Object>> expressionsArg =
              (List<Map<String, Object>>) conditionArg.get("expressions");
          if (expressionsArg == null || expressionsArg.isEmpty()) {
            throw new IllegalStateException("Empty expressions passed to PutIf condition");
          }
          put.withCondition(new PutIf(getConditionalExpressions(expressionsArg)));
          break;
        case PutIfExists:
          put.withCondition(new PutIfExists());
          break;
        case PutIfNotExists:
          put.withCondition(new PutIfNotExists());
          break;
      }
    }

    performPut(environment, put);

    return ImmutableMap.of("applied", true, "key", keyArg);
  }
}
