package com.scalar.db.graphql.datafetcher;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.graphql.schema.ScalarDbTypes;
import com.scalar.db.graphql.schema.ScalarDbTypes.DeleteConditionType;
import com.scalar.db.graphql.schema.ScalarDbTypes.PutConditionType;
import com.scalar.db.graphql.schema.TableGraphQlModel;
import com.scalar.db.io.Key;
import graphql.Scalars;
import graphql.execution.DataFetcherResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetcherFactory;
import graphql.schema.DataFetcherFactoryEnvironment;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLScalarType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MutationDataFetcherFactory implements DataFetcherFactory<DataFetcherResult<Boolean>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MutationDataFetcherFactory.class);

  private final TableGraphQlModel tableGraphQlModel;
  private final DistributedStorage storage;

  private final DataFetcher<DataFetcherResult<Boolean>> dataFetcherForPut;
  private final DataFetcher<DataFetcherResult<Boolean>> dataFetcherForDelete;
  private final DataFetcher<DataFetcherResult<Boolean>> dataFetcherForBulkPut;
  private final DataFetcher<DataFetcherResult<Boolean>> dataFetcherForBulkDelete;
  private final DataFetcher<DataFetcherResult<Boolean>> dataFetcherForMutate;

  public MutationDataFetcherFactory(
      TableGraphQlModel tableGraphQlModel, DistributedStorage storage) {
    this.tableGraphQlModel = tableGraphQlModel;
    this.storage = storage;

    dataFetcherForPut = this::dataFetcherForPut;
    dataFetcherForDelete = this::dataFetcherForDelete;
    dataFetcherForBulkPut = this::dataFetcherForBulkPut;
    dataFetcherForBulkDelete = this::dataFetcherForBulkDelete;
    dataFetcherForMutate = this::dataFetcherForMutate;
  }

  @Override
  public DataFetcher<DataFetcherResult<Boolean>> get(DataFetcherFactoryEnvironment environment) {
    GraphQLFieldDefinition fieldDefinition = environment.getFieldDefinition();
    if (fieldDefinition.equals(tableGraphQlModel.getMutationPutField())) {
      return dataFetcherForPut;
    } else if (fieldDefinition.equals(tableGraphQlModel.getMutationDeleteField())) {
      return dataFetcherForDelete;
    } else if (fieldDefinition.equals(tableGraphQlModel.getMutationBulkPutField())) {
      return dataFetcherForBulkPut;
    } else if (fieldDefinition.equals(tableGraphQlModel.getMutationBulkDeleteField())) {
      return dataFetcherForBulkDelete;
    } else if (fieldDefinition.equals(tableGraphQlModel.getMutationMutateField())) {
      return dataFetcherForMutate;
    } else {
      throw new IllegalArgumentException("Unexpected mutation field: " + fieldDefinition);
    }
  }

  @VisibleForTesting
  DataFetcherResult<Boolean> dataFetcherForPut(DataFetchingEnvironment environment) {
    Map<String, Object> putInput = environment.getArgument("put");
    LOGGER.debug("got put argument in put operation: {}", putInput);
    Put put = createPut(putInput);

    DataFetcherResult.Builder<Boolean> result = DataFetcherResult.newResult();
    try {
      performPut(environment, put);
      result.data(true);
    } catch (CrudException | ExecutionException e) {
      LOGGER.warn("Scalar DB put operation failed", e);
      result.data(false).error(DataFetcherUtils.createGraphQLError(e, environment));
    }

    return result.build();
  }

  @VisibleForTesting
  DataFetcherResult<Boolean> dataFetcherForDelete(DataFetchingEnvironment environment) {
    Map<String, Object> deleteInput = environment.getArgument("delete");
    LOGGER.debug("got delete argument in delete operation: {}", deleteInput);
    Delete delete = createDelete(deleteInput);

    DataFetcherResult.Builder<Boolean> result = DataFetcherResult.newResult();
    try {
      performDelete(environment, delete);
      result.data(true);
    } catch (CrudException | ExecutionException e) {
      LOGGER.warn("Scalar DB delete operation failed", e);
      result.data(false).error(DataFetcherUtils.createGraphQLError(e, environment));
    }

    return result.build();
  }

  @VisibleForTesting
  DataFetcherResult<Boolean> dataFetcherForBulkPut(DataFetchingEnvironment environment) {
    List<Map<String, Object>> putInput = environment.getArgument("put");
    LOGGER.debug("got put argument in bulkPut operation: {}", putInput);
    List<Put> puts = putInput.stream().map(this::createPut).collect(Collectors.toList());

    DataFetcherResult.Builder<Boolean> result = DataFetcherResult.newResult();
    try {
      performPut(environment, puts);
      result.data(true);
    } catch (CrudException | ExecutionException e) {
      LOGGER.warn("Scalar DB put operation failed", e);
      result.data(false).error(DataFetcherUtils.createGraphQLError(e, environment));
    }

    return result.build();
  }

  @VisibleForTesting
  DataFetcherResult<Boolean> dataFetcherForBulkDelete(DataFetchingEnvironment environment) {
    List<Map<String, Object>> deleteInput = environment.getArgument("delete");
    LOGGER.debug("got delete argument in bulkDelete operation: {}", deleteInput);
    List<Delete> deletes =
        deleteInput.stream().map(this::createDelete).collect(Collectors.toList());

    DataFetcherResult.Builder<Boolean> result = DataFetcherResult.newResult();
    try {
      performDelete(environment, deletes);
      result.data(true);
    } catch (CrudException | ExecutionException e) {
      LOGGER.warn("Scalar DB delete operation failed", e);
      result.data(false).error(DataFetcherUtils.createGraphQLError(e, environment));
    }

    return result.build();
  }

  @VisibleForTesting
  DataFetcherResult<Boolean> dataFetcherForMutate(DataFetchingEnvironment environment) {
    List<Map<String, Object>> putInput = environment.getArgument("put");
    List<Map<String, Object>> deleteInput = environment.getArgument("delete");
    List<Mutation> mutations = new ArrayList<>();
    if (putInput != null) {
      LOGGER.debug("got put argument in mutate operation: {}", putInput);
      mutations.addAll(putInput.stream().map(this::createPut).collect(Collectors.toList()));
    }
    if (deleteInput != null) {
      LOGGER.debug("got delete argument in mutate operation: {}", deleteInput);
      mutations.addAll(deleteInput.stream().map(this::createDelete).collect(Collectors.toList()));
    }

    DataFetcherResult.Builder<Boolean> result = DataFetcherResult.newResult();
    try {
      performMutate(environment, mutations);
      result.data(true);
    } catch (CrudException | ExecutionException e) {
      LOGGER.warn("Scalar DB mutate operation failed", e);
      result.data(false).error(DataFetcherUtils.createGraphQLError(e, environment));
    }

    return result.build();
  }

  @SuppressWarnings("unchecked")
  @VisibleForTesting
  Put createPut(Map<String, Object> putInput) {
    Map<String, Object> keyArg = (Map<String, Object>) putInput.get("key"); // table_Key
    Map<String, Object> valuesArg = (Map<String, Object>) putInput.get("values"); // table_PutValues
    Map<String, Object> conditionArg =
        (Map<String, Object>) putInput.get("condition"); // PutCondition

    Key partitionKey =
        DataFetcherUtils.createPartitionKeyFromKeyArgument(tableGraphQlModel, keyArg);
    Key clusteringKey =
        DataFetcherUtils.createClusteringKeyFromKeyArgument(tableGraphQlModel, keyArg);
    Put put =
        new Put(partitionKey, clusteringKey)
            .forNamespace(tableGraphQlModel.getNamespaceName())
            .forTable(tableGraphQlModel.getTableName());
    String consistency = (String) putInput.get("consistency");
    if (consistency != null) {
      put.withConsistency(Consistency.valueOf(consistency));
    }

    Map<String, GraphQLScalarType> inputNameToGraphQLScalarTypeMap =
        tableGraphQlModel.getPutValuesObjectType().getFields().stream()
            .collect(
                Collectors.toMap(
                    GraphQLInputObjectField::getName, f -> (GraphQLScalarType) f.getType()));

    valuesArg.forEach(
        (name, value) -> {
          GraphQLScalarType graphQLScalarType = inputNameToGraphQLScalarTypeMap.get(name);
          if (Scalars.GraphQLInt.equals(graphQLScalarType)) {
            put.withValue(name, (Integer) value);
          } else if (ScalarDbTypes.BIG_INT_SCALAR.equals(graphQLScalarType)) {
            put.withValue(name, (Long) value);
          } else if (ScalarDbTypes.FLOAT_32_SCALAR.equals(graphQLScalarType)) {
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
            throw new IllegalArgumentException("Empty expressions passed to PutIf condition");
          }
          put.withCondition(
              new PutIf(DataFetcherUtils.createConditionalExpressions(expressionsArg)));
          break;
        case PutIfExists:
          put.withCondition(new PutIfExists());
          break;
        case PutIfNotExists:
          put.withCondition(new PutIfNotExists());
          break;
      }
    }
    return put;
  }

  @SuppressWarnings("unchecked")
  @VisibleForTesting
  Delete createDelete(Map<String, Object> deleteInput) {
    Map<String, Object> keyArg = (Map<String, Object>) deleteInput.get("key"); // table_Key
    Map<String, Object> conditionArg =
        (Map<String, Object>) deleteInput.get("condition"); // DeleteCondition

    Key partitionKey =
        DataFetcherUtils.createPartitionKeyFromKeyArgument(tableGraphQlModel, keyArg);
    Key clusteringKey =
        DataFetcherUtils.createClusteringKeyFromKeyArgument(tableGraphQlModel, keyArg);
    Delete delete =
        new Delete(partitionKey, clusteringKey)
            .forNamespace(tableGraphQlModel.getNamespaceName())
            .forTable(tableGraphQlModel.getTableName());
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
            throw new IllegalArgumentException("Empty expressions passed to PutIf condition");
          }
          delete.withCondition(
              new DeleteIf(DataFetcherUtils.createConditionalExpressions(expressionsArg)));
          break;
        case DeleteIfExists:
          delete.withCondition(new DeleteIfExists());
          break;
      }
    }
    return delete;
  }

  @VisibleForTesting
  void performPut(DataFetchingEnvironment environment, Put put)
      throws CrudException, ExecutionException {
    DistributedTransaction transaction = DataFetcherUtils.getCurrentTransaction(environment);
    if (transaction != null) {
      LOGGER.debug("running Put operation with transaction: {}", put);
      transaction.put(put);
    } else {
      LOGGER.debug("running Put operation with storage: {}", put);
      DataFetcherUtils.failIfConsensusCommitTransactionalTable(tableGraphQlModel);
      storage.put(put);
    }
  }

  @VisibleForTesting
  void performPut(DataFetchingEnvironment environment, List<Put> puts)
      throws CrudException, ExecutionException {
    DistributedTransaction transaction = DataFetcherUtils.getCurrentTransaction(environment);
    if (transaction != null) {
      LOGGER.debug("running Put operations with transaction: {}", puts);
      transaction.put(puts);
    } else {
      LOGGER.debug("running Put operations with storage: {}", puts);
      DataFetcherUtils.failIfConsensusCommitTransactionalTable(tableGraphQlModel);
      storage.put(puts);
    }
  }

  @VisibleForTesting
  void performDelete(DataFetchingEnvironment environment, Delete delete)
      throws CrudException, ExecutionException {
    DistributedTransaction transaction = DataFetcherUtils.getCurrentTransaction(environment);
    if (transaction != null) {
      LOGGER.debug("running Delete operation with transaction: {}", delete);
      transaction.delete(delete);
    } else {
      LOGGER.debug("running Delete operation with storage: {}", delete);
      DataFetcherUtils.failIfConsensusCommitTransactionalTable(tableGraphQlModel);
      storage.delete(delete);
    }
  }

  @VisibleForTesting
  void performDelete(DataFetchingEnvironment environment, List<Delete> deletes)
      throws CrudException, ExecutionException {
    DistributedTransaction transaction = DataFetcherUtils.getCurrentTransaction(environment);
    if (transaction != null) {
      LOGGER.debug("running Delete operations with transaction: {}", deletes);
      transaction.delete(deletes);
    } else {
      LOGGER.debug("running Delete operations with storage: {}", deletes);
      DataFetcherUtils.failIfConsensusCommitTransactionalTable(tableGraphQlModel);
      storage.delete(deletes);
    }
  }

  @VisibleForTesting
  void performMutate(DataFetchingEnvironment environment, List<Mutation> mutations)
      throws CrudException, ExecutionException {
    DistributedTransaction transaction = DataFetcherUtils.getCurrentTransaction(environment);
    if (transaction != null) {
      LOGGER.debug("running Mutation operations with transaction: {}", mutations);
      transaction.mutate(mutations);
    } else {
      LOGGER.debug("running Mutation operations with storage: {}", mutations);
      DataFetcherUtils.failIfConsensusCommitTransactionalTable(tableGraphQlModel);
      storage.mutate(mutations);
    }
  }
}
