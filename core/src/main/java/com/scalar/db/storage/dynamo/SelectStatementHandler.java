package com.scalar.db.storage.dynamo;

import com.scalar.db.api.Consistency;
import com.scalar.db.api.Get;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Value;
import com.scalar.db.storage.common.TableMetadataManager;
import com.scalar.db.util.Utility;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

/**
 * A handler class for select statement
 *
 * @author Yuji Ito
 */
@ThreadSafe
public class SelectStatementHandler extends StatementHandler {

  /**
   * Constructs a {@code SelectStatementHandler} with the specified {@link DynamoDbClient} and a new
   * {@link TableMetadataManager}
   *
   * @param client {@code DynamoDbClient}
   * @param metadataManager {@code TableMetadataManager}
   */
  public SelectStatementHandler(DynamoDbClient client, TableMetadataManager metadataManager) {
    super(client, metadataManager);
  }

  @Nonnull
  @Override
  public List<Map<String, AttributeValue>> handle(Operation operation) throws ExecutionException {
    checkArgument(operation, Get.class, Scan.class);

    TableMetadata tableMetadata = metadataManager.getTableMetadata(operation);
    try {
      if (Utility.isSecondaryIndexSpecified(operation, tableMetadata)) {
        return executeQueryWithIndex((Selection) operation, tableMetadata);
      }

      if (operation instanceof Get) {
        return executeGet((Get) operation, tableMetadata);
      } else {
        return executeQuery((Scan) operation, tableMetadata);
      }
    } catch (DynamoDbException e) {
      throw new ExecutionException(e.getMessage(), e);
    }
  }

  private List<Map<String, AttributeValue>> executeGet(Get get, TableMetadata tableMetadata) {
    DynamoOperation dynamoOperation = new DynamoOperation(get, tableMetadata);

    GetItemRequest.Builder builder =
        GetItemRequest.builder()
            .tableName(dynamoOperation.getTableName())
            .key(dynamoOperation.getKeyMap());

    if (!get.getProjections().isEmpty()) {
      Map<String, String> expressionAttributeNames = new HashMap<>();
      projectionExpression(builder, get, expressionAttributeNames);
      builder.expressionAttributeNames(expressionAttributeNames);
    }

    if (get.getConsistency() != Consistency.EVENTUAL) {
      builder.consistentRead(true);
    }

    GetItemResponse getItemResponse = client.getItem(builder.build());
    if (getItemResponse.hasItem()) {
      return Collections.singletonList(getItemResponse.item());
    } else {
      return Collections.emptyList();
    }
  }

  private List<Map<String, AttributeValue>> executeQueryWithIndex(
      Selection selection, TableMetadata tableMetadata) {
    DynamoOperation dynamoOperation = new DynamoOperation(selection, tableMetadata);
    Value<?> keyValue = selection.getPartitionKey().get().get(0);
    String column = keyValue.getName();
    String indexTable = dynamoOperation.getGlobalIndexName(column);
    QueryRequest.Builder builder =
        QueryRequest.builder().tableName(dynamoOperation.getTableName()).indexName(indexTable);

    String expressionColumnName = DynamoOperation.COLUMN_NAME_ALIAS + "0";
    String condition = expressionColumnName + " = " + DynamoOperation.VALUE_ALIAS + "0";
    ValueBinder binder = new ValueBinder(DynamoOperation.VALUE_ALIAS);
    keyValue.accept(binder);
    Map<String, AttributeValue> bindMap = binder.build();
    builder.keyConditionExpression(condition).expressionAttributeValues(bindMap);

    Map<String, String> expressionAttributeNames = new HashMap<>();
    expressionAttributeNames.put(expressionColumnName, column);

    if (!selection.getProjections().isEmpty()) {
      projectionExpression(builder, selection, expressionAttributeNames);
    }

    builder.expressionAttributeNames(expressionAttributeNames);

    if (selection instanceof Scan) {
      Scan scan = (Scan) selection;
      if (scan.getLimit() > 0) {
        builder.limit(scan.getLimit());
      }
    }

    QueryResponse queryResponse = client.query(builder.build());
    return new ArrayList<>(queryResponse.items());
  }

  private List<Map<String, AttributeValue>> executeQuery(Scan scan, TableMetadata tableMetadata) {
    DynamoOperation dynamoOperation = new DynamoOperation(scan, tableMetadata);
    QueryRequest.Builder builder = QueryRequest.builder().tableName(dynamoOperation.getTableName());

    getIndexName(scan)
        .ifPresent(
            name -> {
              String indexTableName = dynamoOperation.getIndexName(name);
              builder.indexName(indexTableName);
            });

    setConditions(builder, scan, tableMetadata);

    // When multiple clustering keys exist, the ordering and the limitation will be applied later
    if (dynamoOperation.isSingleClusteringKey() && !scan.getOrderings().isEmpty()) {
      scan.getOrderings()
          .forEach(
              o -> {
                if (dynamoOperation.getMetadata().getClusteringKeyNames().contains(o.getName())
                    && o.getOrder() == Scan.Ordering.Order.DESC) {
                  builder.scanIndexForward(false);
                }
              });
    }

    if (dynamoOperation.isSingleClusteringKey() && scan.getLimit() > 0) {
      builder.limit(scan.getLimit());
    }

    if (!scan.getProjections().isEmpty()) {
      Map<String, String> expressionAttributeNames = new HashMap<>();
      projectionExpression(builder, scan, expressionAttributeNames);
      builder.expressionAttributeNames(expressionAttributeNames);
    }

    if (scan.getConsistency() != Consistency.EVENTUAL) {
      builder.consistentRead(true);
    }

    QueryResponse queryResponse = client.query(builder.build());
    List<Map<String, AttributeValue>> ret = new ArrayList<>(queryResponse.items());
    if (!dynamoOperation.isSingleClusteringKey()) {
      ret = new ItemSorter(scan, tableMetadata).sort(ret);
    }
    return ret;
  }

  private void projectionExpression(
      DynamoDbRequest.Builder builder,
      Selection selection,
      Map<String, String> expressionAttributeNames) {
    assert builder instanceof GetItemRequest.Builder || builder instanceof QueryRequest.Builder;

    List<String> projections = new ArrayList<>(selection.getProjections().size());
    for (String projection : selection.getProjections()) {
      String alias = DynamoOperation.COLUMN_NAME_ALIAS + expressionAttributeNames.size();
      projections.add(alias);
      expressionAttributeNames.put(alias, projection);
    }
    String projectionExpression = String.join(",", projections);

    if (builder instanceof GetItemRequest.Builder) {
      ((GetItemRequest.Builder) builder).projectionExpression(projectionExpression);
    } else {
      ((QueryRequest.Builder) builder).projectionExpression(projectionExpression);
    }
  }

  private Optional<String> getIndexName(Scan scan) {
    if (scan.getStartClusteringKey().isPresent()) {
      List<Value<?>> start = scan.getStartClusteringKey().get().get();
      return Optional.of(start.get(start.size() - 1).getName());
    }

    if (scan.getEndClusteringKey().isPresent()) {
      List<Value<?>> end = scan.getEndClusteringKey().get().get();
      return Optional.of(end.get(end.size() - 1).getName());
    }

    return Optional.empty();
  }

  private void setConditions(QueryRequest.Builder builder, Scan scan, TableMetadata tableMetadata) {
    List<String> conditions = new ArrayList<>();
    List<String> filters = new ArrayList<>();

    conditions.add(getPartitionKeyCondition());

    boolean isRangeEnabled = setRangeCondition(scan, conditions);
    setStartCondition(scan, conditions, filters, isRangeEnabled);
    setEndCondition(scan, conditions, filters, isRangeEnabled);
    String keyConditions = String.join(" AND ", conditions);

    Map<String, AttributeValue> bindMap = getPartitionKeyBindMap(scan, tableMetadata);
    if (isRangeEnabled) {
      bindMap.putAll(getRangeBindMap(scan));
    }
    bindMap.putAll(getStartBindMap(scan, isRangeEnabled));
    bindMap.putAll(getEndBindMap(scan, isRangeEnabled));

    if (!filters.isEmpty()) {
      String filterExpression = String.join(" AND ", filters);
      builder.filterExpression(filterExpression);
    }

    builder.keyConditionExpression(keyConditions).expressionAttributeValues(bindMap);
  }

  private String getPartitionKeyCondition() {
    return DynamoOperation.PARTITION_KEY + " = " + DynamoOperation.PARTITION_KEY_ALIAS;
  }

  private Map<String, AttributeValue> getPartitionKeyBindMap(
      Scan scan, TableMetadata tableMetadata) {
    Map<String, AttributeValue> bindMap = new HashMap<>();
    DynamoOperation dynamoOperation = new DynamoOperation(scan, tableMetadata);
    String partitionKey = dynamoOperation.getConcatenatedPartitionKey();

    bindMap.put(
        DynamoOperation.PARTITION_KEY_ALIAS, AttributeValue.builder().s(partitionKey).build());

    return bindMap;
  }

  private boolean setRangeCondition(Scan scan, List<String> conditions) {
    if (!scan.getStartClusteringKey().isPresent() || !scan.getEndClusteringKey().isPresent()) {
      return false;
    }

    List<Value<?>> start = scan.getStartClusteringKey().get().get();
    List<Value<?>> end = scan.getEndClusteringKey().get().get();
    String startKeyName = start.get(start.size() - 1).getName();
    String endKeyName = end.get(end.size() - 1).getName();

    if (startKeyName.equals(endKeyName)) {
      if (!scan.getStartInclusive() || !scan.getEndInclusive()) {
        throw new IllegalArgumentException("DynamoDB does NOT support scan with exclusive range.");
      }
      conditions.add(startKeyName + DynamoOperation.RANGE_CONDITION);
      return true;
    } else {
      return false;
    }
  }

  private void setStartCondition(
      Scan scan, List<String> conditions, List<String> filters, boolean isRangeEnabled) {
    scan.getStartClusteringKey()
        .ifPresent(
            k -> {
              List<Value<?>> start = k.get();
              for (int i = 0; i < start.size(); i++) {
                Value<?> value = start.get(i);
                List<String> elements = new ArrayList<>();
                elements.add(value.getName());
                if (i < start.size() - 1) {
                  elements.add("=");
                  elements.add(DynamoOperation.START_CLUSTERING_KEY_ALIAS + i);
                  filters.add(String.join(" ", elements));
                } else if (!isRangeEnabled) {
                  if (scan.getStartInclusive()) {
                    elements.add(">=");
                  } else {
                    elements.add(">");
                  }
                  elements.add(DynamoOperation.START_CLUSTERING_KEY_ALIAS + i);
                  conditions.add(String.join(" ", elements));
                }
              }
            });
  }

  private void setEndCondition(
      Scan scan, List<String> conditions, List<String> filters, boolean isRangeEnabled) {
    scan.getEndClusteringKey()
        .ifPresent(
            k -> {
              List<Value<?>> end = k.get();
              for (int i = 0; i < end.size(); i++) {
                Value<?> value = end.get(i);
                List<String> elements = new ArrayList<>();
                elements.add(value.getName());
                if (i < end.size() - 1) {
                  elements.add("=");
                  elements.add(DynamoOperation.END_CLUSTERING_KEY_ALIAS + i);
                  filters.add(String.join(" ", elements));
                } else if (!isRangeEnabled) {
                  if (scan.getEndInclusive()) {
                    elements.add("<=");
                  } else {
                    elements.add("<");
                  }
                  elements.add(DynamoOperation.END_CLUSTERING_KEY_ALIAS + i);
                  conditions.add(String.join(" ", elements));
                }
              }
            });
  }

  private Map<String, AttributeValue> getRangeBindMap(Scan scan) {
    ValueBinder binder = new ValueBinder(DynamoOperation.RANGE_KEY_ALIAS);
    List<Value<?>> start = scan.getStartClusteringKey().get().get();
    List<Value<?>> end = scan.getEndClusteringKey().get().get();
    start.get(start.size() - 1).accept(binder);
    end.get(end.size() - 1).accept(binder);

    return binder.build();
  }

  private Map<String, AttributeValue> getStartBindMap(Scan scan, boolean isRangeEnabled) {
    ValueBinder binder = new ValueBinder(DynamoOperation.START_CLUSTERING_KEY_ALIAS);
    scan.getStartClusteringKey()
        .ifPresent(
            k -> {
              List<Value<?>> start = k.get();
              int size = isRangeEnabled ? start.size() - 1 : start.size();
              IntStream.range(0, size).forEach(i -> start.get(i).accept(binder));
            });

    return binder.build();
  }

  private Map<String, AttributeValue> getEndBindMap(Scan scan, boolean isRangeEnabled) {
    ValueBinder binder = new ValueBinder(DynamoOperation.END_CLUSTERING_KEY_ALIAS);
    scan.getEndClusteringKey()
        .ifPresent(
            k -> {
              List<Value<?>> end = k.get();
              int size = isRangeEnabled ? end.size() - 1 : end.size();
              IntStream.range(0, size).forEach(i -> end.get(i).accept(binder));
            });

    return binder.build();
  }
}
