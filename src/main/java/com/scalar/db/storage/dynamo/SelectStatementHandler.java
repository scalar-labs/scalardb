package com.scalar.db.storage.dynamo;

import com.scalar.db.api.Consistency;
import com.scalar.db.api.Get;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Selection;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Value;
import com.scalar.db.storage.Utility;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
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
  private static final Logger LOGGER = LoggerFactory.getLogger(SelectStatementHandler.class);

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

  @Override
  public List<Map<String, AttributeValue>> handle(Operation operation) throws ExecutionException {
    checkArgument(operation, Get.class, Scan.class);

    try {
      if (Utility.isSecondaryIndexSpecified(
          operation, metadataManager.getTableMetadata(operation))) {
        // convert to a mutable list for the Scanner
        return new ArrayList<>(executeQueryWithIndex((Selection) operation).items());
      }

      if (operation instanceof Get) {
        GetItemResponse response = executeGet((Get) operation);
        if (response.hasItem()) {
          return Arrays.asList(response.item());
        } else {
          return Collections.emptyList();
        }
      } else {
        // convert to a mutable list for the Scanner
        return new ArrayList<>(executeQuery((Scan) operation).items());
      }
    } catch (DynamoDbException e) {
      throw new ExecutionException(e.getMessage(), e);
    }
  }

  private GetItemResponse executeGet(Get get) {
    DynamoOperation dynamoOperation = new DynamoOperation(get, metadataManager);

    GetItemRequest.Builder builder =
        GetItemRequest.builder()
            .tableName(dynamoOperation.getTableName())
            .key(dynamoOperation.getKeyMap());

    if (!get.getProjections().isEmpty()) {
      builder.projectionExpression(String.join(",", get.getProjections()));
    }

    if (get.getConsistency() != Consistency.EVENTUAL) {
      builder.consistentRead(true);
    }

    return client.getItem(builder.build());
  }

  private QueryResponse executeQueryWithIndex(Selection selection) {
    DynamoOperation dynamoOperation = new DynamoOperation(selection, metadataManager);
    Value keyValue = selection.getPartitionKey().get().get(0);
    String column = keyValue.getName();
    String indexTable = dynamoOperation.getGlobalIndexName(column);
    QueryRequest.Builder builder =
        QueryRequest.builder().tableName(dynamoOperation.getTableName()).indexName(indexTable);

    String condition = column + " = " + DynamoOperation.VALUE_ALIAS + "0";
    ValueBinder binder = new ValueBinder(DynamoOperation.VALUE_ALIAS);
    keyValue.accept(binder);
    Map<String, AttributeValue> bindMap = binder.build();
    builder.keyConditionExpression(condition).expressionAttributeValues(bindMap);

    if (!selection.getProjections().isEmpty()) {
      builder.projectionExpression(String.join(",", selection.getProjections()));
    }

    if (selection instanceof Scan) {
      Scan scan = (Scan) selection;
      if (scan.getLimit() > 0) {
        builder.limit(scan.getLimit());
      }
    }

    return client.query(builder.build());
  }

  private QueryResponse executeQuery(Scan scan) {
    DynamoOperation dynamoOperation = new DynamoOperation(scan, metadataManager);
    QueryRequest.Builder builder = QueryRequest.builder().tableName(dynamoOperation.getTableName());

    getIndexName(scan)
        .ifPresent(
            name -> {
              String indexTableName = dynamoOperation.getIndexName(name);
              builder.indexName(indexTableName);
            });

    setConditions(builder, scan);

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
      builder.projectionExpression(String.join(",", scan.getProjections()));
    }

    if (scan.getConsistency() != Consistency.EVENTUAL) {
      builder.consistentRead(true);
    }

    return client.query(builder.build());
  }

  private Optional<String> getIndexName(Scan scan) {
    if (scan.getStartClusteringKey().isPresent()) {
      List<Value> start = scan.getStartClusteringKey().get().get();
      return Optional.of(start.get(start.size() - 1).getName());
    }

    if (scan.getEndClusteringKey().isPresent()) {
      List<Value> end = scan.getEndClusteringKey().get().get();
      return Optional.of(end.get(end.size() - 1).getName());
    }

    return Optional.empty();
  }

  private void setConditions(QueryRequest.Builder builder, Scan scan) {
    List<String> conditions = new ArrayList<>();
    List<String> filters = new ArrayList<>();

    conditions.add(getPartitionKeyCondition());

    boolean isRangeEnabled = setRangeCondition(scan, conditions);
    setStartCondition(scan, conditions, filters, isRangeEnabled);
    setEndCondition(scan, conditions, filters, isRangeEnabled);
    String keyConditions = String.join(" AND ", conditions);

    Map<String, AttributeValue> bindMap = getPartitionKeyBindMap(scan);
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

  private Map<String, AttributeValue> getPartitionKeyBindMap(Scan scan) {
    Map<String, AttributeValue> bindMap = new HashMap<>();
    DynamoOperation dynamoOperation = new DynamoOperation(scan, metadataManager);
    String partitionKey = dynamoOperation.getConcatenatedPartitionKey();

    bindMap.put(
        DynamoOperation.PARTITION_KEY_ALIAS, AttributeValue.builder().s(partitionKey).build());

    return bindMap;
  }

  private boolean setRangeCondition(Scan scan, List<String> conditions) {
    if (!scan.getStartClusteringKey().isPresent() || !scan.getEndClusteringKey().isPresent()) {
      return false;
    }

    List<Value> start = scan.getStartClusteringKey().get().get();
    List<Value> end = scan.getEndClusteringKey().get().get();
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
    DynamoOperation dynamoOperation = new DynamoOperation(scan, metadataManager);
    scan.getStartClusteringKey()
        .ifPresent(
            k -> {
              List<Value> start = k.get();
              for (int i = 0; i < start.size(); i++) {
                Value value = start.get(i);
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
    DynamoOperation dynamoOperation = new DynamoOperation(scan, metadataManager);
    scan.getEndClusteringKey()
        .ifPresent(
            k -> {
              List<Value> end = k.get();
              for (int i = 0; i < end.size(); i++) {
                Value value = end.get(i);
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
    List<Value> start = scan.getStartClusteringKey().get().get();
    List<Value> end = scan.getEndClusteringKey().get().get();
    start.get(start.size() - 1).accept(binder);
    end.get(end.size() - 1).accept(binder);

    return binder.build();
  }

  private Map<String, AttributeValue> getStartBindMap(Scan scan, boolean isRangeEnabled) {
    ValueBinder binder = new ValueBinder(DynamoOperation.START_CLUSTERING_KEY_ALIAS);
    scan.getStartClusteringKey()
        .ifPresent(
            k -> {
              List<Value> start = k.get();
              int size = isRangeEnabled ? start.size() - 1 : start.size();
              IntStream.range(0, size)
                  .forEach(
                      i -> {
                        start.get(i).accept(binder);
                      });
            });

    return binder.build();
  }

  private Map<String, AttributeValue> getEndBindMap(Scan scan, boolean isRangeEnabled) {
    ValueBinder binder = new ValueBinder(DynamoOperation.END_CLUSTERING_KEY_ALIAS);
    scan.getEndClusteringKey()
        .ifPresent(
            k -> {
              List<Value> end = k.get();
              int size = isRangeEnabled ? end.size() - 1 : end.size();
              IntStream.range(0, size)
                  .forEach(
                      i -> {
                        end.get(i).accept(binder);
                      });
            });

    return binder.build();
  }
}
