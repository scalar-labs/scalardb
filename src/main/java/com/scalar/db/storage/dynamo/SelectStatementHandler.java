package com.scalar.db.storage.dynamo;

import com.scalar.db.api.Consistency;
import com.scalar.db.api.Get;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Scan;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Value;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

  private QueryResponse executeQuery(Scan scan) {
    DynamoOperation dynamoOperation = new DynamoOperation(scan, metadataManager);
    QueryRequest.Builder builder = QueryRequest.builder().tableName(dynamoOperation.getTableName());

    setConditions(builder, scan);

    if (!scan.getOrderings().isEmpty()) {
      scan.getOrderings()
          .forEach(
              o -> {
                if (dynamoOperation.isSortKey(o.getName())
                    && o.getOrder() == Scan.Ordering.Order.DESC) {
                  builder.scanIndexForward(false);
                }
              });
    }

    if (scan.getLimit() > 0) {
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

  private void setConditions(QueryRequest.Builder builder, Scan scan) {
    List<String> conditions = new ArrayList<>();

    conditions.add(getPartitionKeyCondition());

    boolean isRangeEnabled = setRangeCondition(scan, conditions);
    if (!isRangeEnabled) {
      setStartCondition(scan, conditions);
      setEndCondition(scan, conditions);
    }
    String keyConditions = String.join(" AND ", conditions);

    Map<String, AttributeValue> bindMap = getPartitionKeyBindMap(scan);
    if (isRangeEnabled) {
      bindMap.putAll(getRangeBindMap(scan));
    }
    bindMap.putAll(getStartBindMap(scan, isRangeEnabled));
    bindMap.putAll(getEndBindMap(scan, isRangeEnabled));

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

    DynamoOperation dynamoOperation = new DynamoOperation(scan, metadataManager);

    if (startKeyName.equals(endKeyName)) {
      conditions.add(startKeyName + DynamoOperation.RANGE_CONDITION);
      if (!scan.getStartInclusive() || !scan.getEndInclusive()) {
        throw new IllegalArgumentException(
            "DynamoDB does NOT support the range scan with the exclusiving option");
      }
      return true;
    } else {
      return false;
    }
  }

  private void setStartCondition(Scan scan, List<String> conditions) {
    DynamoOperation dynamoOperation = new DynamoOperation(scan, metadataManager);
    scan.getStartClusteringKey()
        .ifPresent(
            k -> {
              if (k.get().size() > 1) {
                throw new IllegalArgumentException(
                    "DynamoDB doesn't suuport multiple clustering keys.");
              }
              Value value = k.get().get(0);
              List<String> elements = new ArrayList<>();
              elements.add(value.getName());
              if (scan.getStartInclusive()) {
                elements.add(">=");
              } else {
                elements.add(">");
              }
              elements.add(DynamoOperation.START_CLUSTERING_KEY_ALIAS + "0");
              conditions.add(String.join(" ", elements));
            });
  }

  private void setEndCondition(Scan scan, List<String> conditions) {
    DynamoOperation dynamoOperation = new DynamoOperation(scan, metadataManager);
    scan.getEndClusteringKey()
        .ifPresent(
            k -> {
              if (k.get().size() > 1) {
                throw new IllegalArgumentException(
                    "DynamoDB doesn't suuport multiple clustering keys.");
              }
              Value value = k.get().get(0);
              List<String> elements = new ArrayList<>();
              elements.add(value.getName());
              if (scan.getStartInclusive()) {
                elements.add("<=");
              } else {
                elements.add("<");
              }
              elements.add(DynamoOperation.START_CLUSTERING_KEY_ALIAS + "0");
              conditions.add(String.join(" ", elements));
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
