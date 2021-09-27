package com.scalar.db.storage.dynamo;

import com.scalar.db.api.Consistency;
import com.scalar.db.api.Get;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.Selection;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.Value;
import com.scalar.db.util.Utility;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 * @author Yuji Ito, Pham Ba Thong
 */
@ThreadSafe
public class SelectStatementHandler extends StatementHandler {

  /**
   * Constructs a {@code SelectStatementHandler} with the specified {@link DynamoDbClient} and a new
   * {@link DynamoTableMetadataManager}
   *
   * @param client {@code DynamoDbClient}
   * @param metadataManager {@code TableMetadataManager}
   */
  public SelectStatementHandler(DynamoDbClient client, DynamoTableMetadataManager metadataManager) {
    super(client, metadataManager);
  }

  @Nonnull
  @Override
  public List<Map<String, AttributeValue>> handle(Operation operation) throws ExecutionException {
    checkArgument(operation, Get.class, Scan.class);

    try {
      if (Utility.isSecondaryIndexSpecified(
          operation, metadataManager.getTableMetadata(operation))) {
        return executeQueryWithIndex((Selection) operation);
      }

      if (operation instanceof Get) {
        return executeGet((Get) operation);
      } else {
        return executeQuery((Scan) operation);
      }
    } catch (DynamoDbException e) {
      throw new ExecutionException(e.getMessage(), e);
    }
  }

  private List<Map<String, AttributeValue>> executeGet(Get get) {
    DynamoOperation dynamoOperation = new DynamoOperation(get, metadataManager);

    GetItemRequest.Builder builder =
        GetItemRequest.builder()
            .tableName(dynamoOperation.getTableName())
            .key(dynamoOperation.getKeyMap());

    if (!get.getProjections().isEmpty()) {
      projectionExpression(builder, get);
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

  private List<Map<String, AttributeValue>> executeQueryWithIndex(Selection selection) {
    DynamoOperation dynamoOperation = new DynamoOperation(selection, metadataManager);
    Value<?> keyValue = selection.getPartitionKey().get().get(0);
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
      projectionExpression(builder, selection);
    }

    if (selection instanceof Scan) {
      Scan scan = (Scan) selection;
      if (scan.getLimit() > 0) {
        builder.limit(scan.getLimit());
      }
    }

    QueryResponse queryResponse = client.query(builder.build());
    return new ArrayList<>(queryResponse.items());
  }

  private List<Map<String, AttributeValue>> executeQuery(Scan scan) {
    DynamoOperation dynamoOperation = new DynamoOperation(scan, metadataManager);
    QueryRequest.Builder builder = QueryRequest.builder().tableName(dynamoOperation.getTableName());

    setConditions(builder, scan);

    if (!scan.getOrderings().isEmpty()) {
      for (Ordering o : scan.getOrderings()) {
        builder.scanIndexForward(
            !dynamoOperation.getMetadata().getClusteringKeyNames().contains(o.getName())
                || o.getOrder() != Order.DESC);
      }
    }

    if (scan.getLimit() > 0) {
      builder.limit(scan.getLimit());
    }

    if (!scan.getProjections().isEmpty()) {
      projectionExpression(builder, scan);
    }

    if (scan.getConsistency() != Consistency.EVENTUAL) {
      builder.consistentRead(true);
    }

    QueryResponse queryResponse = client.query(builder.build());
    return new ArrayList<>(queryResponse.items());
  }

  private void projectionExpression(DynamoDbRequest.Builder builder, Selection selection) {
    assert builder instanceof GetItemRequest.Builder || builder instanceof QueryRequest.Builder;

    Map<String, String> columnMap = new HashMap<>();
    List<String> projections = new ArrayList<>(selection.getProjections().size());
    for (int i = 0; i < selection.getProjections().size(); i++) {
      String alias = DynamoOperation.COLUMN_NAME_ALIAS + i;
      projections.add(alias);
      columnMap.put(alias, selection.getProjections().get(i));
    }
    String projectionExpression = String.join(",", projections);

    if (builder instanceof GetItemRequest.Builder) {
      ((GetItemRequest.Builder) builder).projectionExpression(projectionExpression);
      ((GetItemRequest.Builder) builder).expressionAttributeNames(columnMap);
    } else {
      ((QueryRequest.Builder) builder).projectionExpression(projectionExpression);
      ((QueryRequest.Builder) builder).expressionAttributeNames(columnMap);
    }
  }

  private void setConditions(QueryRequest.Builder builder, Scan scan) {
    List<String> conditions = new ArrayList<>();

    conditions.add(getPartitionKeyCondition());

    boolean isRangeEnabled = setRangeConditionOnConcatenatedClusteringKey(scan, conditions);
    setStartConditionOnConcatenatedClusteringKey(scan, conditions, isRangeEnabled);
    setEndConditionOnConcatenatedClusteringKey(scan, conditions, isRangeEnabled);
    String keyConditions = String.join(" AND ", conditions);

    Map<String, AttributeValue> bindMap = getPartitionKeyBindMap(scan);
    if (isRangeEnabled) {
      bindMap.putAll(getRangeBindMapForConcatenatedClusteringKey(scan));
    }
    bindMap.putAll(getStartBindMapOnConcatenatedClusteringKey(scan, isRangeEnabled));
    bindMap.putAll(getEndBindMapOnConcatenatedClusteringKey(scan, isRangeEnabled));

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

  private boolean setRangeConditionOnConcatenatedClusteringKey(Scan scan, List<String> conditions) {
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
      conditions.add(DynamoOperation.CLUSTERING_KEY + DynamoOperation.RANGE_CONDITION);
      return true;
    } else {
      return false;
    }
  }

  private void setStartConditionOnConcatenatedClusteringKey(
      Scan scan, List<String> conditions, boolean isRangeEnabled) {
    scan.getStartClusteringKey()
        .ifPresent(
            k -> {
              List<String> elements = new ArrayList<>();
              elements.add(DynamoOperation.CLUSTERING_KEY);
              if (!isRangeEnabled) {
                if (scan.getStartInclusive()) {
                  elements.add(">=");
                } else {
                  elements.add(">");
                }
                elements.add(DynamoOperation.START_CLUSTERING_KEY_ALIAS + "0");
                conditions.add(String.join(" ", elements));
              }
            });
  }

  private void setEndConditionOnConcatenatedClusteringKey(
      Scan scan, List<String> conditions, boolean isRangeEnabled) {
    scan.getEndClusteringKey()
        .ifPresent(
            k -> {
              List<String> elements = new ArrayList<>();
              elements.add(DynamoOperation.CLUSTERING_KEY);
              if (!isRangeEnabled) {
                if (scan.getEndInclusive()) {
                  elements.add("<=");
                } else {
                  elements.add("<");
                }
                elements.add(DynamoOperation.END_CLUSTERING_KEY_ALIAS + "0");
                conditions.add(String.join(" ", elements));
              }
            });
  }

  private Map<String, AttributeValue> getRangeBindMapForConcatenatedClusteringKey(Scan scan) {
    ValueBinder binder = new ValueBinder(DynamoOperation.RANGE_KEY_ALIAS);

    OrderedConcatenationVisitor startConcatenatedClusteringKeyValueVisitor =
        new OrderedConcatenationVisitor();
    List<Value<?>> start = scan.getStartClusteringKey().get().get();
    for (Value<?> s : start) {
      s.accept(startConcatenatedClusteringKeyValueVisitor);
    }
    OrderedConcatenationVisitor endConcatenatedClusteringKeyValueVisitor =
        new OrderedConcatenationVisitor();
    List<Value<?>> end = scan.getEndClusteringKey().get().get();
    for (Value<?> e : end) {
      e.accept(endConcatenatedClusteringKeyValueVisitor);
    }

    binder.visit(new BlobValue(startConcatenatedClusteringKeyValueVisitor.buildAsStartInclusive()));
    binder.visit(new BlobValue(endConcatenatedClusteringKeyValueVisitor.buildAsEndInclusive()));

    return binder.build();
  }

  private Map<String, AttributeValue> getStartBindMapOnConcatenatedClusteringKey(
      Scan scan, boolean isRangeEnabled) {
    if (isRangeEnabled || !scan.getStartClusteringKey().isPresent()) {
      return Collections.emptyMap();
    } else {
      ValueBinder binder = new ValueBinder(DynamoOperation.START_CLUSTERING_KEY_ALIAS);
      OrderedConcatenationVisitor startValueVisitor = new OrderedConcatenationVisitor();
      List<Value<?>> start = scan.getStartClusteringKey().get().get();
      for (Value<?> s : start) {
        s.accept(startValueVisitor);
      }
      byte[] value;
      if (scan.getStartInclusive()) {
        value = startValueVisitor.buildAsStartInclusive();
      } else {
        value = startValueVisitor.buildAsStartExclusive();
      }
      binder.visit(new BlobValue(value));
      return binder.build();
    }
  }

  private Map<String, AttributeValue> getEndBindMapOnConcatenatedClusteringKey(
      Scan scan, boolean isRangeEnabled) {
    if (isRangeEnabled || !scan.getEndClusteringKey().isPresent()) {
      return Collections.emptyMap();
    } else {
      ValueBinder binder = new ValueBinder(DynamoOperation.END_CLUSTERING_KEY_ALIAS);
      OrderedConcatenationVisitor endValueVisitor = new OrderedConcatenationVisitor();
      List<Value<?>> end = scan.getEndClusteringKey().get().get();
      for (Value<?> e : end) {
        e.accept(endValueVisitor);
      }
      byte[] value;
      if (scan.getEndInclusive()) {
        value = endValueVisitor.buildAsEndInclusive();
      } else {
        value = endValueVisitor.buildAsEndExclusive();
      }
      binder.visit(new BlobValue(value));
      return binder.build();
    }
  }
}
