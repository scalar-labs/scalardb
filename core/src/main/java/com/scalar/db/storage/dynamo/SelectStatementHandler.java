package com.scalar.db.storage.dynamo;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.UnsignedBytes;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Get;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import com.scalar.db.storage.dynamo.bytes.BytesUtils;
import com.scalar.db.storage.dynamo.bytes.KeyBytesEncoder;
import com.scalar.db.util.ScalarDbUtils;
import com.scalar.db.util.TableMetadataManager;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import software.amazon.awssdk.core.SdkBytes;
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
 * @author Yuji Ito, Toshihiro Suzuki
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
      if (ScalarDbUtils.isSecondaryIndexSpecified(operation, tableMetadata)) {
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
    builder
        .keyConditionExpression(condition)
        .expressionAttributeValues(bindMap)
        .expressionAttributeNames(ImmutableMap.of(expressionColumnName, column));

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

  private List<Map<String, AttributeValue>> executeQuery(Scan scan, TableMetadata tableMetadata) {
    DynamoOperation dynamoOperation = new DynamoOperation(scan, tableMetadata);
    QueryRequest.Builder builder = QueryRequest.builder().tableName(dynamoOperation.getTableName());

    if (!setConditions(builder, scan, tableMetadata)) {
      // if setConditions() fails, return empty list
      return new ArrayList<>();
    }

    if (!scan.getOrderings().isEmpty()) {
      Ordering ordering = scan.getOrderings().get(0);
      if (ordering.getOrder() != tableMetadata.getClusteringOrder(ordering.getName())) {
        // reverse scan
        builder.scanIndexForward(false);
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

  private boolean setConditions(
      QueryRequest.Builder builder, Scan scan, TableMetadata tableMetadata) {
    List<String> conditions = new ArrayList<>();
    Map<String, AttributeValue> bindMap = new HashMap<>();

    setConditionForPartitionKey(scan, tableMetadata, conditions, bindMap);

    // If the scan is for DESC clustering order, use the end clustering key as a start key and the
    // start clustering key as an end key
    boolean scanForDescClusteringOrder = isScanForDescClusteringOrder(scan, tableMetadata);
    Optional<Key> startKey =
        scanForDescClusteringOrder ? scan.getEndClusteringKey() : scan.getStartClusteringKey();
    boolean startInclusive =
        scanForDescClusteringOrder ? scan.getEndInclusive() : scan.getStartInclusive();
    Optional<Key> endKey =
        scanForDescClusteringOrder ? scan.getStartClusteringKey() : scan.getEndClusteringKey();
    boolean endInclusive =
        scanForDescClusteringOrder ? scan.getStartInclusive() : scan.getEndInclusive();

    if (startKey.isPresent() && endKey.isPresent()) {
      if (!setBetweenCondition(
          startKey.get(),
          startInclusive,
          endKey.get(),
          endInclusive,
          tableMetadata,
          conditions,
          bindMap)) {
        return false;
      }
    } else {
      if (startKey.isPresent()) {
        if (startKey.get().size() == 1) {
          if (!setStartCondition(
              startKey.get(), startInclusive, tableMetadata, conditions, bindMap)) {
            return false;
          }
        } else {
          // if a start key with multiple values specified and no end key specified, use between
          // condition and use a key based on the start key without the last value as an end key
          if (!setBetweenCondition(
              startKey.get(),
              startInclusive,
              getKeyWithoutLastValue(startKey.get()),
              true,
              tableMetadata,
              conditions,
              bindMap)) {
            return false;
          }
        }
      }

      if (endKey.isPresent()) {
        if (endKey.get().size() == 1) {
          setEndCondition(endKey.get(), endInclusive, tableMetadata, conditions, bindMap);
        } else {
          // if an end key with multiple values specified and no start key specified, use between
          // condition and use a key based on the end key without the last value as a start key
          if (!setBetweenCondition(
              getKeyWithoutLastValue(endKey.get()),
              true,
              endKey.get(),
              endInclusive,
              tableMetadata,
              conditions,
              bindMap)) {
            return false;
          }
        }
      }
    }

    builder
        .keyConditionExpression(String.join(" AND ", conditions))
        .expressionAttributeValues(bindMap);
    return true;
  }

  private Key getKeyWithoutLastValue(Key originalKey) {
    Key.Builder keyBuilder = Key.newBuilder();
    for (int i = 0; i < originalKey.get().size() - 1; i++) {
      keyBuilder.add(originalKey.get().get(i));
    }
    return keyBuilder.build();
  }

  private void setConditionForPartitionKey(
      Scan scan,
      TableMetadata tableMetadata,
      List<String> conditions,
      Map<String, AttributeValue> bindMap) {
    conditions.add(DynamoOperation.PARTITION_KEY + " = " + DynamoOperation.PARTITION_KEY_ALIAS);

    DynamoOperation dynamoOperation = new DynamoOperation(scan, tableMetadata);
    ByteBuffer concatenatedPartitionKey = dynamoOperation.getConcatenatedPartitionKey();
    bindMap.put(
        DynamoOperation.PARTITION_KEY_ALIAS,
        AttributeValue.builder().b(SdkBytes.fromByteBuffer(concatenatedPartitionKey)).build());
  }

  private boolean setBetweenCondition(
      Key startKey,
      boolean startInclusive,
      Key endKey,
      boolean endInclusive,
      TableMetadata tableMetadata,
      List<String> conditions,
      Map<String, AttributeValue> bindMap) {

    ByteBuffer startKeyBytes = getKeyBytes(startKey, tableMetadata);
    if (!startInclusive) {
      // if exclusive scan for the start key, we use the closest next bytes of the start key bytes
      Optional<ByteBuffer> closestNextBytes = BytesUtils.getClosestNextBytes(startKeyBytes);
      if (!closestNextBytes.isPresent()) {
        // if we can't find the closest next bytes of the start key bytes, return false. That means
        // we should return empty results in this case
        return false;
      }
      startKeyBytes = closestNextBytes.get();
    }

    ByteBuffer endKeyBytes = getKeyBytes(endKey, tableMetadata);
    boolean fullClusteringKeySpecified =
        endKey.size() == tableMetadata.getClusteringKeyNames().size();
    if (fullClusteringKeySpecified) {
      if (!endInclusive) {
        // if full end key specified, and it's an exclusive scan for the end key, we use the closest
        // previous bytes of the end key bytes for the between condition
        Optional<ByteBuffer> closestPreviousBytes = BytesUtils.getClosestPreviousBytes(endKeyBytes);
        if (!closestPreviousBytes.isPresent()) {
          // if we can't find the closest previous bytes of the end key bytes, return false. That
          // means we should return empty results in this case
          return false;
        }
        endKeyBytes = closestPreviousBytes.get();
      }
    } else {
      if (endInclusive) {
        // if partial end key specified, and it's an inclusive scan for the end key, we use the
        // closest next bytes of the end key bytes for the between condition
        Optional<ByteBuffer> closestNextBytes = BytesUtils.getClosestNextBytes(endKeyBytes);
        if (!closestNextBytes.isPresent()) {
          // if we can't find the closest next bytes of the end key bytes, set start condition with
          // the start key
          return setStartCondition(startKey, startInclusive, tableMetadata, conditions, bindMap);
        }
        endKeyBytes = closestNextBytes.get();
      }
    }

    byte[] start = BytesUtils.toBytes(startKeyBytes);
    byte[] end = BytesUtils.toBytes(endKeyBytes);
    if (UnsignedBytes.lexicographicalComparator().compare(start, end) > 0) {
      // if the start key bytes are greater than the end key bytes, return false. That means we
      // should return empty results in this case. This situation could happen when full clustering
      // keys specified and scanning exclusively
      return false;
    }

    conditions.add(
        DynamoOperation.CLUSTERING_KEY
            + " BETWEEN "
            + DynamoOperation.START_CLUSTERING_KEY_ALIAS
            + " AND "
            + DynamoOperation.END_CLUSTERING_KEY_ALIAS);

    bindMap.put(
        DynamoOperation.START_CLUSTERING_KEY_ALIAS,
        AttributeValue.builder().b(SdkBytes.fromByteArray(start)).build());
    bindMap.put(
        DynamoOperation.END_CLUSTERING_KEY_ALIAS,
        AttributeValue.builder().b(SdkBytes.fromByteArray(end)).build());

    return true;
  }

  private boolean setStartCondition(
      Key startKey,
      boolean startInclusive,
      TableMetadata tableMetadata,
      List<String> conditions,
      Map<String, AttributeValue> bindMap) {
    ByteBuffer startKeyBytes = getKeyBytes(startKey, tableMetadata);

    boolean fullClusteringKeySpecified =
        startKey.size() == tableMetadata.getClusteringKeyNames().size();
    if (fullClusteringKeySpecified) {
      conditions.add(
          DynamoOperation.CLUSTERING_KEY
              + (startInclusive ? " >= " : " > ")
              + DynamoOperation.START_CLUSTERING_KEY_ALIAS);
      bindMap.put(
          DynamoOperation.START_CLUSTERING_KEY_ALIAS,
          AttributeValue.builder().b(SdkBytes.fromByteBuffer(startKeyBytes)).build());
    } else {
      if (startInclusive) {
        conditions.add(
            DynamoOperation.CLUSTERING_KEY + " >= " + DynamoOperation.START_CLUSTERING_KEY_ALIAS);
        bindMap.put(
            DynamoOperation.START_CLUSTERING_KEY_ALIAS,
            AttributeValue.builder().b(SdkBytes.fromByteBuffer(startKeyBytes)).build());
      } else {
        // if partial start key specified, and it's an exclusive scan for the start key, we use
        // the closest next bytes of the start key bytes for the grater than or equal condition
        Optional<ByteBuffer> closestNextBytes = BytesUtils.getClosestNextBytes(startKeyBytes);
        if (closestNextBytes.isPresent()) {
          conditions.add(
              DynamoOperation.CLUSTERING_KEY + " >= " + DynamoOperation.START_CLUSTERING_KEY_ALIAS);
          bindMap.put(
              DynamoOperation.START_CLUSTERING_KEY_ALIAS,
              AttributeValue.builder().b(SdkBytes.fromByteBuffer(closestNextBytes.get())).build());
        } else {
          // if we can't find the closest next bytes of the start key bytes, return false. That
          // means we should return empty results in this case
          return false;
        }
      }
    }
    return true;
  }

  private void setEndCondition(
      Key endKey,
      boolean endInclusive,
      TableMetadata tableMetadata,
      List<String> conditions,
      Map<String, AttributeValue> bindMap) {
    ByteBuffer endKeyBytes = getKeyBytes(endKey, tableMetadata);

    boolean fullClusteringKeySpecified =
        endKey.size() == tableMetadata.getClusteringKeyNames().size();
    if (fullClusteringKeySpecified) {
      conditions.add(
          DynamoOperation.CLUSTERING_KEY
              + (endInclusive ? " <= " : " < ")
              + DynamoOperation.END_CLUSTERING_KEY_ALIAS);
      bindMap.put(
          DynamoOperation.END_CLUSTERING_KEY_ALIAS,
          AttributeValue.builder().b(SdkBytes.fromByteBuffer(endKeyBytes)).build());
    } else {
      if (endInclusive) {
        // if partial end key specified, and it's an inclusive scan for the end key, we use the
        // closest next bytes of the end key bytes for the less than condition
        BytesUtils.getClosestNextBytes(endKeyBytes)
            .ifPresent(
                k -> {
                  conditions.add(
                      DynamoOperation.CLUSTERING_KEY
                          + " < "
                          + DynamoOperation.END_CLUSTERING_KEY_ALIAS);
                  bindMap.put(
                      DynamoOperation.END_CLUSTERING_KEY_ALIAS,
                      AttributeValue.builder().b(SdkBytes.fromByteBuffer(k)).build());
                });
      } else {
        conditions.add(
            DynamoOperation.CLUSTERING_KEY + " < " + DynamoOperation.END_CLUSTERING_KEY_ALIAS);
        bindMap.put(
            DynamoOperation.END_CLUSTERING_KEY_ALIAS,
            AttributeValue.builder().b(SdkBytes.fromByteBuffer(endKeyBytes)).build());
      }
    }
  }

  private ByteBuffer getKeyBytes(Key key, TableMetadata tableMetadata) {
    return new KeyBytesEncoder().encode(key, tableMetadata.getClusteringOrders());
  }

  private boolean isScanForDescClusteringOrder(Scan scan, TableMetadata tableMetadata) {
    if (scan.getStartClusteringKey().isPresent()) {
      Key startClusteringKey = scan.getStartClusteringKey().get();
      String lastValueName = startClusteringKey.get().get(startClusteringKey.size() - 1).getName();
      return tableMetadata.getClusteringOrder(lastValueName) == Order.DESC;
    }
    if (scan.getEndClusteringKey().isPresent()) {
      Key endClusteringKey = scan.getEndClusteringKey().get();
      String lastValueName = endClusteringKey.get().get(endClusteringKey.size() - 1).getName();
      return tableMetadata.getClusteringOrder(lastValueName) == Order.DESC;
    }
    return false;
  }
}
