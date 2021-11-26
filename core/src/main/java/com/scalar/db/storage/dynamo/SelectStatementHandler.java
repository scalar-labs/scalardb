package com.scalar.db.storage.dynamo;

import com.google.common.primitives.UnsignedBytes;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Get;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import com.scalar.db.storage.common.TableMetadataManager;
import com.scalar.db.storage.dynamo.bytes.BytesUtils;
import com.scalar.db.storage.dynamo.bytes.KeyBytesEncoder;
import com.scalar.db.util.Utility;
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

    if (scan.getStartClusteringKey().isPresent() && scan.getEndClusteringKey().isPresent()) {
      if (!setBetweenCondition(
          scan.getStartClusteringKey().get(),
          scan.getStartInclusive(),
          scan.getEndClusteringKey().get(),
          scan.getEndInclusive(),
          tableMetadata,
          conditions,
          bindMap)) {
        return false;
      }
    } else {
      if (scan.getStartClusteringKey().isPresent()) {
        Key startKey = scan.getStartClusteringKey().get();
        if (startKey.size() == 1) {
          setStartCondition(startKey, scan.getStartInclusive(), tableMetadata, conditions, bindMap);
        } else {
          // if a start key with multiple values specified and no end key specified, use between
          // condition and use a key based on the start key without the last value as an end key
          Key endKey = getKeyWithoutLastValue(startKey);
          if (!setBetweenCondition(
              startKey,
              scan.getStartInclusive(),
              endKey,
              true,
              tableMetadata,
              conditions,
              bindMap)) {
            return false;
          }
        }
      }

      if (scan.getEndClusteringKey().isPresent()) {
        Key endKey = scan.getEndClusteringKey().get();
        if (endKey.size() == 1) {
          setEndCondition(endKey, scan.getEndInclusive(), tableMetadata, conditions, bindMap);
        } else {
          // if an end key with multiple values specified and no start key specified, use between
          // condition and use a key based on the end key without the last value as a start key
          Key startKey = getKeyWithoutLastValue(endKey);
          if (!setBetweenCondition(
              startKey, true, endKey, scan.getEndInclusive(), tableMetadata, conditions, bindMap)) {
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

    Optional<ByteBuffer> startKeyBytes =
        getStartKeyBytesForBetweenCondition(startKey, startInclusive, tableMetadata);
    Optional<ByteBuffer> endKeyBytes =
        getEndKeyBytesForBetweenCondition(endKey, endInclusive, tableMetadata);

    if (!startKeyBytes.isPresent() || !endKeyBytes.isPresent()) {
      if (startKeyBytes.isPresent()) {
        setStartCondition(startKey, startInclusive, tableMetadata, conditions, bindMap);
      }
      if (endKeyBytes.isPresent()) {
        setEndCondition(endKey, endInclusive, tableMetadata, conditions, bindMap);
      }
      return true;
    }

    byte[] start = BytesUtils.toBytes(startKeyBytes.get());
    byte[] end = BytesUtils.toBytes(endKeyBytes.get());
    if (UnsignedBytes.lexicographicalComparator().compare(start, end) > 0) {
      // if the start key bytes are greater than the end key bytes, return false. This situation
      // could happen when full clustering keys specified and scanning exclusively
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

  private Optional<ByteBuffer> getStartKeyBytesForBetweenCondition(
      Key startKey, boolean startInclusive, TableMetadata tableMetadata) {
    ByteBuffer startKeyBytes = getKeyBytes(startKey, tableMetadata);
    if (startInclusive) {
      return Optional.of(startKeyBytes);
    } else {
      // if exclusive scan for the start clustering key, we use the closest next Bytes of the
      // start clustering key bytes
      return BytesUtils.getClosestNextBytes(startKeyBytes);
    }
  }

  private Optional<ByteBuffer> getEndKeyBytesForBetweenCondition(
      Key endKey, boolean endInclusive, TableMetadata tableMetadata) {
    ByteBuffer endKeyBytes = getKeyBytes(endKey, tableMetadata);

    boolean fullEndClusteringKeySpecified =
        endKey.size() == tableMetadata.getClusteringKeyNames().size();
    if (fullEndClusteringKeySpecified) {
      if (endInclusive) {
        return Optional.of(endKeyBytes);
      } else {
        // if full end key specified, and it's an exclusive scan for the end key, we use the closest
        // previous bytes of the end key bytes for the between condition
        return BytesUtils.getClosestPreviousBytes(endKeyBytes);
      }
    } else {
      if (endInclusive) {
        // if partial end key specified, and it's an inclusive scan for the end key, we use the
        // closest next bytes of the end key bytes for the between condition
        return BytesUtils.getClosestNextBytes(endKeyBytes);
      } else {
        return Optional.of(endKeyBytes);
      }
    }
  }

  private void setStartCondition(
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
        BytesUtils.getClosestNextBytes(startKeyBytes)
            .ifPresent(
                k -> {
                  conditions.add(
                      DynamoOperation.CLUSTERING_KEY
                          + " >= "
                          + DynamoOperation.START_CLUSTERING_KEY_ALIAS);
                  bindMap.put(
                      DynamoOperation.START_CLUSTERING_KEY_ALIAS,
                      AttributeValue.builder().b(SdkBytes.fromByteBuffer(k)).build());
                });
      }
    }
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
}
