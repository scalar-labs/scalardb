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

    setForPartitionKey(scan, tableMetadata, conditions, bindMap);

    if (scan.getStartClusteringKey().isPresent() && scan.getEndClusteringKey().isPresent()) {
      if (!setForBetween(scan, tableMetadata, conditions, bindMap)) {
        // setForBetween() fails, then return false
        return false;
      }
    } else {
      if (scan.getStartClusteringKey().isPresent()) {
        setForStart(scan, tableMetadata, conditions, bindMap);
      }
      if (scan.getEndClusteringKey().isPresent()) {
        setForEnd(scan, tableMetadata, conditions, bindMap);
      }
    }

    builder
        .keyConditionExpression(String.join(" AND ", conditions))
        .expressionAttributeValues(bindMap);
    return true;
  }

  private void setForPartitionKey(
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

  private boolean setForBetween(
      Scan scan,
      TableMetadata tableMetadata,
      List<String> conditions,
      Map<String, AttributeValue> bindMap) {
    assert scan.getStartClusteringKey().isPresent() && scan.getEndClusteringKey().isPresent();

    Optional<ByteBuffer> startKeyBytes = getStartKeyBytesForBetween(scan, tableMetadata);
    Optional<ByteBuffer> endKeyBytes = getEndKeyBytesForBetween(scan, tableMetadata);

    if (!startKeyBytes.isPresent() || !endKeyBytes.isPresent()) {
      if (startKeyBytes.isPresent()) {
        setForStart(scan, tableMetadata, conditions, bindMap);
      }
      if (endKeyBytes.isPresent()) {
        setForEnd(scan, tableMetadata, conditions, bindMap);
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

  private Optional<ByteBuffer> getStartKeyBytesForBetween(Scan scan, TableMetadata tableMetadata) {
    assert scan.getStartClusteringKey().isPresent();

    ByteBuffer startKeyBytes = getKeyBytes(scan.getStartClusteringKey().get(), tableMetadata);
    if (scan.getStartInclusive()) {
      return Optional.of(startKeyBytes);
    } else {
      // if exclusive scan for the start clustering key, we use the closest next Bytes of the start
      // clustering key bytes
      return BytesUtils.getClosestNextBytes(startKeyBytes);
    }
  }

  private Optional<ByteBuffer> getEndKeyBytesForBetween(Scan scan, TableMetadata tableMetadata) {
    assert scan.getEndClusteringKey().isPresent();

    ByteBuffer endKeyBytes = getKeyBytes(scan.getEndClusteringKey().get(), tableMetadata);

    boolean fullEndClusteringKeySpecified =
        scan.getEndClusteringKey().get().size() == tableMetadata.getClusteringKeyNames().size();
    if (fullEndClusteringKeySpecified) {
      if (scan.getEndInclusive()) {
        return Optional.of(endKeyBytes);
      } else {
        // if full end clustering key specified, and it's an exclusive scan for the end clustering
        // key, we use the closest previous bytes of the end clustering key bytes for the between
        // condition
        return BytesUtils.getClosestPreviousBytes(endKeyBytes);
      }
    } else {
      if (scan.getEndInclusive()) {
        // if partial end clustering key specified, and it's an inclusive scan for the end
        // clustering key, we use the closest next bytes of the end clustering key bytes for the
        // between condition
        return BytesUtils.getClosestNextBytes(endKeyBytes);
      } else {
        return Optional.of(endKeyBytes);
      }
    }
  }

  private void setForStart(
      Scan scan,
      TableMetadata tableMetadata,
      List<String> conditions,
      Map<String, AttributeValue> bindMap) {
    assert scan.getStartClusteringKey().isPresent();

    ByteBuffer startKeyBytes = getKeyBytes(scan.getStartClusteringKey().get(), tableMetadata);

    boolean fullClusteringKeySpecified =
        scan.getStartClusteringKey().get().size() == tableMetadata.getClusteringKeyNames().size();
    if (fullClusteringKeySpecified) {
      conditions.add(
          DynamoOperation.CLUSTERING_KEY
              + (scan.getStartInclusive() ? " >= " : " > ")
              + DynamoOperation.START_CLUSTERING_KEY_ALIAS);
      bindMap.put(
          DynamoOperation.START_CLUSTERING_KEY_ALIAS,
          AttributeValue.builder().b(SdkBytes.fromByteBuffer(startKeyBytes)).build());
    } else {
      if (scan.getStartInclusive()) {
        conditions.add(
            DynamoOperation.CLUSTERING_KEY + " >= " + DynamoOperation.START_CLUSTERING_KEY_ALIAS);
        bindMap.put(
            DynamoOperation.START_CLUSTERING_KEY_ALIAS,
            AttributeValue.builder().b(SdkBytes.fromByteBuffer(startKeyBytes)).build());
      } else {
        // if partial start clustering key specified, and it's an exclusive scan for the start
        // clustering key, we use the closest next bytes of the start clustering key bytes for the
        // grater than or equal condition
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

  private void setForEnd(
      Scan scan,
      TableMetadata tableMetadata,
      List<String> conditions,
      Map<String, AttributeValue> bindMap) {
    assert scan.getEndClusteringKey().isPresent();

    ByteBuffer endKeyBytes = getKeyBytes(scan.getEndClusteringKey().get(), tableMetadata);

    boolean fullClusteringKeySpecified =
        scan.getEndClusteringKey().get().size() == tableMetadata.getClusteringKeyNames().size();
    if (fullClusteringKeySpecified) {
      conditions.add(
          DynamoOperation.CLUSTERING_KEY
              + (scan.getEndInclusive() ? " <= " : " < ")
              + DynamoOperation.END_CLUSTERING_KEY_ALIAS);
      bindMap.put(
          DynamoOperation.END_CLUSTERING_KEY_ALIAS,
          AttributeValue.builder().b(SdkBytes.fromByteBuffer(endKeyBytes)).build());
    } else {
      if (scan.getEndInclusive()) {
        // if partial end clustering key specified, and it's an inclusive scan for the end
        // clustering key, we use the closest next bytes of the start clustering key bytes for the
        // less than condition
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
