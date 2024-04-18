package com.scalar.db.storage.dynamo;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.primitives.UnsignedBytes;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Get;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.ScanAll;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.EmptyScanner;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import com.scalar.db.storage.dynamo.bytes.BytesUtils;
import com.scalar.db.storage.dynamo.bytes.KeyBytesEncoder;
import com.scalar.db.util.ScalarDbUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

/**
 * A handler class for select statement
 *
 * @author Yuji Ito, Toshihiro Suzuki
 */
@ThreadSafe
public class SelectStatementHandler {
  private final DynamoDbClient client;
  private final TableMetadataManager metadataManager;
  private final String namespacePrefix;

  /**
   * Constructs a {@code SelectStatementHandler} with the specified {@link DynamoDbClient} and a new
   * {@link TableMetadataManager}
   *
   * @param client {@code DynamoDbClient}
   * @param metadataManager {@code TableMetadataManager}
   * @param namespacePrefix a namespace prefix
   */
  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public SelectStatementHandler(
      DynamoDbClient client,
      TableMetadataManager metadataManager,
      Optional<String> namespacePrefix) {
    this.client = checkNotNull(client);
    this.metadataManager = checkNotNull(metadataManager);
    this.namespacePrefix = namespacePrefix.orElse("");
  }

  @Nonnull
  public Scanner handle(Selection selection) throws ExecutionException {
    TableMetadata tableMetadata = metadataManager.getTableMetadata(selection);
    if (selection instanceof Get) {
      selection = copyAndAppendNamespacePrefix((Get) selection);
    } else {
      selection = copyAndAppendNamespacePrefix((Scan) selection);
    }

    try {
      if (!(selection instanceof ScanAll)
          && ScalarDbUtils.isSecondaryIndexSpecified(selection, tableMetadata)) {
        return executeScanWithIndex(selection, tableMetadata);
      }

      if (selection instanceof Get) {
        return executeGet((Get) selection, tableMetadata);
      } else if (selection instanceof ScanAll) {
        return executeFullScan((ScanAll) selection, tableMetadata);
      } else {
        return executeScan((Scan) selection, tableMetadata);
      }
    } catch (DynamoDbException e) {
      throw new ExecutionException(
          CoreError.DYNAMO_ERROR_OCCURRED_IN_SELECTION.buildMessage(e.getMessage()), e);
    }
  }

  private Scanner executeGet(Get get, TableMetadata tableMetadata) {
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

    return new GetItemScanner(
        client, builder.build(), new ResultInterpreter(get.getProjections(), tableMetadata));
  }

  private Scanner executeScanWithIndex(Selection selection, TableMetadata tableMetadata) {
    DynamoOperation dynamoOperation = new DynamoOperation(selection, tableMetadata);
    Column<?> keyColumn = selection.getPartitionKey().getColumns().get(0);
    String column = keyColumn.getName();
    String indexTable = dynamoOperation.getGlobalIndexName(column);
    QueryRequest.Builder builder =
        QueryRequest.builder().tableName(dynamoOperation.getTableName()).indexName(indexTable);

    String expressionColumnName = DynamoOperation.COLUMN_NAME_ALIAS + "0";
    String condition = expressionColumnName + " = " + DynamoOperation.VALUE_ALIAS + "0";
    ValueBinder binder = new ValueBinder(DynamoOperation.VALUE_ALIAS);
    keyColumn.accept(binder);
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
    com.scalar.db.storage.dynamo.request.QueryRequest request =
        new com.scalar.db.storage.dynamo.request.QueryRequest(client, builder.build());
    return new QueryScanner(
        request, new ResultInterpreter(selection.getProjections(), tableMetadata));
  }

  private Scanner executeScan(Scan scan, TableMetadata tableMetadata) {
    DynamoOperation dynamoOperation = new DynamoOperation(scan, tableMetadata);
    QueryRequest.Builder builder = QueryRequest.builder().tableName(dynamoOperation.getTableName());

    if (!setConditions(builder, scan, tableMetadata)) {
      // if setConditions() fails, return an empty scanner
      return new EmptyScanner();
    }

    if (!scan.getOrderings().isEmpty()) {
      Ordering ordering = scan.getOrderings().get(0);
      if (ordering.getOrder() != tableMetadata.getClusteringOrder(ordering.getColumnName())) {
        // reverse scan
        builder.scanIndexForward(false);
      }
    }

    if (scan.getLimit() > 0) {
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
    com.scalar.db.storage.dynamo.request.QueryRequest queryRequest =
        new com.scalar.db.storage.dynamo.request.QueryRequest(client, builder.build());
    return new QueryScanner(
        queryRequest, new ResultInterpreter(scan.getProjections(), tableMetadata));
  }

  private Scanner executeFullScan(ScanAll scan, TableMetadata tableMetadata) {
    DynamoOperation dynamoOperation = new DynamoOperation(scan, tableMetadata);
    ScanRequest.Builder builder = ScanRequest.builder().tableName(dynamoOperation.getTableName());

    // Ignore limit to control it in FilterableQueryScanner if scan has condition(s)
    if (scan.getLimit() > 0 && scan.getConjunctions().isEmpty()) {
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
    com.scalar.db.storage.dynamo.request.ScanRequest requestWrapper =
        new com.scalar.db.storage.dynamo.request.ScanRequest(client, builder.build());

    if (scan.getConjunctions().isEmpty()) {
      return new QueryScanner(
          requestWrapper, new ResultInterpreter(scan.getProjections(), tableMetadata));
    } else {
      return new FilterableQueryScanner(
          scan, requestWrapper, new ResultInterpreter(scan.getProjections(), tableMetadata));
    }
  }

  private void projectionExpression(
      DynamoDbRequest.Builder builder,
      Selection selection,
      Map<String, String> expressionAttributeNames) {
    assert builder instanceof GetItemRequest.Builder
        || builder instanceof QueryRequest.Builder
        || builder instanceof ScanRequest.Builder;

    List<String> projections = new ArrayList<>(selection.getProjections().size());
    for (String projection : selection.getProjections()) {
      String alias = DynamoOperation.COLUMN_NAME_ALIAS + expressionAttributeNames.size();
      projections.add(alias);
      expressionAttributeNames.put(alias, projection);
    }
    String projectionExpression = String.join(",", projections);

    if (builder instanceof GetItemRequest.Builder) {
      ((GetItemRequest.Builder) builder).projectionExpression(projectionExpression);
    } else if (builder instanceof QueryRequest.Builder) {
      ((QueryRequest.Builder) builder).projectionExpression(projectionExpression);
    } else {
      ((ScanRequest.Builder) builder).projectionExpression(projectionExpression);
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

  private Get copyAndAppendNamespacePrefix(Get get) {
    assert get.forNamespace().isPresent();
    return Get.newBuilder(get).namespace(namespacePrefix + get.forNamespace().get()).build();
  }

  private Scan copyAndAppendNamespacePrefix(Scan scan) {
    assert scan.forNamespace().isPresent();
    return Scan.newBuilder(scan).namespace(namespacePrefix + scan.forNamespace().get()).build();
  }
}
