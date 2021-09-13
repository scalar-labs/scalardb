package com.scalar.db.schemaloader.schema;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import com.scalar.db.transaction.consensuscommit.Attribute;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Table {

  private static final Logger LOGGER = LoggerFactory.getLogger(Table.class);

  private static final String COLUMNS = "columns";
  private static final String TRANSACTION = "transaction";
  private static final String PARTITION_KEY = "partition-key";
  private static final String CLUSTERING_KEY = "clustering-key";
  private static final String SECONDARY_INDEX = "secondary-index";
  private static final String TRANSACTION_COL_PREFIX = "before_";

  private static final ImmutableMap<String, DataType> DATA_MAP_TYPE =
      ImmutableMap.<String, DataType>builder()
          .put("BOOLEAN", DataType.BOOLEAN)
          .put("INT", DataType.INT)
          .put("BIGINT", DataType.BIGINT)
          .put("FLOAT", DataType.FLOAT)
          .put("DOUBLE", DataType.DOUBLE)
          .put("TEXT", DataType.TEXT)
          .put("BLOB", DataType.BLOB)
          .build();
  private static final ImmutableMap<String, DataType> TRANSACTION_META_COLUMNS =
      ImmutableMap.<String, DataType>builder()
          .put(Attribute.COMMITTED_AT, DataType.BIGINT)
          .put(Attribute.ID, DataType.TEXT)
          .put(Attribute.PREPARED_AT, DataType.BIGINT)
          .put(Attribute.STATE, DataType.INT)
          .put(Attribute.VERSION, DataType.INT)
          .build();
  private static final ImmutableMap<String, Order> ORDER_MAP =
      ImmutableMap.<String, Order>builder().put("ASC", Order.ASC).put("DESC", Order.DESC).build();

  private String namespace;
  private String tableName;
  private TableMetadata tableMetadata;
  private Map<String, String> options;
  private boolean isTransactionTable = false;
  private Set<String> traveledKeys;

  @VisibleForTesting
  public Table() {}

  @VisibleForTesting
  public Table(Set<String> traveledKeys) {
    this.traveledKeys = traveledKeys;
  }

  public Table(String tableFullName, JsonObject tableDefinition, Map<String, String> metaOptions)
      throws RuntimeException {
    traveledKeys = new HashSet<>();

    String[] fullName = tableFullName.split("\\.", -1);
    if (fullName.length < 2) {
      throw new RuntimeException("Table full name must contains table name and namespace");
    }
    namespace = fullName[0];
    tableName = fullName[1];
    tableMetadata = buildTableMetadata(tableDefinition);
    options = buildOptions(tableDefinition, metaOptions);
  }

  protected TableMetadata buildTableMetadata(JsonObject tableDefinition) {
    TableMetadata.Builder tableBuilder = TableMetadata.newBuilder();

    // Add partition keys
    if (!tableDefinition.keySet().contains(PARTITION_KEY)) {
      throw new RuntimeException("Table must contains partition key");
    }
    JsonArray partitionKeys = tableDefinition.get(PARTITION_KEY).getAsJsonArray();
    traveledKeys.add(PARTITION_KEY);
    Set<String> partitionKeySet = new HashSet<>();
    for (JsonElement partitionKey : partitionKeys) {
      partitionKeySet.add(partitionKey.getAsString());
      tableBuilder.addPartitionKey(partitionKey.getAsString());
    }

    // Add clustering keys
    Set<String> clusteringKeySet = new HashSet<>();
    if (tableDefinition.keySet().contains(CLUSTERING_KEY)) {
      JsonArray clusteringKeys = tableDefinition.get(CLUSTERING_KEY).getAsJsonArray();
      traveledKeys.add(CLUSTERING_KEY);
      for (JsonElement clusteringKeyRaw : clusteringKeys) {
        String clusteringKey;
        String order;
        String[] clusteringKeyFull = clusteringKeyRaw.getAsString().split(" ", -1);
        if (clusteringKeyFull.length < 2) {
          clusteringKey = clusteringKeyFull[0];
          tableBuilder.addClusteringKey(clusteringKey);
        } else if (clusteringKeyFull.length == 2
            && (clusteringKeyFull[1].equalsIgnoreCase("ASC")
                || clusteringKeyFull[1].equalsIgnoreCase("DESC"))) {
          clusteringKey = clusteringKeyFull[0];
          order = clusteringKeyFull[1];
          tableBuilder.addClusteringKey(clusteringKey, ORDER_MAP.get(order.toUpperCase()));
        } else {
          throw new RuntimeException("Invalid clustering keys");
        }
        clusteringKeySet.add(clusteringKey);
      }
    }

    boolean transaction = false;
    if (tableDefinition.keySet().contains(TRANSACTION)) {
      transaction = tableDefinition.get(TRANSACTION).getAsBoolean();
      traveledKeys.add(TRANSACTION);
      LOGGER.debug("transaction: " + transaction);
    }

    // Add transaction metadata columns
    if (transaction) {
      isTransactionTable = true;
      for (Map.Entry<String, DataType> col : TRANSACTION_META_COLUMNS.entrySet()) {
        tableBuilder.addColumn(col.getKey(), col.getValue());
        tableBuilder.addColumn(TRANSACTION_COL_PREFIX + col.getKey(), col.getValue());
      }
    }

    // Add columns
    if (!tableDefinition.keySet().contains(COLUMNS)) {
      throw new RuntimeException("Table must contains columns");
    }
    JsonObject columns = tableDefinition.get(COLUMNS).getAsJsonObject();
    traveledKeys.add(COLUMNS);
    Set<String> columnNameSet =
        columns.entrySet().stream().map(Entry::getKey).collect(Collectors.toSet());
    Set<String> transactionMetaExtraColumnSet =
        TRANSACTION_META_COLUMNS.keySet().stream()
            .map(col -> TRANSACTION_COL_PREFIX + col)
            .collect(Collectors.toSet());
    for (Entry<String, JsonElement> column : columns.entrySet()) {
      String columnName = column.getKey();
      DataType columnDataType = DATA_MAP_TYPE.get(column.getValue().getAsString().toUpperCase());
      if (columnDataType == null) {
        throw new RuntimeException("Invalid column type for column " + columnName);
      }
      tableBuilder.addColumn(columnName, columnDataType);
      if (transaction
          && !partitionKeySet.contains(columnName)
          && !clusteringKeySet.contains(columnName)) {

        String transactionExtraColumn = TRANSACTION_COL_PREFIX + columnName;
        if (TRANSACTION_META_COLUMNS.keySet().contains(columnName)
            || transactionMetaExtraColumnSet.contains(columnName)
            || columnNameSet.contains(transactionExtraColumn)) {
          throw new RuntimeException(
              "Column name \""
                  + (columnNameSet.contains(transactionExtraColumn)
                      ? transactionExtraColumn
                      : columnName)
                  + "\" in table \""
                  + tableName
                  + "\" has been already reserved as transaction metadata!");
        }
        tableBuilder.addColumn(transactionExtraColumn, columnDataType);
      }
    }

    // Add secondary indexes
    if (tableDefinition.keySet().contains(SECONDARY_INDEX)) {
      JsonArray secondaryIndexes = tableDefinition.get(SECONDARY_INDEX).getAsJsonArray();
      traveledKeys.add(SECONDARY_INDEX);
      for (JsonElement sIdx : secondaryIndexes) {
        tableBuilder.addSecondaryIndex(sIdx.getAsString());
      }
    }

    return tableBuilder.build();
  }

  protected Map<String, String> buildOptions(
      JsonObject tableDefinition, Map<String, String> metaOptions) {
    Map<String, String> options = new HashMap<>(metaOptions);
    for (Map.Entry<String, JsonElement> option : tableDefinition.entrySet()) {
      if (!traveledKeys.contains(option.getKey())) {
        options.put(option.getKey(), option.getValue().getAsString());
      }
    }
    return options;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getTable() {
    return tableName;
  }

  public TableMetadata getTableMetadata() {
    return tableMetadata;
  }

  public Map<String, String> getOptions() {
    return options;
  }

  public boolean isTransactionTable() {
    return isTransactionTable;
  }
}
