package utils;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TableMetadata.Builder;
import com.scalar.db.io.DataType;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Table {
  protected String namespace;
  protected String tableName;
  protected TableMetadata tableMetadata;
  protected Map<String, String> options;
  protected boolean isTransactionTable = false;
  protected Set<String> traveledKeys;

  private static final String COLUMNS = "columns";
  private static final String TRANSACTION = "transaction";
  private static final String PARTITION_KEY = "partition-key";
  private static final String CLUSTERING_KEY = "clustering-key";
  private static final String SECONDARY_INDEX = "secondary-index";

  private static final String TRANSACTION_COL_PREFIX = "before_";

  private static final Map<String, DataType> dtypeMap =
      new HashMap<String, DataType>() {
        {
          put("BOOLEAN", DataType.BOOLEAN);
          put("INT", DataType.INT);
          put("BIGINT", DataType.BIGINT);
          put("FLOAT", DataType.FLOAT);
          put("DOUBLE", DataType.DOUBLE);
          put("TEXT", DataType.TEXT);
          put("BLOB", DataType.BLOB);
        }
      };

  private static final Map<String, DataType> transactionMetadataColumns =
      new HashMap<String, DataType>() {
        {
          put("tx_committed_at", DataType.BIGINT);
          put("tx_id", DataType.TEXT);
          put("tx_prepared_at", DataType.BIGINT);
          put("tx_state", DataType.INT);
          put("tx_version", DataType.INT);
        }
      };

  private static final Map<String, Order> orderMap =
      new HashMap<String, Order>() {
        {
          put("ASC", Order.ASC);
          put("DESC", Order.DESC);
        }
      };

  public Table(String tableFullName, JsonObject tableDefinition) throws RuntimeException {
    traveledKeys = new HashSet<String>();

    String[] fullName = tableFullName.split("\\.");
    if (fullName.length < 2) {
      throw new RuntimeException("Table full name must contains table name and namespace");
    }
    namespace = fullName[0];
    tableName = fullName[1];
    tableMetadata = buildTableMetadata(tableDefinition);
    options = buildOptions(tableDefinition);
  }

  protected TableMetadata buildTableMetadata(JsonObject tableDefinition) {
    Builder tableBuilder = TableMetadata.newBuilder();

    // Add partition keys
    if (!tableDefinition.keySet().contains(PARTITION_KEY)) {
      throw new RuntimeException("Table must contains partition key");
    }
    JsonArray partitionKeys = tableDefinition.get(PARTITION_KEY).getAsJsonArray();
    traveledKeys.add(PARTITION_KEY);
    Set<String> partitionKeySet = new HashSet<String>();
    for (JsonElement pKey : partitionKeys) {
      partitionKeySet.add(pKey.getAsString());
      tableBuilder.addPartitionKey(pKey.getAsString());
    }

    // Add clustering keys
    Set<String> clusteringKeySet = new HashSet<String>();
    if (tableDefinition.keySet().contains(CLUSTERING_KEY)) {
      JsonArray clusteringKeys = tableDefinition.get(CLUSTERING_KEY).getAsJsonArray();
      traveledKeys.add(CLUSTERING_KEY);
      for (JsonElement cKeyRaw : clusteringKeys) {
        String cKey = "";
        String oder = "ASC";
        String[] cKeyFull = cKeyRaw.getAsString().split(" ");
        if (cKeyFull.length < 2) {
          cKey = cKeyFull[0];
          tableBuilder.addClusteringKey(cKey);
        } else if (cKeyFull.length == 2
            && (cKeyFull[1].toUpperCase().equals("ASC")
                || cKeyFull[1].toUpperCase().equals("DESC"))) {
          cKey = cKeyFull[0];
          oder = cKeyFull[1];
          tableBuilder.addClusteringKey(cKey, orderMap.get(oder.toUpperCase()));
        } else {
          throw new RuntimeException("Invalid clustering keys");
        }
        clusteringKeySet.add(cKey);
      }
    }

    boolean transaction = false;
    if (tableDefinition.keySet().contains(TRANSACTION)) {
      transaction = tableDefinition.get(TRANSACTION).getAsBoolean();
      traveledKeys.add(TRANSACTION);
      Logger.getGlobal().log(Level.FINE, "transaction: " + transaction);
    }
    // Add transaction metadata columns
    if (transaction) {
      isTransactionTable = true;
      for (Map.Entry<String, DataType> col : transactionMetadataColumns.entrySet()) {
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
    for (Entry<String, JsonElement> col : columns.entrySet()) {
      String colName = col.getKey();
      DataType colDtype = dtypeMap.get(col.getValue().getAsString().toUpperCase());
      if (colDtype == null) {
        throw new RuntimeException("Invalid column type for column " + colName);
      }
      tableBuilder.addColumn(colName, colDtype);
      if (transaction
          && !partitionKeySet.contains(colName)
          && !clusteringKeySet.contains(colName)) {
        tableBuilder.addColumn(TRANSACTION_COL_PREFIX + colName, colDtype);
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

//    Logger.getGlobal().log(Level.FINE, "cols: " + tableBuilder.build().getColumnNames());
//    Logger.getGlobal()
//        .log(Level.FINE, "partition keys: " + tableBuilder.build().getPartitionKeyNames());
//    Logger.getGlobal()
//        .log(Level.FINE, "clustering keys: " + tableBuilder.build().getClusteringKeyNames());
//    Logger.getGlobal()
//        .log(Level.FINE, "secondary indexes: " + tableBuilder.build().getSecondaryIndexNames());

    return tableBuilder.build();
  }

  protected Map<String, String> buildOptions(JsonObject tableDefinition) {
    Map<String, String> options = new HashMap<String, String>();
    for (Map.Entry<String, ?> opt : tableDefinition.entrySet()) {
      if (!traveledKeys.contains(opt.getKey())) {
        options.put(opt.getKey(), opt.getValue().toString());
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
