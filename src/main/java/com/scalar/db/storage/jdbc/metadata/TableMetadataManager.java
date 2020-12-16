package com.scalar.db.storage.jdbc.metadata;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.scalar.db.api.Scan;
import com.scalar.db.storage.jdbc.Table;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * A manager to read and cache {@link TableMetadata}
 *
 * @author Toshihiro Suzuki
 */
@ThreadSafe
public class TableMetadataManager {
  private static final String SCHEMA = "scalardb";
  private static final String TABLE = "metadata";

  public static String getSchema() {
    return getSchema("");
  }

  public static String getSchema(String schemaPrefix) {
    return schemaPrefix + SCHEMA;
  }

  public static String getTable() {
    return getTable("");
  }

  public static String getTable(String schemaPrefix) {
    return getSchema(schemaPrefix) + "." + TABLE;
  }

  private final LoadingCache<Table, TableMetadata> tableMetadataCache;

  public TableMetadataManager(DataSource dataSource) {
    this(dataSource, "");
  }

  public TableMetadataManager(DataSource dataSource, String schemaPrefix) {
    // TODO Need to add an expiration to handle the case of altering table
    tableMetadataCache =
        CacheBuilder.newBuilder()
            .build(
                new CacheLoader<Table, TableMetadata>() {
                  @Override
                  public TableMetadata load(@Nonnull Table table) throws SQLException {
                    return TableMetadataManager.this.load(dataSource, table, schemaPrefix);
                  }
                });
  }

  private TableMetadata load(DataSource dataSource, Table table, String schemaPrefix)
      throws SQLException {

    Map<String, DataType> columnTypes = new LinkedHashMap<>();
    List<String> partitionKeys = new ArrayList<>();
    List<String> clusteringKeys = new ArrayList<>();
    Map<String, Scan.Ordering.Order> clusteringKeyOrders = new HashMap<>();
    Set<String> indexedColumns = new HashSet<>();

    try (Connection connection = dataSource.getConnection()) {
      try (PreparedStatement preparedStatement =
          connection.prepareStatement(getSelectColumnsStatement(schemaPrefix))) {
        preparedStatement.setString(1, table.getSchema());
        preparedStatement.setString(2, table.getTable());

        try (ResultSet resultSet = preparedStatement.executeQuery()) {
          while (resultSet.next()) {
            String columnName = resultSet.getString("column_name");
            DataType dataType = DataType.valueOf(resultSet.getString("data_type"));
            columnTypes.put(columnName, dataType);

            boolean indexed = resultSet.getBoolean("indexed");
            if (indexed) {
              indexedColumns.add(columnName);
            }

            String keyType = resultSet.getString("key_type");
            if (keyType == null) {
              continue;
            }

            switch (KeyType.valueOf(keyType)) {
              case PARTITION:
                partitionKeys.add(columnName);
                break;
              case CLUSTERING:
                clusteringKeys.add(columnName);
                clusteringKeyOrders.put(
                    columnName,
                    Scan.Ordering.Order.valueOf(resultSet.getString("clustering_order")));
                break;
              default:
                throw new AssertionError("Invalid key type: " + keyType);
            }
          }
        }
      }
    }

    return new TableMetadata(
        table, columnTypes, partitionKeys, clusteringKeys, clusteringKeyOrders, indexedColumns);
  }

  private String getSelectColumnsStatement(String schemaPrefix) {
    return "SELECT column_name, data_type, key_type, clustering_order, indexed "
        + "FROM "
        + getTable(schemaPrefix)
        + " WHERE namespace = ? AND table_name = ? "
        + "ORDER BY ordinal_position ASC";
  }

  public TableMetadata getTableMetadata(Table table) throws SQLException {
    try {
      return tableMetadataCache.get(table);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof SQLException) {
        throw (SQLException) e.getCause();
      } else {
        throw new SQLException(e.getCause());
      }
    }
  }
}
