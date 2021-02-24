package com.scalar.db.storage.jdbc.metadata;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.scalar.db.api.Scan;
import com.scalar.db.storage.jdbc.RdbEngine;

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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.scalar.db.storage.jdbc.query.QueryUtils.enclose;
import static com.scalar.db.storage.jdbc.query.QueryUtils.enclosedFullTableName;

/**
 * A manager of the instances of {@link JdbcTableMetadata}.
 *
 * @author Toshihiro Suzuki
 */
@ThreadSafe
public class TableMetadataManager {
  public static final String SCHEMA = "scalardb";
  public static final String TABLE = "metadata";
  private final LoadingCache<String, JdbcTableMetadata> tableMetadataCache;

  public TableMetadataManager(
      DataSource dataSource, Optional<String> schemaPrefix, RdbEngine rdbEngine) {
    // TODO Need to add an expiration to handle the case of altering table
    tableMetadataCache =
        CacheBuilder.newBuilder()
            .build(
                new CacheLoader<String, JdbcTableMetadata>() {
                  @Override
                  public JdbcTableMetadata load(@Nonnull String fullTableName) throws SQLException {
                    return TableMetadataManager.this.load(
                        dataSource, fullTableName, schemaPrefix, rdbEngine);
                  }
                });
  }

  private JdbcTableMetadata load(
      DataSource dataSource,
      String fullTableName,
      Optional<String> schemaPrefix,
      RdbEngine rdbEngine)
      throws SQLException {

    LinkedHashMap<String, DataType> columnsAndDataTypes = new LinkedHashMap<>();
    List<String> partitionKeys = new ArrayList<>();
    List<String> clusteringKeys = new ArrayList<>();
    Map<String, Scan.Ordering.Order> clusteringKeyOrders = new HashMap<>();
    Set<String> indexedColumns = new HashSet<>();
    Map<String, Scan.Ordering.Order> indexOrders = new HashMap<>();

    try (Connection connection = dataSource.getConnection();
        PreparedStatement preparedStatement =
            connection.prepareStatement(getSelectColumnsStatement(schemaPrefix, rdbEngine))) {
      preparedStatement.setString(1, fullTableName);

      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        while (resultSet.next()) {
          String columnName = resultSet.getString("column_name");
          DataType dataType = DataType.valueOf(resultSet.getString("data_type"));
          columnsAndDataTypes.put(columnName, dataType);

          boolean indexed = resultSet.getBoolean("indexed");
          if (indexed) {
            indexedColumns.add(columnName);
            indexOrders.put(
                columnName, Scan.Ordering.Order.valueOf(resultSet.getString("index_order")));
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
                  columnName, Scan.Ordering.Order.valueOf(resultSet.getString("clustering_order")));
              break;
            default:
              throw new AssertionError("invalid key type: " + keyType);
          }
        }
      }
    }

    return new JdbcTableMetadata(
        fullTableName,
        columnsAndDataTypes,
        partitionKeys,
        clusteringKeys,
        clusteringKeyOrders,
        indexedColumns,
        indexOrders);
  }

  private String getSelectColumnsStatement(Optional<String> schemaPrefix, RdbEngine rdbEngine) {
    return "SELECT "
        + enclose("column_name", rdbEngine)
        + ", "
        + enclose("data_type", rdbEngine)
        + ", "
        + enclose("key_type", rdbEngine)
        + ", "
        + enclose("clustering_order", rdbEngine)
        + ", "
        + enclose("indexed", rdbEngine)
        + ", "
        + enclose("index_order", rdbEngine)
        + " FROM "
        + enclosedFullTableName(schemaPrefix.orElse("") + SCHEMA, TABLE, rdbEngine)
        + " WHERE "
        + enclose("full_table_name", rdbEngine)
        + " = ? ORDER BY "
        + enclose("ordinal_position", rdbEngine)
        + " ASC";
  }

  public JdbcTableMetadata getTableMetadata(String fullTableName) throws SQLException {
    try {
      return tableMetadataCache.get(fullTableName);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof SQLException) {
        throw (SQLException) e.getCause();
      } else {
        throw new SQLException(e.getCause());
      }
    }
  }
}
