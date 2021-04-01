package com.scalar.db.storage.jdbc.metadata;

import static com.scalar.db.storage.jdbc.query.QueryUtils.enclose;
import static com.scalar.db.storage.jdbc.query.QueryUtils.enclosedFullTableName;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Scan;
import com.scalar.db.exception.storage.StorageRuntimeException;
import com.scalar.db.storage.common.metadata.DataType;
import com.scalar.db.storage.common.metadata.TableMetadataManager;
import com.scalar.db.storage.jdbc.RdbEngine;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import javax.sql.DataSource;

/**
 * A manager of the instances of {@link JdbcTableMetadata}.
 *
 * @author Toshihiro Suzuki
 */
@ThreadSafe
public class JdbcTableMetadataManager implements TableMetadataManager {
  public static final String SCHEMA = "scalardb";
  public static final String TABLE = "metadata";

  private final LoadingCache<String, Optional<JdbcTableMetadata>> tableMetadataCache;

  public JdbcTableMetadataManager(
      DataSource dataSource, Optional<String> schemaPrefix, RdbEngine rdbEngine) {
    // TODO Need to add an expiration to handle the case of altering table
    tableMetadataCache =
        CacheBuilder.newBuilder()
            .build(
                new CacheLoader<String, Optional<JdbcTableMetadata>>() {
                  @Override
                  public Optional<JdbcTableMetadata> load(@Nonnull String fullTableName)
                      throws SQLException {
                    return JdbcTableMetadataManager.this.load(
                        dataSource, fullTableName, schemaPrefix, rdbEngine);
                  }
                });
  }

  private Optional<JdbcTableMetadata> load(
      DataSource dataSource,
      String fullTableName,
      Optional<String> schemaPrefix,
      RdbEngine rdbEngine)
      throws SQLException {

    List<String> partitionKeyNames = new ArrayList<>();
    List<String> clusteringKeyNames = new ArrayList<>();
    Map<String, Scan.Ordering.Order> clusteringOrders = new HashMap<>();
    Map<String, DataType> columnDataTypes = new HashMap<>();
    List<String> secondaryIndexNames = new ArrayList<>();

    try (Connection connection = dataSource.getConnection();
        PreparedStatement preparedStatement =
            connection.prepareStatement(getSelectColumnsStatement(schemaPrefix, rdbEngine))) {
      preparedStatement.setString(1, fullTableName);

      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        while (resultSet.next()) {
          String columnName = resultSet.getString("column_name");
          DataType dataType = DataType.valueOf(resultSet.getString("data_type"));
          columnDataTypes.put(columnName, dataType);

          boolean indexed = resultSet.getBoolean("indexed");
          if (indexed) {
            secondaryIndexNames.add(columnName);
          }

          String keyType = resultSet.getString("key_type");
          if (keyType == null) {
            continue;
          }

          switch (KeyType.valueOf(keyType)) {
            case PARTITION:
              partitionKeyNames.add(columnName);
              break;
            case CLUSTERING:
              clusteringKeyNames.add(columnName);
              clusteringOrders.put(
                  columnName, Scan.Ordering.Order.valueOf(resultSet.getString("clustering_order")));
              break;
            default:
              throw new AssertionError("invalid key type: " + keyType);
          }
        }
      }
    }

    if (partitionKeyNames.isEmpty()) {
      // The specified table is not found
      return Optional.empty();
    }

    return Optional.of(
        new JdbcTableMetadata(
            fullTableName,
            partitionKeyNames,
            clusteringKeyNames,
            clusteringOrders,
            columnDataTypes,
            secondaryIndexNames));
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
        + " FROM "
        + enclosedFullTableName(schemaPrefix.orElse("") + SCHEMA, TABLE, rdbEngine)
        + " WHERE "
        + enclose("full_table_name", rdbEngine)
        + " = ? ORDER BY "
        + enclose("ordinal_position", rdbEngine)
        + " ASC";
  }

  @Override
  public JdbcTableMetadata getTableMetadata(Operation operation) {
    if (!operation.forNamespace().isPresent() || !operation.forTable().isPresent()) {
      throw new IllegalArgumentException("operation has no target namespace and table name");
    }

    String fullTableName = operation.forFullTableName().get();
    return getTableMetadata(fullTableName);
  }

  public JdbcTableMetadata getTableMetadata(String fullTableName) {
    try {
      return tableMetadataCache.get(fullTableName).orElse(null);
    } catch (ExecutionException e) {
      throw new StorageRuntimeException("Failed to read the table metadata", e);
    }
  }
}
