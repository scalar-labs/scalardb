package com.scalar.db.storage.jdbc;

import static com.scalar.db.storage.jdbc.query.QueryUtils.enclose;
import static com.scalar.db.storage.jdbc.query.QueryUtils.enclosedFullTableName;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.StorageRuntimeException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.common.TableMetadataManager;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import javax.sql.DataSource;

/**
 * A manager of the instances of {@link TableMetadata}.
 *
 * @author Toshihiro Suzuki
 */
@ThreadSafe
public class JdbcTableMetadataManager implements TableMetadataManager {
  public static final String SCHEMA = "scalardb";
  public static final String TABLE = "metadata";

  private final LoadingCache<String, Optional<TableMetadata>> tableMetadataCache;

  public JdbcTableMetadataManager(
      DataSource dataSource, Optional<String> schemaPrefix, RdbEngine rdbEngine) {
    // TODO Need to add an expiration to handle the case of altering table
    tableMetadataCache =
        CacheBuilder.newBuilder()
            .build(
                new CacheLoader<String, Optional<TableMetadata>>() {
                  @Override
                  public Optional<TableMetadata> load(@Nonnull String fullTableName)
                      throws SQLException {
                    return JdbcTableMetadataManager.this.load(
                        dataSource, fullTableName, schemaPrefix, rdbEngine);
                  }
                });
  }

  @SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
  private Optional<TableMetadata> load(
      DataSource dataSource,
      String fullTableName,
      Optional<String> schemaPrefix,
      RdbEngine rdbEngine)
      throws SQLException {
    TableMetadata.Builder builder = TableMetadata.newBuilder();
    boolean tableExists = false;

    try (Connection connection = dataSource.getConnection();
        PreparedStatement preparedStatement =
            connection.prepareStatement(getSelectColumnsStatement(schemaPrefix, rdbEngine))) {
      preparedStatement.setString(1, fullTableName);

      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        while (resultSet.next()) {
          tableExists = true;

          String columnName = resultSet.getString("column_name");
          DataType dataType = DataType.valueOf(resultSet.getString("data_type"));
          builder.addColumn(columnName, dataType);

          boolean indexed = resultSet.getBoolean("indexed");
          if (indexed) {
            builder.addSecondaryIndex(columnName);
          }

          String keyType = resultSet.getString("key_type");
          if (keyType == null) {
            continue;
          }

          switch (KeyType.valueOf(keyType)) {
            case PARTITION:
              builder.addPartitionKey(columnName);
              break;
            case CLUSTERING:
              Scan.Ordering.Order clusteringOrder =
                  Scan.Ordering.Order.valueOf(resultSet.getString("clustering_order"));
              builder.addClusteringKey(columnName, clusteringOrder);
              break;
            default:
              throw new AssertionError("invalid key type: " + keyType);
          }
        }
      }
    }

    if (!tableExists) {
      return Optional.empty();
    }

    return Optional.of(builder.build());
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
  public TableMetadata getTableMetadata(Operation operation) {
    if (!operation.forNamespace().isPresent() || !operation.forTable().isPresent()) {
      throw new IllegalArgumentException("operation has no target namespace and table name");
    }

    String fullTableName = operation.forFullTableName().get();
    return getTableMetadata(fullTableName);
  }

  @Override
  public TableMetadata getTableMetadata(String namespace, String table) {
    return getTableMetadata(namespace + "." + table);
  }

  public TableMetadata getTableMetadata(String fullTableName) {
    try {
      return tableMetadataCache.get(fullTableName).orElse(null);
    } catch (ExecutionException e) {
      throw new StorageRuntimeException("Failed to read the table metadata", e);
    }
  }
}
