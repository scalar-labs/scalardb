package com.scalar.db.storage.jdbc;

import static com.scalar.db.storage.jdbc.query.QueryUtils.enclose;
import static com.scalar.db.storage.jdbc.query.QueryUtils.enclosedFullTableName;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.StorageRuntimeException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.common.TableMetadataManager;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
  private static final String FULL_TABLE_NAME = "full_table_name";
  private static final String COLUMN_NAME = "column_name";
  private static final String DATA_TYPE = "data_type";
  private static final String KEY_TYPE = "key_type";
  private static final String CLUSTERING_ORDER = "clustering_order";
  private static final String INDEXED = "indexed";
  private static final String ORDINAL_POSITION = "ordinal_position";
  private final LoadingCache<String, Optional<TableMetadata>> tableMetadataCache;
  private final DataSource dataSource;
  private final Optional<String> schemaPrefix;
  private final RdbEngine rdbEngine;

  public JdbcTableMetadataManager(
      DataSource dataSource, Optional<String> schemaPrefix, RdbEngine rdbEngine) {
    this.dataSource = dataSource;
    this.schemaPrefix = schemaPrefix;
    this.rdbEngine = rdbEngine;
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
        + enclose(COLUMN_NAME, rdbEngine)
        + ", "
        + enclose(DATA_TYPE, rdbEngine)
        + ", "
        + enclose(KEY_TYPE, rdbEngine)
        + ", "
        + enclose(CLUSTERING_ORDER, rdbEngine)
        + ", "
        + enclose(INDEXED, rdbEngine)
        + " FROM "
        + enclosedFullTableName(schemaPrefix.orElse("") + SCHEMA, TABLE, rdbEngine)
        + " WHERE "
        + enclose(FULL_TABLE_NAME, rdbEngine)
        + " = ? ORDER BY "
        + enclose(ORDINAL_POSITION, rdbEngine)
        + " ASC";
  }

  private PreparedStatement getInsertStatement(
      Connection connection,
      String columnName,
      DataType dataType,
      @Nullable KeyType keyType,
      @Nullable Ordering.Order ckOrder,
      boolean indexed,
      int ordinalPosition)
      throws SQLException {
    String statement =
        "INSERT INTO "
            + enclosedFullTableName(schemaPrefix.orElse("") + SCHEMA, TABLE, rdbEngine)
            + "("
            + FULL_TABLE_NAME
            + ", "
            + COLUMN_NAME
            + ", "
            + DATA_TYPE
            + ", "
            + KEY_TYPE
            + ", "
            + CLUSTERING_ORDER
            + ", "
            + INDEXED
            + ") VALUES (?, ?, ?, ?, ?, ?)";
    PreparedStatement preparedStatement = connection.prepareStatement(statement);
    preparedStatement.setString(1, schemaPrefix.orElse("") + SCHEMA + "." + TABLE);
    preparedStatement.setString(2, columnName);
    preparedStatement.setString(3, dataType.toString());
    if (keyType != null) {
      preparedStatement.setString(4, keyType.toString());
    } else {
      preparedStatement.setNull(5, Types.VARCHAR);
    }
    if (ckOrder != null) {
      preparedStatement.setString(6, ckOrder.toString());
    } else {
      preparedStatement.setNull(6, Types.VARCHAR);
    }
    if (rdbEngine == RdbEngine.ORACLE || rdbEngine == RdbEngine.SQL_SERVER) {
      preparedStatement.setInt(7, indexed ? 1 : 0);
    } else {
      preparedStatement.setBoolean(7, indexed);
    }
    preparedStatement.setInt(8, ordinalPosition);

    return preparedStatement;
  }

  private String getDeleteStatement(String tableName) {
    return "DELETE FROM "
        + enclosedFullTableName(schemaPrefix.orElse("") + SCHEMA, TABLE, rdbEngine)
        + " WHERE "
        + enclose(FULL_TABLE_NAME, rdbEngine)
        + " = "
        + tableName;
  }

  private void insertMetadataColumn(
      TableMetadata metadata, Connection connection, int ordinalPosition, String column)
      throws SQLException {
    KeyType keyType = null;
    if (metadata.getPartitionKeyNames().contains(column)) {
      keyType = KeyType.PARTITION;
    }
    if (metadata.getClusteringKeyNames().contains(column)) {
      keyType = KeyType.CLUSTERING;
    }

    try (PreparedStatement preparedStatement =
        getInsertStatement(
            connection,
            column,
            metadata.getColumnDataType(column),
            keyType,
            metadata.getClusteringOrder(column),
            metadata.getSecondaryIndexNames().contains(column),
            ordinalPosition)) {
      preparedStatement.execute();
    } catch (SQLException e) {
      connection.rollback();
      throw e;
    }
  }

  private void createMetadataTable() {
    String fullTableName = schemaPrefix.orElse("") + SCHEMA + "." + TABLE;

    String statement =
        String.format("CREATE TABLE %s (", fullTableName)
            + String.format("%s %s, ", FULL_TABLE_NAME, getTextType(128))
            + String.format("%s %s, ", COLUMN_NAME, getTextType(128))
            + String.format("%s %s NOT NULL, ", DATA_TYPE, getTextType(20))
            + String.format("%s %s, ", KEY_TYPE, getTextType(20))
            + String.format("%s %s, ", CLUSTERING_ORDER, getTextType(10))
            + String.format("%s %s NOT NULL, ", INDEXED, getBooleanType())
            + String.format("%s INTEGER NOT NULL, ", ORDINAL_POSITION)
            + String.format("PRIMARY KEY (%s, %s))", FULL_TABLE_NAME, COLUMN_NAME);

    try (Connection connection = dataSource.getConnection();
        PreparedStatement preparedStatement =
            connection.prepareStatement(getDeleteStatement(statement))) {
      preparedStatement.executeQuery();
    } catch (SQLException e) {
      // TODO Suppress exception if table exists

    }
  }

  private String getTextType(int charLength) {
    if (this.rdbEngine == RdbEngine.ORACLE) {
      return String.format("VARCHAR2( %s )", charLength);
    } else {
      return String.format("VARCHAR( %s )", charLength);
    }
  }

  private String getBooleanType() {
    switch (rdbEngine) {
      case MYSQL:
      case POSTGRESQL:
        return "BOOLEAN";
      case ORACLE:
        return "NUMBER(1)";
      case SQL_SERVER:
        return "BIT";
      default:
        throw new UnsupportedOperationException(
            String.format("The rdb engine %s is not supported", rdbEngine));
    }
  }

  @Override
  public void addTableMetadata(TableMetadata metadata) {
    createMetadataTable();
    try (Connection connection = dataSource.getConnection()) {
      // Start transaction to commit all the insert statements at once
      connection.setAutoCommit(false);
      connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

      LinkedHashSet<String> orderedColumns = new LinkedHashSet<>(metadata.getPartitionKeyNames());
      orderedColumns.addAll(metadata.getClusteringKeyNames());
      orderedColumns.addAll(metadata.getColumnNames());

      int ordinalPosition = 1;
      for (String column : orderedColumns) {
        insertMetadataColumn(metadata, connection, ordinalPosition++, column);
      }
      try {
        connection.commit();
      } catch (SQLException e) {
        connection.rollback();
        throw e;
      }
    } catch (SQLException e) {
      throw new StorageRuntimeException("adding the table metadata failed", e);
    }
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

  @Override
  public void deleteTableMetadata(String namespace, String table) {
    try (Connection connection = dataSource.getConnection();
        PreparedStatement preparedStatement =
            connection.prepareStatement(getDeleteStatement(table))) {
      preparedStatement.executeQuery();
    } catch (SQLException e) {
      throw new StorageRuntimeException(
          String.format(
              "deleting the %s table metadata failed", schemaPrefix + namespace + "." + table));
    }
  }
}
