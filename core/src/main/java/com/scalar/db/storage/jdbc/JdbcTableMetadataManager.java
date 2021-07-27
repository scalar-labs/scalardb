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
import java.sql.Statement;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
                    return JdbcTableMetadataManager.this.load(dataSource, fullTableName, rdbEngine);
                  }
                });
  }

  private Optional<TableMetadata> load(
      DataSource dataSource, String fullTableName, RdbEngine rdbEngine) throws SQLException {
    TableMetadata.Builder builder = TableMetadata.newBuilder();
    boolean tableExists = false;

    try (Connection connection = dataSource.getConnection();
        PreparedStatement preparedStatement =
            connection.prepareStatement(getSelectColumnsStatement(rdbEngine))) {
      preparedStatement.setString(1, fullTableName);

      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        while (resultSet.next()) {
          tableExists = true;

          String columnName = resultSet.getString(COLUMN_NAME);
          DataType dataType = DataType.valueOf(resultSet.getString(DATA_TYPE));
          builder.addColumn(columnName, dataType);

          boolean indexed = resultSet.getBoolean(INDEXED);
          if (indexed) {
            builder.addSecondaryIndex(columnName);
          }

          String keyType = resultSet.getString(KEY_TYPE);
          if (keyType == null) {
            continue;
          }

          switch (KeyType.valueOf(keyType)) {
            case PARTITION:
              builder.addPartitionKey(columnName);
              break;
            case CLUSTERING:
              Scan.Ordering.Order clusteringOrder =
                  Scan.Ordering.Order.valueOf(resultSet.getString(CLUSTERING_ORDER));
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

  private String getSelectColumnsStatement(RdbEngine rdbEngine) {
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
        + enclosedFullTableName(getMetadataSchema(), TABLE, rdbEngine)
        + " WHERE "
        + enclose(FULL_TABLE_NAME, rdbEngine)
        + " = ? ORDER BY "
        + enclose(ORDINAL_POSITION, rdbEngine)
        + " ASC";
  }

  private String getInsertStatement(
      String schema,
      String table,
      String columnName,
      DataType dataType,
      @Nullable KeyType keyType,
      @Nullable Ordering.Order ckOrder,
      boolean indexed,
      int ordinalPosition)
      throws SQLException {

    return String.format(
        "INSERT INTO %s VALUES ('%s','%s','%s',%s,%s,%s,%d)",
        enclosedFullTableName(getMetadataSchema(), TABLE, rdbEngine),
        schemaPrefix.orElse("") + schema + "." + table,
        columnName,
        dataType.toString(),
        keyType != null ? "'" + keyType + "'" : "NULL",
        ckOrder != null ? "'" + ckOrder + "'" : "NULL",
        computeBooleanValue(indexed),
        ordinalPosition);
  }

  private String computeBooleanValue(boolean value) {
    switch (rdbEngine) {
      case ORACLE:
      case SQL_SERVER:
        return value ? "1" : "0";
      default:
        return value ? "true" : "false";
    }
  }

  private String getDeleteStatement(String namespace, String tableName) {
    String fullTableName = schemaPrefix.orElse("") + namespace + "." + tableName;
    return "DELETE FROM "
        + enclosedFullTableName(getMetadataSchema(), TABLE, rdbEngine)
        + " WHERE "
        + enclose(FULL_TABLE_NAME, rdbEngine)
        + " = "
        + fullTableName;
  }

  private void insertMetadataColumn(
      String schema,
      String table,
      TableMetadata metadata,
      Connection connection,
      int ordinalPosition,
      String column)
      throws SQLException {
    KeyType keyType = null;
    if (metadata.getPartitionKeyNames().contains(column)) {
      keyType = KeyType.PARTITION;
    }
    if (metadata.getClusteringKeyNames().contains(column)) {
      keyType = KeyType.CLUSTERING;
    }

    try {
      String insertStatement =
          getInsertStatement(
              schema,
              table,
              column,
              metadata.getColumnDataType(column),
              keyType,
              metadata.getClusteringOrder(column),
              metadata.getSecondaryIndexNames().contains(column),
              ordinalPosition);
      execute(connection, insertStatement);
    } catch (SQLException e) {
      connection.rollback();
      throw e;
    }
  }

  private void createMetadataTable() {
    String createTableQuery =
        String.format(
                "CREATE TABLE %s(", enclosedFullTableName(getMetadataSchema(), TABLE, rdbEngine))
            + String.format("%s %s,", enclose(FULL_TABLE_NAME, rdbEngine), getTextType(128))
            + String.format("%s %s,", enclose(COLUMN_NAME, rdbEngine), getTextType(128))
            + String.format("%s %s NOT NULL,", enclose(DATA_TYPE, rdbEngine), getTextType(20))
            + String.format("%s %s,", enclose(KEY_TYPE, rdbEngine), getTextType(20))
            + String.format("%s %s,", enclose(CLUSTERING_ORDER, rdbEngine), getTextType(10))
            + String.format("%s %s NOT NULL,", enclose(INDEXED, rdbEngine), getBooleanType())
            + String.format("%s INTEGER NOT NULL,", enclose(ORDINAL_POSITION, rdbEngine))
            + String.format(
                "PRIMARY KEY (%s, %s))",
                enclose(FULL_TABLE_NAME, rdbEngine), enclose(COLUMN_NAME, rdbEngine));

    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);
      connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
      try {
        createMetadataSchema(connection);
        execute(connection, createTableQuery);
      } catch (SQLException e) {
        connection.rollback();
        throw e;
      }
      connection.commit();
    } catch (SQLException e) {
      if (e.getMessage().contains("database exists") || e.getMessage().contains("already exists")) {
        return;
      }
      throw new StorageRuntimeException("creating the metadata table failed", e);
    }
  }

  private void createMetadataSchema(Connection connection) throws SQLException {
    if (rdbEngine == RdbEngine.ORACLE) {
      execute(
          connection,
          "CREATE USER " + enclose(getMetadataSchema(), rdbEngine) + " IDENTIFIED BY \"oracle\"");
      execute(
          connection,
          "ALTER USER " + enclose(getMetadataSchema(), rdbEngine) + " quota unlimited on USERS");
    } else {
      execute(connection, "CREATE SCHEMA " + enclose(getMetadataSchema(), rdbEngine));
    }
  }

  private void execute(Connection connection, String sql) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute(sql);
    }
  }

  private String getMetadataSchema() {
    return schemaPrefix.orElse("") + JdbcTableMetadataManager.SCHEMA;
  }

  private String getBooleanType() {
    switch (rdbEngine) {
      case MYSQL:
      case POSTGRESQL:
        return "BOOLEAN";
      case SQL_SERVER:
        return "BIT";
      case ORACLE:
        return "NUMBER(1)";
      default:
        throw new UnsupportedOperationException(
            String.format("The rdb engine %s is not supported", rdbEngine));
    }
  }

  @Override
  public void addTableMetadata(String namespace, String table, TableMetadata metadata) {
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
        insertMetadataColumn(namespace, table, metadata, connection, ordinalPosition++, column);
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

  private String getTextType(int charLength) {
    if (rdbEngine == RdbEngine.ORACLE) {
      return String.format("VARCHAR2(%s)", charLength);
    }
    return String.format("VARCHAR(%s)", charLength);
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
    try (Connection connection = dataSource.getConnection()) {
      execute(connection, getDeleteStatement(namespace, table));
    } catch (SQLException e) {
      if (e.getMessage().contains("Unknown table") || e.getMessage().contains("does not exist")) {
        return;
      }
      throw new StorageRuntimeException(
          String.format(
              "deleting the %s table metadata failed", schemaPrefix + namespace + "." + table));
    }
    tableMetadataCache.put(schemaPrefix.orElse("") + namespace + "." + table, Optional.empty());
  }
}
