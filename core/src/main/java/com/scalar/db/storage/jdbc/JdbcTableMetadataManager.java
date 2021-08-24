package com.scalar.db.storage.jdbc;

import static com.scalar.db.util.Utility.getFullNamespaceName;
import static com.scalar.db.util.Utility.getFullTableName;

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
import com.scalar.db.storage.jdbc.query.QueryUtils;
import com.scalar.db.util.Utility;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
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
  public static final String FULL_TABLE_NAME = "full_table_name";
  public static final String COLUMN_NAME = "column_name";
  public static final String DATA_TYPE = "data_type";
  public static final String KEY_TYPE = "key_type";
  public static final String CLUSTERING_ORDER = "clustering_order";
  public static final String INDEXED = "indexed";
  public static final String ORDINAL_POSITION = "ordinal_position";
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
                    return JdbcTableMetadataManager.this.load(fullTableName);
                  }
                });
  }

  @SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
  private Optional<TableMetadata> load(String fullTableName) throws SQLException {
    TableMetadata.Builder builder = TableMetadata.newBuilder();
    boolean tableExists = false;

    try (Connection connection = dataSource.getConnection();
        PreparedStatement preparedStatement =
            connection.prepareStatement(getSelectColumnsStatement())) {
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

  private String getSelectColumnsStatement() {
    return "SELECT "
        + enclose(COLUMN_NAME)
        + ", "
        + enclose(DATA_TYPE)
        + ", "
        + enclose(KEY_TYPE)
        + ", "
        + enclose(CLUSTERING_ORDER)
        + ", "
        + enclose(INDEXED)
        + " FROM "
        + encloseFullTableName(getMetadataSchema(), TABLE)
        + " WHERE "
        + enclose(FULL_TABLE_NAME)
        + " = ? ORDER BY "
        + enclose(ORDINAL_POSITION)
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
      int ordinalPosition) {

    return String.format(
        "INSERT INTO %s VALUES ('%s','%s','%s',%s,%s,%s,%d)",
        encloseFullTableName(getMetadataSchema(), TABLE),
        getFullTableName(schemaPrefix, schema, table),
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

  private String getDeleteTableMetadataStatement(String schema, String table) {
    return "DELETE FROM "
        + encloseFullTableName(getMetadataSchema(), TABLE)
        + " WHERE "
        + enclose(FULL_TABLE_NAME)
        + " = '"
        + getFullTableName(schemaPrefix, schema, table)
        + "'";
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
  }

  @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
  private void createMetadataTableIfNotExists(Connection connection) throws SQLException {
    String createTableStatement =
        "CREATE TABLE "
            + encloseFullTableName(getMetadataSchema(), TABLE)
            + "("
            + enclose(FULL_TABLE_NAME)
            + " "
            + getTextType(128)
            + ","
            + enclose(COLUMN_NAME)
            + " "
            + getTextType(128)
            + ","
            + enclose(DATA_TYPE)
            + " "
            + getTextType(20)
            + " NOT NULL,"
            + enclose(KEY_TYPE)
            + " "
            + getTextType(20)
            + ","
            + enclose(CLUSTERING_ORDER)
            + " "
            + getTextType(10)
            + ","
            + enclose(INDEXED)
            + " "
            + getBooleanType()
            + " NOT NULL,"
            + enclose(ORDINAL_POSITION)
            + " INTEGER NOT NULL,"
            + "PRIMARY KEY ("
            + enclose(FULL_TABLE_NAME)
            + ", "
            + enclose(COLUMN_NAME)
            + "))";

    String createTableIfNotExistsStatement;
    switch (rdbEngine) {
      case POSTGRESQL:
      case MYSQL:
        createTableIfNotExistsStatement =
            createTableStatement.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");
        execute(connection, createTableIfNotExistsStatement);
        break;
      case SQL_SERVER:
        try {
          execute(connection, createTableStatement);
        } catch (SQLException e) {
          // Suppress the exception thrown when the table already exists
          if (e.getErrorCode() != 2714) {
            throw e;
          }
        }
        break;
      case ORACLE:
        try {
          execute(connection, createTableStatement);
        } catch (SQLException e) {
          // Suppress the exception thrown when the table already exists
          if (e.getErrorCode() != 955) {
            throw e;
          }
        }
        break;
      default:
        throw new UnsupportedOperationException("rdb engine not supported");
    }
  }

  private void createMetadataSchemaAndTableIfNotExist() {
    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);
      connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
      try {
        createMetadataSchemaIfNotExists(connection);
        createMetadataTableIfNotExists(connection);
        connection.commit();
      } catch (SQLException e) {
        connection.rollback();
        throw e;
      }

    } catch (SQLException e) {
      throw new StorageRuntimeException("creating the metadata table failed", e);
    }
  }

  private void createMetadataSchemaIfNotExists(Connection connection) throws SQLException {
    switch (rdbEngine) {
      case MYSQL:
      case POSTGRESQL:
        execute(connection, "CREATE SCHEMA IF NOT EXISTS " + enclose(getMetadataSchema()));
        break;
      case SQL_SERVER:
        try {
          execute(connection, "CREATE SCHEMA " + enclose(getMetadataSchema()));
        } catch (SQLException e) {
          // Suppress the exception thrown when the schema already exists
          if (e.getErrorCode() != 2714) {
            throw e;
          }
        }

        break;
      case ORACLE:
        try {
          execute(
              connection,
              "CREATE USER " + enclose(getMetadataSchema()) + " IDENTIFIED BY \"oracle\"");
        } catch (SQLException e) {
          // Suppress the exception thrown when the user already exists
          if (e.getErrorCode() != 1920) {
            throw e;
          }
        }
        execute(
            connection, "ALTER USER " + enclose(getMetadataSchema()) + " quota unlimited on USERS");
        break;
    }
  }

  @SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
  private void execute(Connection connection, String sql) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute(sql);
    }
  }

  private String getMetadataSchema() {
    return schemaPrefix.orElse("") + SCHEMA;
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
    createMetadataSchemaAndTableIfNotExist();
    try (Connection connection = dataSource.getConnection()) {
      // Start transaction to commit all the insert statements at once
      connection.setAutoCommit(false);
      connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

      LinkedHashSet<String> orderedColumns = new LinkedHashSet<>(metadata.getPartitionKeyNames());
      orderedColumns.addAll(metadata.getClusteringKeyNames());
      orderedColumns.addAll(metadata.getColumnNames());

      int ordinalPosition = 1;
      try {
        for (String column : orderedColumns) {
          insertMetadataColumn(namespace, table, metadata, connection, ordinalPosition++, column);
        }
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
    return getTableMetadata(getFullTableName(schemaPrefix, namespace, table));
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
      connection.setAutoCommit(false);
      connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
      try {
        execute(connection, getDeleteTableMetadataStatement(namespace, table));
        deleteMetadataSchemaAndTableIfEmpty(connection);
        connection.commit();
      } catch (SQLException e) {
        connection.rollback();
        throw e;
      }

    } catch (SQLException e) {
      if (e.getMessage().contains("Unknown table") || e.getMessage().contains("does not exist")) {
        return;
      }
      throw new StorageRuntimeException(
          String.format(
              "deleting the %s table metadata failed ",
              getFullTableName(schemaPrefix, namespace, table)),
          e);
    }
    tableMetadataCache.put(getFullTableName(schemaPrefix, namespace, table), Optional.empty());
  }

  private String enclose(String name) {
    return QueryUtils.enclose(name, rdbEngine);
  }

  private String encloseFullTableName(String schema, String table) {
    return QueryUtils.enclosedFullTableName(schema, table, rdbEngine);
  }

  @SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
  @Override
  public Set<String> getTableNames(String namespace) {
    String selectTablesOfNamespaceStatement =
        "SELECT DISTINCT "
            + enclose(FULL_TABLE_NAME)
            + " FROM "
            + encloseFullTableName(getMetadataSchema(), TABLE)
            + " WHERE "
            + enclose(FULL_TABLE_NAME)
            + " LIKE ?";
    try (Connection connection = dataSource.getConnection();
        PreparedStatement preparedStatement =
            connection.prepareStatement(selectTablesOfNamespaceStatement)) {
      String fullSchemaName = Utility.getFullNamespaceName(schemaPrefix, namespace);
      preparedStatement.setString(1, fullSchemaName + ".%");
      try (ResultSet results = preparedStatement.executeQuery()) {
        Set<String> tableNames = new HashSet<>();
        while (results.next()) {
          String tableName =
              results.getString(FULL_TABLE_NAME).replaceFirst("^" + fullSchemaName + ".", "");
          tableNames.add(tableName);
        }
        return tableNames;
      }
    } catch (SQLException e) {
      // An exception will be thrown if the metadata table does not exist when executing the select
      // query
      if ((rdbEngine == RdbEngine.MYSQL && e.getErrorCode() == 1049)
          || (rdbEngine == RdbEngine.POSTGRESQL && e.getSQLState().equals("42P01"))
          || (rdbEngine == RdbEngine.ORACLE && e.getErrorCode() == 942)
          || (rdbEngine == RdbEngine.SQL_SERVER && e.getErrorCode() == 208)) {
        return Collections.emptySet();
      }
      throw new StorageRuntimeException(
          "error retrieving the table names of the given namespace", e);
    }
  }

  private void deleteMetadataSchemaAndTableIfEmpty(Connection connection) throws SQLException {
    if (isMetadataTableEmpty(connection)) {
      deleteMetadataTable(connection);
      deleteMetadataSchema(connection);
    }
  }

  private void deleteMetadataSchema(Connection connection) throws SQLException {
    String dropStatement;
    if (rdbEngine == RdbEngine.ORACLE) {
      dropStatement = "DROP USER " + enclose(getFullNamespaceName(schemaPrefix, SCHEMA));
    } else {
      dropStatement = "DROP SCHEMA " + enclose(getFullNamespaceName(schemaPrefix, SCHEMA));
    }

    execute(connection, dropStatement);
  }

  private void deleteMetadataTable(Connection connection) throws SQLException {
    String dropTableStatement =
        "DROP TABLE " + encloseFullTableName(getFullNamespaceName(schemaPrefix, SCHEMA), TABLE);

    execute(connection, dropTableStatement);
  }

  @SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
  private boolean isMetadataTableEmpty(Connection connection) throws SQLException {
    String selectAllTables =
        "SELECT DISTINCT "
            + enclose(FULL_TABLE_NAME)
            + " FROM "
            + encloseFullTableName(getMetadataSchema(), TABLE);
    try (Statement statement = connection.createStatement();
        ResultSet results = statement.executeQuery(selectAllTables)) {
      return !results.next();
    }
  }
}
