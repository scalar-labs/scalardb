package com.scalar.db.storage.jdbc;

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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
        + encloseFullTableNames(getMetadataSchema(), TABLE)
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
      int ordinalPosition)
      throws SQLException {

    return String.format(
        "INSERT INTO %s VALUES ('%s','%s','%s',%s,%s,%s,%d)",
        encloseFullTableNames(getMetadataSchema(), TABLE),
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

  private String getDeleteTableMetadataStatement(String schema, String table) {
    String fullTableName = schemaPrefix.orElse("") + schema + "." + table;
    return "DELETE FROM "
        + encloseFullTableNames(getMetadataSchema(), TABLE)
        + " WHERE "
        + enclose(FULL_TABLE_NAME)
        + " = '"
        + fullTableName
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

  private void createTableIfNotExists(Connection connection) throws SQLException {
    String createTableStatement =
        String.format("CREATE TABLE %s(", encloseFullTableNames(getMetadataSchema(), TABLE))
            + String.format("%s %s,", enclose(FULL_TABLE_NAME), getTextType(128))
            + String.format("%s %s,", enclose(COLUMN_NAME), getTextType(128))
            + String.format("%s %s NOT NULL,", enclose(DATA_TYPE), getTextType(20))
            + String.format("%s %s,", enclose(KEY_TYPE), getTextType(20))
            + String.format("%s %s,", enclose(CLUSTERING_ORDER), getTextType(10))
            + String.format("%s %s NOT NULL,", enclose(INDEXED), getBooleanType())
            + String.format("%s INTEGER NOT NULL,", enclose(ORDINAL_POSITION))
            + String.format(
                "PRIMARY KEY (%s, %s))", enclose(FULL_TABLE_NAME), enclose(COLUMN_NAME));
    String createTableIfNotExistsStatement;
    switch (rdbEngine) {
      case ORACLE:
        // Save into the tableExistCount variable if the metadata table
        // exists (1=true, 0=false) by querying a system table
        String selectTableExistStatement =
            String.format(
                "SELECT COUNT(*) INTO tableExistCount FROM dba_tables WHERE owner='%s' AND table_name='%s';",
                getMetadataSchema(), TABLE);
        createTableStatement = "'" + createTableStatement + "';";
        // Will run the above count request first and if the result is false, will execute the
        // create
        // table statement
        createTableIfNotExistsStatement =
            "DECLARE\n"
                + "tableExistCount integer;\n"
                + "BEGIN\n"
                + selectTableExistStatement
                + "\n"
                + "IF (tableExistCount = 0) THEN EXECUTE IMMEDIATE "
                + createTableStatement
                + "\n"
                + "END IF;\n"
                + "END;";
        break;
      case POSTGRESQL:
      case MYSQL:
        createTableIfNotExistsStatement =
            createTableStatement.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");
        break;
      case SQL_SERVER:
        String selectTableQuery =
            // Will return a row if the metadata table exists
            String.format(
                "SELECT * "
                    + "FROM [sys].[tables] "
                    + "INNER JOIN [sys].[schemas] "
                    + "ON [sys].[tables].[schema_id]=[sys].[schemas].[schema_id] "
                    + "WHERE "
                    + "[sys].[schemas].[name] = '%s' AND [sys].[tables].[name]  = '%s'",
                getMetadataSchema(), TABLE);
        // If the metadata table do not exists, the create table statement will be executed
        createTableIfNotExistsStatement =
            "IF NOT EXISTS (" + selectTableQuery + ") " + createTableStatement;
        break;
      default:
        throw new UnsupportedOperationException("rdb engine not supported");
    }
    execute(connection, createTableIfNotExistsStatement);
  }

  private void createMetadataSchemaAndTableIfNotExist() {
    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);
      connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
      try {
        createMetadataSchemaIfNotExists(connection);
        createTableIfNotExists(connection);
      } catch (SQLException e) {
        connection.rollback();
        throw e;
      }
      connection.commit();
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
        // Query into a system table to see if the schema is already created
        String selectQuery =
            String.format(
                "SELECT * " + "FROM [sys].[schemas] " + "WHERE [sys].[schemas].[name]  = '%s'",
                getMetadataSchema());
        // If the schema do not already exist, run the create schema statement
        execute(
            connection,
            "IF NOT EXISTS ("
                + selectQuery
                + ") EXEC ('CREATE SCHEMA "
                + enclose(getMetadataSchema())
                + "')");
        break;
      case ORACLE:
        // Query into a system table to see if the user already exist and save the result into the
        // userExistCount variable(true=1, false=0)
        String selectUserExistStatement =
            "SELECT COUNT(*) INTO userExistCount FROM dba_users WHERE username='"
                + getMetadataSchema()
                + "';";
        String createUserStatement =
            "'CREATE USER " + enclose(getMetadataSchema()) + " IDENTIFIED BY \"oracle\"';";
        // If the user doest not exist, will execute the create user statement
        execute(
            connection,
            "DECLARE "
                + "userExistCount integer;\n"
                + "BEGIN\n"
                + selectUserExistStatement
                + " IF (userExistCount = 0) THEN EXECUTE IMMEDIATE "
                + createUserStatement
                + "\n"
                + "END IF;\n"
                + "END;");
        execute(
            connection, "ALTER USER " + enclose(getMetadataSchema()) + " quota unlimited on USERS");
        break;
    }
  }

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
      execute(connection, getDeleteTableMetadataStatement(namespace, table));
    } catch (SQLException e) {
      if (e.getMessage().contains("Unknown table") || e.getMessage().contains("does not exist")) {
        return;
      }
      throw new StorageRuntimeException(
          String.format(
              "deleting the %s table metadata failed",
              schemaPrefix.orElse("") + namespace + "." + table),
          e);
    }
    tableMetadataCache.put(schemaPrefix.orElse("") + namespace + "." + table, Optional.empty());
  }

  private String enclose(String name) {
    return QueryUtils.enclose(name, rdbEngine);
  }

  private String encloseFullTableNames(String schema, String table) {
    return QueryUtils.enclosedFullTableName(schema, table, rdbEngine);
  }
}
