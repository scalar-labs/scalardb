package com.scalar.db.storage.jdbc;

import static com.scalar.db.storage.jdbc.JdbcUtils.getJdbcType;
import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.RdbTableMetadata.IndexColumn;
import com.scalar.db.storage.jdbc.RdbTableMetadata.PrimaryKeyColumn;
import com.scalar.db.storage.jdbc.RdbTableMetadata.SortOrder;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressFBWarnings({"OBL_UNSATISFIED_OBLIGATION", "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE"})
@ThreadSafe
public class JdbcAdmin implements DistributedStorageAdmin {
  public static final String METADATA_TABLE = "metadata";

  @VisibleForTesting static final String METADATA_COL_FULL_TABLE_NAME = "full_table_name";
  @VisibleForTesting static final String METADATA_COL_COLUMN_NAME = "column_name";
  @VisibleForTesting static final String METADATA_COL_DATA_TYPE = "data_type";
  @VisibleForTesting static final String METADATA_COL_KEY_TYPE = "key_type";
  @VisibleForTesting static final String METADATA_COL_CLUSTERING_ORDER = "clustering_order";
  @VisibleForTesting static final String METADATA_COL_INDEXED = "indexed";
  @VisibleForTesting static final String METADATA_COL_ORDINAL_POSITION = "ordinal_position";
  @VisibleForTesting static final String JDBC_COL_COLUMN_NAME = "COLUMN_NAME";
  @VisibleForTesting static final String JDBC_COL_DATA_TYPE = "DATA_TYPE";
  @VisibleForTesting static final String JDBC_COL_TYPE_NAME = "TYPE_NAME";
  @VisibleForTesting static final String JDBC_COL_COLUMN_SIZE = "COLUMN_SIZE";
  @VisibleForTesting static final String JDBC_COL_DECIMAL_DIGITS = "DECIMAL_DIGITS";
  @VisibleForTesting static final String JDBC_COL_KEY_SEQ = "KEY_SEQ";
  @VisibleForTesting static final String JDBC_INDEX_COLUMN_NAME = "COLUMN_NAME";
  @VisibleForTesting static final String JDBC_INDEX_INDEX_NAME = "INDEX_NAME";
  @VisibleForTesting static final String JDBC_INDEX_ASC_OR_DESC = "ASC_OR_DESC";
  public static final String NAMESPACES_TABLE = "namespaces";
  @VisibleForTesting static final String NAMESPACE_COL_NAMESPACE_NAME = "namespace_name";
  private static final Logger logger = LoggerFactory.getLogger(JdbcAdmin.class);
  private static final String INDEX_NAME_PREFIX = "index";

  private final RdbEngineStrategy rdbEngine;
  private final BasicDataSource dataSource;
  private final String metadataSchema;

  @Inject
  public JdbcAdmin(DatabaseConfig databaseConfig) {
    JdbcConfig config = new JdbcConfig(databaseConfig);
    rdbEngine = RdbEngineFactory.create(config);
    dataSource = JdbcUtils.initDataSourceForAdmin(config, rdbEngine);
    metadataSchema = config.getMetadataSchema();
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public JdbcAdmin(BasicDataSource dataSource, JdbcConfig config) {
    rdbEngine = RdbEngineFactory.create(config);
    this.dataSource = dataSource;
    metadataSchema = config.getMetadataSchema();
  }

  @Override
  public void createNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    if (!rdbEngine.isValidNamespaceOrTableName(namespace)) {
      throw new IllegalArgumentException(
          CoreError.JDBC_NAMESPACE_NAME_NOT_ACCEPTABLE.buildMessage(namespace));
    }
    try (Connection connection = dataSource.getConnection()) {
      execute(connection, rdbEngine.createSchemaSqls(namespace));
      createNamespacesTableIfNotExists(connection);
      insertIntoNamespacesTable(connection, namespace);
    } catch (SQLException e) {
      throw new ExecutionException("Creating the " + namespace + " schema failed", e);
    }
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    try (Connection connection = dataSource.getConnection()) {
      createNamespacesTableIfNotExists(connection);
      createTableInternal(connection, namespace, table, metadata, false);
      addTableMetadata(connection, namespace, table, metadata, true, false);
    } catch (SQLException e) {
      throw new ExecutionException(
          "Creating the " + getFullTableName(namespace, table) + " table failed ", e);
    }
  }

  @VisibleForTesting
  void createTableInternal(
      Connection connection,
      String schema,
      String table,
      TableMetadata metadata,
      boolean ifNotExists)
      throws SQLException {
    if (!rdbEngine.isValidNamespaceOrTableName(table)) {
      throw new IllegalArgumentException(
          CoreError.JDBC_TABLE_NAME_NOT_ACCEPTABLE.buildMessage(table));
    }
    String createTableStatement = "CREATE TABLE " + encloseFullTableName(schema, table) + "(";
    // Order the columns for their creation by (partition keys >> clustering keys >> other columns)
    LinkedHashSet<String> sortedColumnNames =
        Sets.newLinkedHashSet(
            Iterables.concat(
                metadata.getPartitionKeyNames(),
                metadata.getClusteringKeyNames(),
                metadata.getColumnNames()));
    // Add columns definition
    createTableStatement +=
        sortedColumnNames.stream()
            .map(
                columnName ->
                    enclose(columnName) + " " + getVendorDbColumnType(metadata, columnName))
            .collect(Collectors.joining(","));

    // Add primary key definition
    createTableStatement +=
        ", "
            + rdbEngine.createTableInternalPrimaryKeyClause(
                hasDescClusteringOrder(metadata), metadata);
    createTable(connection, createTableStatement, ifNotExists);
    createTableInternalSqlsAfterCreateTable(connection, schema, table, metadata, ifNotExists);
    createIndex(connection, schema, table, metadata, ifNotExists);
  }

  private void createIndex(
      Connection connection,
      String schema,
      String table,
      TableMetadata metadata,
      boolean ifNotExists)
      throws SQLException {
    for (String indexedColumn : metadata.getSecondaryIndexNames()) {
      createIndex(connection, schema, table, indexedColumn, ifNotExists);
    }
  }

  @VisibleForTesting
  void addTableMetadata(
      Connection connection,
      String namespace,
      String table,
      TableMetadata metadata,
      boolean createMetadataTable,
      boolean overwriteMetadata)
      throws SQLException {
    if (createMetadataTable) {
      createMetadataSchemaAndTableIfNotExists(connection);
    }
    if (overwriteMetadata) {
      // Delete the metadata for the table before we add them
      execute(connection, getDeleteTableMetadataStatement(namespace, table));
    }
    LinkedHashSet<String> orderedColumns = new LinkedHashSet<>(metadata.getPartitionKeyNames());
    orderedColumns.addAll(metadata.getClusteringKeyNames());
    orderedColumns.addAll(metadata.getColumnNames());
    int ordinalPosition = 1;
    for (String column : orderedColumns) {
      insertMetadataColumn(namespace, table, metadata, connection, ordinalPosition++, column);
    }
  }

  private void createMetadataSchemaAndTableIfNotExists(Connection connection) throws SQLException {
    createSchemaIfNotExists(connection, metadataSchema);
    createMetadataTableIfNotExists(connection);
  }

  private void createSchemaIfNotExists(Connection connection, String schema) throws SQLException {
    String[] sqls = rdbEngine.createSchemaIfNotExistsSqls(schema);
    try {
      execute(connection, sqls);
    } catch (SQLException e) {
      // Suppress exceptions indicating the duplicate metadata schema
      if (!rdbEngine.isCreateMetadataSchemaDuplicateSchemaError(e)) {
        throw e;
      }
    }
  }

  @VisibleForTesting
  void createMetadataTableIfNotExists(Connection connection) throws SQLException {
    String createTableStatement =
        "CREATE TABLE "
            + encloseFullTableName(metadataSchema, METADATA_TABLE)
            + "("
            + enclose(METADATA_COL_FULL_TABLE_NAME)
            + " "
            + getTextType(128)
            + ","
            + enclose(METADATA_COL_COLUMN_NAME)
            + " "
            + getTextType(128)
            + ","
            + enclose(METADATA_COL_DATA_TYPE)
            + " "
            + getTextType(20)
            + " NOT NULL,"
            + enclose(METADATA_COL_KEY_TYPE)
            + " "
            + getTextType(20)
            + ","
            + enclose(METADATA_COL_CLUSTERING_ORDER)
            + " "
            + getTextType(10)
            + ","
            + enclose(METADATA_COL_INDEXED)
            + " "
            + getBooleanType()
            + " NOT NULL,"
            + enclose(METADATA_COL_ORDINAL_POSITION)
            + " INTEGER NOT NULL,"
            + "PRIMARY KEY ("
            + enclose(METADATA_COL_FULL_TABLE_NAME)
            + ", "
            + enclose(METADATA_COL_COLUMN_NAME)
            + "))";

    createTable(connection, createTableStatement, true);
  }

  private void createTable(Connection connection, String createTableStatement, boolean ifNotExists)
      throws SQLException {
    String stmt = createTableStatement;
    if (ifNotExists) {
      stmt = rdbEngine.tryAddIfNotExistsToCreateTableSql(createTableStatement);
    }
    try {
      execute(connection, stmt);
    } catch (SQLException e) {
      // Suppress the exception thrown when the table already exists
      if (!(ifNotExists && rdbEngine.isDuplicateTableError(e))) {
        throw e;
      }
    }
  }

  private void createTableInternalSqlsAfterCreateTable(
      Connection connection,
      String schema,
      String table,
      TableMetadata metadata,
      boolean ifNotExists)
      throws SQLException {
    String[] stmts =
        rdbEngine.createTableInternalSqlsAfterCreateTable(
            hasDifferentClusteringOrders(metadata), schema, table, metadata, ifNotExists);
    try {
      execute(connection, stmts);
    } catch (SQLException e) {
      if (!(ifNotExists && rdbEngine.isDuplicateIndexError(e))) {
        throw e;
      }
    }
  }

  private String getTextType(int charLength) {
    return rdbEngine.getTextType(charLength);
  }

  private String getBooleanType() {
    return rdbEngine.getDataTypeForEngine(DataType.BOOLEAN);
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
        encloseFullTableName(metadataSchema, METADATA_TABLE),
        getFullTableName(schema, table),
        columnName,
        dataType.toString(),
        keyType != null ? "'" + keyType + "'" : "NULL",
        ckOrder != null ? "'" + ckOrder + "'" : "NULL",
        computeBooleanValue(indexed),
        ordinalPosition);
  }

  private String computeBooleanValue(boolean value) {
    return rdbEngine.computeBooleanValue(value);
  }

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    try (Connection connection = dataSource.getConnection()) {
      dropTableInternal(connection, namespace, table);
      deleteTableMetadata(connection, namespace, table);
      deleteNamespacesTableAndMetadataSchemaIfEmpty(connection);
    } catch (SQLException e) {
      throw new ExecutionException(
          "Dropping the " + getFullTableName(namespace, table) + " table failed", e);
    }
  }

  private void dropTableInternal(Connection connection, String schema, String table)
      throws SQLException {
    String dropTableStatement = "DROP TABLE " + encloseFullTableName(schema, table);
    execute(connection, dropTableStatement);
  }

  private void deleteTableMetadata(Connection connection, String namespace, String table)
      throws SQLException {
    try {
      execute(connection, getDeleteTableMetadataStatement(namespace, table));
      deleteMetadataTableIfEmpty(connection);
    } catch (SQLException e) {
      if (e.getMessage().contains("Unknown table") || e.getMessage().contains("does not exist")) {
        return;
      }
      throw e;
    }
  }

  private String getDeleteTableMetadataStatement(String schema, String table) {
    return "DELETE FROM "
        + encloseFullTableName(metadataSchema, METADATA_TABLE)
        + " WHERE "
        + enclose(METADATA_COL_FULL_TABLE_NAME)
        + " = '"
        + getFullTableName(schema, table)
        + "'";
  }

  private void deleteMetadataTableIfEmpty(Connection connection) throws SQLException {
    if (isMetadataTableEmpty(connection)) {
      deleteTable(connection, encloseFullTableName(metadataSchema, METADATA_TABLE));
    }
  }

  private boolean isMetadataTableEmpty(Connection connection) throws SQLException {
    String selectAllTables =
        "SELECT DISTINCT "
            + enclose(METADATA_COL_FULL_TABLE_NAME)
            + " FROM "
            + encloseFullTableName(metadataSchema, METADATA_TABLE);
    try (Statement statement = connection.createStatement();
        ResultSet results = statement.executeQuery(selectAllTables)) {
      return !results.next();
    }
  }

  private void deleteTable(Connection connection, String fullTableName) throws SQLException {
    String dropTableStatement = "DROP TABLE " + fullTableName;

    execute(connection, dropTableStatement);
  }

  private void deleteMetadataSchema(Connection connection) throws SQLException {
    String sql = rdbEngine.deleteMetadataSchemaSql(metadataSchema);
    execute(connection, sql);
  }

  @Override
  public void dropNamespace(String namespace) throws ExecutionException {
    try (Connection connection = dataSource.getConnection()) {
      execute(connection, rdbEngine.dropNamespaceSql(namespace));
      deleteFromNamespacesTable(connection, namespace);
      deleteNamespacesTableAndMetadataSchemaIfEmpty(connection);
    } catch (SQLException e) {
      rdbEngine.dropNamespaceTranslateSQLException(e, namespace);
    }
  }

  @Override
  public void truncateTable(String namespace, String table) throws ExecutionException {
    String truncateTableStatement = rdbEngine.truncateTableSql(namespace, table);
    try (Connection connection = dataSource.getConnection()) {
      execute(connection, truncateTableStatement);
    } catch (SQLException e) {
      throw new ExecutionException(
          "Truncating the " + getFullTableName(namespace, table) + " table failed", e);
    }
  }

  @Override
  public TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException {
    TableMetadata.Builder builder = TableMetadata.newBuilder();
    boolean tableExists = false;

    try (Connection connection = dataSource.getConnection();
        PreparedStatement preparedStatement =
            connection.prepareStatement(getSelectColumnsStatement())) {
      preparedStatement.setString(1, getFullTableName(namespace, table));

      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        while (resultSet.next()) {
          tableExists = true;

          String columnName = resultSet.getString(METADATA_COL_COLUMN_NAME);
          DataType dataType = DataType.valueOf(resultSet.getString(METADATA_COL_DATA_TYPE));
          builder.addColumn(columnName, dataType);

          boolean indexed = resultSet.getBoolean(METADATA_COL_INDEXED);
          if (indexed) {
            builder.addSecondaryIndex(columnName);
          }

          String keyType = resultSet.getString(METADATA_COL_KEY_TYPE);
          if (keyType == null) {
            continue;
          }

          switch (KeyType.valueOf(keyType)) {
            case PARTITION:
              builder.addPartitionKey(columnName);
              break;
            case CLUSTERING:
              Scan.Ordering.Order clusteringOrder =
                  Scan.Ordering.Order.valueOf(resultSet.getString(METADATA_COL_CLUSTERING_ORDER));
              builder.addClusteringKey(columnName, clusteringOrder);
              break;
            default:
              throw new AssertionError("Invalid key type: " + keyType);
          }
        }
      }
    } catch (SQLException e) {
      // An exception will be thrown if the namespace table does not exist when executing the select
      // query
      if (rdbEngine.isUndefinedTableError(e)) {
        return null;
      }
      throw new ExecutionException(
          "Getting a table metadata for the "
              + getFullTableName(namespace, table)
              + " table failed",
          e);
    }

    if (!tableExists) {
      return null;
    }

    return builder.build();
  }

  @Override
  public TableMetadata getImportTableMetadata(String namespace, String table)
      throws ExecutionException {
    TableMetadata.Builder builder = TableMetadata.newBuilder();
    boolean primaryKeyExists = false;

    if (!rdbEngine.isImportable()) {
      throw new UnsupportedOperationException(
          CoreError.JDBC_IMPORT_NOT_SUPPORTED.buildMessage(rdbEngine.getClass().getName()));
    }

    try (Connection connection = dataSource.getConnection()) {
      String catalogName = rdbEngine.getCatalogName(namespace);
      String schemaName = rdbEngine.getSchemaName(namespace);

      if (!tableExistsInternal(connection, namespace, table)) {
        throw new IllegalArgumentException(
            CoreError.TABLE_NOT_FOUND.buildMessage(getFullTableName(namespace, table)));
      }

      DatabaseMetaData metadata = connection.getMetaData();
      ResultSet resultSet = metadata.getPrimaryKeys(catalogName, schemaName, table);
      while (resultSet.next()) {
        primaryKeyExists = true;
        String columnName = resultSet.getString(JDBC_COL_COLUMN_NAME);
        builder.addPartitionKey(columnName);
      }

      if (!primaryKeyExists) {
        throw new IllegalStateException(
            CoreError.JDBC_IMPORT_TABLE_WITHOUT_PRIMARY_KEY.buildMessage(
                getFullTableName(namespace, table)));
      }

      resultSet = metadata.getColumns(catalogName, schemaName, table, "%");
      while (resultSet.next()) {
        String columnName = resultSet.getString(JDBC_COL_COLUMN_NAME);
        builder.addColumn(
            columnName,
            rdbEngine.getDataTypeForScalarDb(
                getJdbcType(resultSet.getInt(JDBC_COL_DATA_TYPE)),
                resultSet.getString(JDBC_COL_TYPE_NAME),
                resultSet.getInt(JDBC_COL_COLUMN_SIZE),
                resultSet.getInt(JDBC_COL_DECIMAL_DIGITS),
                getFullTableName(namespace, table) + " " + columnName));
      }
    } catch (SQLException e) {
      throw new ExecutionException(
          "Getting a table metadata for the "
              + getFullTableName(namespace, table)
              + " table failed",
          e);
    }

    return builder.build();
  }

  @Override
  public void importTable(String namespace, String table, Map<String, String> options)
      throws ExecutionException {
    try (Connection connection = dataSource.getConnection()) {
      TableMetadata tableMetadata = getImportTableMetadata(namespace, table);
      createNamespacesTableIfNotExists(connection);
      upsertIntoNamespacesTable(connection, namespace);
      addTableMetadata(connection, namespace, table, tableMetadata, true, false);
    } catch (SQLException | ExecutionException e) {
      throw new ExecutionException(
          String.format("Importing the %s table failed", getFullTableName(namespace, table)), e);
    }
  }

  private String getSelectColumnsStatement() {
    return "SELECT "
        + enclose(METADATA_COL_COLUMN_NAME)
        + ","
        + enclose(METADATA_COL_DATA_TYPE)
        + ","
        + enclose(METADATA_COL_KEY_TYPE)
        + ","
        + enclose(METADATA_COL_CLUSTERING_ORDER)
        + ","
        + enclose(METADATA_COL_INDEXED)
        + " FROM "
        + encloseFullTableName(metadataSchema, METADATA_TABLE)
        + " WHERE "
        + enclose(METADATA_COL_FULL_TABLE_NAME)
        + "=? ORDER BY "
        + enclose(METADATA_COL_ORDINAL_POSITION)
        + " ASC";
  }

  @Override
  public Set<String> getNamespaceTableNames(String namespace) throws ExecutionException {
    String selectTablesOfNamespaceStatement =
        "SELECT DISTINCT "
            + enclose(METADATA_COL_FULL_TABLE_NAME)
            + " FROM "
            + encloseFullTableName(metadataSchema, METADATA_TABLE)
            + " WHERE "
            + enclose(METADATA_COL_FULL_TABLE_NAME)
            + " LIKE ?";
    try (Connection connection = dataSource.getConnection();
        PreparedStatement preparedStatement =
            connection.prepareStatement(selectTablesOfNamespaceStatement)) {
      String prefix = namespace + ".";
      preparedStatement.setString(1, prefix + "%");
      try (ResultSet results = preparedStatement.executeQuery()) {
        Set<String> tableNames = new HashSet<>();
        while (results.next()) {
          String tableName =
              results.getString(METADATA_COL_FULL_TABLE_NAME).substring(prefix.length());
          tableNames.add(tableName);
        }
        return tableNames;
      }
    } catch (SQLException e) {
      // An exception will be thrown if the metadata table does not exist when executing the select
      // query
      if (rdbEngine.isUndefinedTableError(e)) {
        return Collections.emptySet();
      }
      throw new ExecutionException(
          "Getting the list of tables of the " + namespace + " schema failed", e);
    }
  }

  @Override
  public boolean namespaceExists(String namespace) throws ExecutionException {
    String selectQuery =
        "SELECT 1 FROM "
            + encloseFullTableName(metadataSchema, NAMESPACES_TABLE)
            + " WHERE "
            + enclose(NAMESPACE_COL_NAMESPACE_NAME)
            + " = ?";
    try (Connection connection = dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(selectQuery)) {
      statement.setString(1, namespace);
      try (ResultSet resultSet = statement.executeQuery()) {
        return resultSet.next();
      }
    } catch (SQLException e) {
      // An exception will be thrown if the namespaces table does not exist when executing the
      // select query
      if (rdbEngine.isUndefinedTableError(e)) {
        return false;
      }
      throw new ExecutionException("Checking if the " + namespace + " schema exists failed", e);
    }
  }

  @Override
  public void close() {
    try {
      dataSource.close();
    } catch (SQLException e) {
      logger.warn("Failed to close the dataSource", e);
    }
  }

  /**
   * Get the vendor DB data type that is equivalent to the ScalarDB data type
   *
   * @param metadata a table metadata
   * @param columnName a column name
   * @return a vendor DB data type
   */
  private String getVendorDbColumnType(TableMetadata metadata, String columnName) {
    HashSet<String> keysAndIndexes =
        Sets.newHashSet(
            Iterables.concat(
                metadata.getPartitionKeyNames(),
                metadata.getClusteringKeyNames(),
                metadata.getSecondaryIndexNames()));
    DataType scalarDbColumnType = metadata.getColumnDataType(columnName);

    String dataType = rdbEngine.getDataTypeForEngine(scalarDbColumnType);
    if (keysAndIndexes.contains(columnName)) {
      String indexDataType = rdbEngine.getDataTypeForKey(scalarDbColumnType);
      return Optional.ofNullable(indexDataType).orElse(dataType);
    } else {
      return dataType;
    }
  }

  private static boolean hasDescClusteringOrder(TableMetadata metadata) {
    return metadata.getClusteringKeyNames().stream()
        .anyMatch(c -> metadata.getClusteringOrder(c) == Order.DESC);
  }

  @VisibleForTesting
  static boolean hasDifferentClusteringOrders(TableMetadata metadata) {
    boolean hasAscOrder = false;
    boolean hasDescOrder = false;
    for (Order order : metadata.getClusteringOrders().values()) {
      if (order == Order.ASC) {
        hasAscOrder = true;
      } else {
        hasDescOrder = true;
      }
    }
    return hasAscOrder && hasDescOrder;
  }

  @Override
  public void createIndex(
      String namespace, String table, String columnName, Map<String, String> options)
      throws ExecutionException {
    try (Connection connection = dataSource.getConnection()) {
      alterToIndexColumnTypeIfNecessary(connection, namespace, table, columnName);
      createIndex(connection, namespace, table, columnName, false);
      updateTableMetadata(connection, namespace, table, columnName, true);
    } catch (ExecutionException | SQLException e) {
      throw new ExecutionException(
          String.format(
              "Creating the secondary index on the %s column for the %s table failed",
              columnName, getFullTableName(namespace, table)),
          e);
    }
  }

  private void alterToIndexColumnTypeIfNecessary(
      Connection connection, String namespace, String table, String columnName)
      throws ExecutionException, SQLException {
    DataType indexType = getTableMetadata(namespace, table).getColumnDataType(columnName);
    String columnTypeForKey = rdbEngine.getDataTypeForKey(indexType);
    if (columnTypeForKey == null) {
      // The column type does not need to be altered to be compatible with being secondary index
      return;
    }

    String sql = rdbEngine.alterColumnTypeSql(namespace, table, columnName, columnTypeForKey);
    execute(connection, sql);
  }

  private void alterToRegularColumnTypeIfNecessary(
      Connection connection, String namespace, String table, String columnName)
      throws ExecutionException, SQLException {
    DataType indexType = getTableMetadata(namespace, table).getColumnDataType(columnName);
    String columnTypeForKey = rdbEngine.getDataTypeForKey(indexType);
    if (columnTypeForKey == null) {
      // The column type is already the type for a regular column. It was not altered to be
      // compatible with being a secondary index, so no alteration is necessary.
      return;
    }

    String columnType = rdbEngine.getDataTypeForEngine(indexType);
    String sql = rdbEngine.alterColumnTypeSql(namespace, table, columnName, columnType);
    execute(connection, sql);
  }

  @Override
  public void dropIndex(String namespace, String table, String columnName)
      throws ExecutionException {
    try (Connection connection = dataSource.getConnection()) {
      dropIndex(connection, namespace, table, columnName);
      alterToRegularColumnTypeIfNecessary(connection, namespace, table, columnName);
      updateTableMetadata(connection, namespace, table, columnName, false);
    } catch (SQLException e) {
      throw new ExecutionException(
          String.format(
              "Dropping the secondary index on the %s column for the %s table failed",
              columnName, getFullTableName(namespace, table)),
          e);
    }
  }

  private boolean tableExistsInternal(Connection connection, String namespace, String table)
      throws ExecutionException {
    String fullTableName = encloseFullTableName(namespace, table);
    String sql = rdbEngine.tableExistsInternalTableCheckSql(fullTableName);
    try {
      execute(connection, sql);
      return true;
    } catch (SQLException e) {
      // An exception will be thrown if the table does not exist when executing the select
      // query
      if (rdbEngine.isUndefinedTableError(e)) {
        return false;
      }
      throw new ExecutionException(
          String.format(
              "Checking if the %s table exists failed", getFullTableName(namespace, table)),
          e);
    }
  }

  @Override
  public void repairNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    if (!rdbEngine.isValidNamespaceOrTableName(namespace)) {
      throw new IllegalArgumentException(
          CoreError.JDBC_NAMESPACE_NAME_NOT_ACCEPTABLE.buildMessage(namespace));
    }
    try (Connection connection = dataSource.getConnection()) {
      createSchemaIfNotExists(connection, namespace);
      createNamespacesTableIfNotExists(connection);
      upsertIntoNamespacesTable(connection, namespace);
    } catch (SQLException e) {
      throw new ExecutionException(String.format("Repairing the %s schema failed", namespace), e);
    }
  }

  @VisibleForTesting
  Optional<RdbTableMetadata> getRdbTableMetadata(String namespace, String table)
      throws ExecutionException {
    try (Connection connection = dataSource.getConnection()) {
      String catalogName = rdbEngine.getCatalogName(namespace);
      String schemaName = rdbEngine.getSchemaName(namespace);
      String tableName = rdbEngine.getTableName(namespace, table);

      if (!tableExistsInternal(connection, namespace, table)) {
        return Optional.empty();
      }

      DatabaseMetaData metadata = connection.getMetaData();

      // Collect the primary key for partition keys and clustering keys.
      ResultSet resultSet = metadata.getPrimaryKeys(catalogName, schemaName, tableName);
      Map<Integer, String> primaryKeysWithSeq = new HashMap<>();
      while (resultSet.next()) {
        int keySeq = resultSet.getInt(JDBC_COL_KEY_SEQ);
        String columnName = resultSet.getString(JDBC_COL_COLUMN_NAME);
        primaryKeysWithSeq.put(keySeq, columnName);
      }

      // Collect the index information for clustering keys and secondary index.
      Map<String, PrimaryKeyColumn> primaryKeyIndex = new HashMap<>();
      ImmutableSet<IndexColumn> normalIndexColumns = null;
      if (rdbEngine.isIndexInfoSupported(metadata)) {
        Optional<String> primaryKeyIndexName = rdbEngine.getPrimaryKeyIndexName(namespace, table);
        Set<IndexColumn> normalIndexes = new HashSet<>();
        ResultSet indexInfo =
            metadata.getIndexInfo(catalogName, schemaName, tableName, false, false);
        while (indexInfo.next()) {
          String colName = indexInfo.getString(JDBC_INDEX_COLUMN_NAME);
          if (colName == null) {
            continue;
          }
          String indexName = indexInfo.getString(JDBC_INDEX_INDEX_NAME);
          String ascOrDesc = indexInfo.getString(JDBC_INDEX_ASC_OR_DESC);
          SortOrder sortOrder = SortOrder.UNKNOWN;
          if (ascOrDesc != null) {
            if (ascOrDesc.equals("A")) {
              sortOrder = SortOrder.ASC;
            } else if (ascOrDesc.equals("D")) {
              sortOrder = SortOrder.DESC;
            } else {
              throw new IllegalStateException("Unexpected value for ASC_OR_DESC: " + ascOrDesc);
            }
          }

          // There are possibly 2 indexes for the primary key. The first one is an index created
          // with CREATE TABLE (PRIMARY KEY). The second one is a custom index created by CREATE
          // INDEX for RDB engines that can't create primary keys with different orderings.
          // If the second index exists, the first index isn't used for the ScalarDB clustering
          // keys.
          if (primaryKeyIndexName.isPresent() && indexName.equals(primaryKeyIndexName.get())) {
            primaryKeyIndex.put(colName, new PrimaryKeyColumn(colName, sortOrder));
          } else if (rdbEngine.isDefaultPrimaryKeyIndex(namespace, table, indexName)) {
            if (!primaryKeyIndex.containsKey(colName)) {
              primaryKeyIndex.put(colName, new PrimaryKeyColumn(colName, sortOrder));
            }
          } else if (indexName.equals(getIndexName(namespace, table, colName))) {
            normalIndexes.add(new IndexColumn(colName, sortOrder));
          }
        }

        normalIndexColumns = ImmutableSet.copyOf(normalIndexes);
      }

      // Sort the primary keys by the key sequence.
      ImmutableList<PrimaryKeyColumn> primaryKeyColumns;
      {
        ImmutableList.Builder<PrimaryKeyColumn> builder = ImmutableList.builder();
        for (int keySeq :
            primaryKeysWithSeq.keySet().stream().sorted().collect(Collectors.toList())) {
          String colName = primaryKeysWithSeq.get(keySeq);
          // Use primary key column info if it's already collected with index key column ordering.
          PrimaryKeyColumn primaryKeyColumn = primaryKeyIndex.get(colName);
          if (primaryKeyColumn != null) {
            builder.add(primaryKeyColumn);
          } else {
            builder.add(new PrimaryKeyColumn(colName, SortOrder.UNKNOWN));
          }
        }
        primaryKeyColumns = builder.build();
      }

      // Collect the column information.
      ImmutableMap.Builder<String, DataType> columnBuilder = ImmutableMap.builder();
      resultSet = metadata.getColumns(catalogName, schemaName, tableName, "%");
      while (resultSet.next()) {
        String columnName = resultSet.getString(JDBC_COL_COLUMN_NAME);
        JDBCType jdbcType = getJdbcType(resultSet.getInt(JDBC_COL_DATA_TYPE));
        String typeName = resultSet.getString(JDBC_COL_TYPE_NAME);
        int colSize = resultSet.getInt(JDBC_COL_COLUMN_SIZE);
        int colDigit = resultSet.getInt(JDBC_COL_DECIMAL_DIGITS);
        String colDesc = getFullTableName(namespace, table) + " " + columnName;
        DataType scalarDbDataType;
        scalarDbDataType =
            rdbEngine.getDataTypeForScalarDbLeniently(
                jdbcType, typeName, colSize, colDigit, colDesc);
        columnBuilder.put(columnName, scalarDbDataType);
      }
      return Optional.of(
          new RdbTableMetadata(primaryKeyColumns, normalIndexColumns, columnBuilder.build()));
    } catch (SQLException e) {
      throw new ExecutionException(
          "Getting a table metadata for the "
              + getFullTableName(namespace, table)
              + " table failed",
          e);
    }
  }

  @VisibleForTesting
  void checkRawTableSchemaForRepairTable(String namespace, String table, TableMetadata metadata)
      throws ExecutionException {
    // Repairing table is supposed to be used for the following purposes:
    // 1. To create a table in the underlying RDBMS with the expected metadata if the table
    //    doesn't exist.
    // 2. To update the ScalarDB metadata table to synchronize it with the expected state.
    //
    // Also, the ScalarDB metadata table and the RDB table schema must be consistent. Therefore,
    // the operation should fail if the RDB table already exists with inconsistent schema.
    Optional<RdbTableMetadata> optRdbTableMetadata = getRdbTableMetadata(namespace, table);
    if (!optRdbTableMetadata.isPresent()) {
      return;
    }

    RdbTableMetadata rdbTableMetadata = optRdbTableMetadata.get();
    try {
      // Check the primary keys.
      if (rdbTableMetadata.primaryKeyColumns.size()
          != metadata.getPartitionKeyNames().size() + metadata.getClusteringKeyNames().size()) {
        throw new IllegalStateException(
            String.format(
                "The size of the primary keys are different between the ScalarDB metadata (%s) and the RDB table schema (%s)",
                metadata.getPartitionKeyNames().size() + metadata.getClusteringKeyNames().size(),
                rdbTableMetadata.primaryKeyColumns.size()));
      }

      // Compare the primary keys with the partition keys of the ScalarDB metadata.
      int indexOfPrimaryKeyColumns = 0;
      for (String keyNameInMetadata : metadata.getPartitionKeyNames()) {
        PrimaryKeyColumn primaryKeyColumn =
            rdbTableMetadata.primaryKeyColumns.get(indexOfPrimaryKeyColumns);

        if (!primaryKeyColumn.name.equals(keyNameInMetadata)) {
          throw new IllegalStateException(
              String.format(
                  "The primary key name is different between the ScalarDB metadata (%s) and the RDB table schema (%s)",
                  keyNameInMetadata, primaryKeyColumn.name));
        }

        indexOfPrimaryKeyColumns++;
      }

      // Compare the primary keys with the clustering keys of the ScalarDB metadata.
      for (String keyNameInMetadata : metadata.getClusteringKeyNames()) {
        PrimaryKeyColumn primaryKeyColumn =
            rdbTableMetadata.primaryKeyColumns.get(indexOfPrimaryKeyColumns);
        Order clusteringOrderInMetadata = metadata.getClusteringOrder(keyNameInMetadata);

        if (!primaryKeyColumn.name.equals(keyNameInMetadata)) {
          throw new IllegalStateException(
              String.format(
                  "The primary key name is different between the ScalarDB metadata (%s) and the RDB table schema (%s)",
                  keyNameInMetadata, primaryKeyColumn.name));
        }

        if ((primaryKeyColumn.sortOrder == SortOrder.ASC && clusteringOrderInMetadata != Order.ASC)
            || (primaryKeyColumn.sortOrder == SortOrder.DESC
                && clusteringOrderInMetadata != Order.DESC)) {
          throw new IllegalStateException(
              String.format(
                  "The primary key sort order is different between the ScalarDB metadata (%s:%s) and the RDB table schema (%s:%s)",
                  keyNameInMetadata,
                  clusteringOrderInMetadata,
                  primaryKeyColumn.name,
                  primaryKeyColumn.sortOrder));
        }

        indexOfPrimaryKeyColumns++;
      }

      // Check the normal index.
      if (rdbTableMetadata.indexColumns != null) {
        // It's possible that users have manually added an index on a scanned column as a
        // performance tuning. That's why this checks if the RDB engine table indexes includes the
        // indexes of ScalarDB metadata.
        if (!rdbTableMetadata.indexColumns.stream()
            .map(c -> c.name)
            .collect(Collectors.toSet())
            .containsAll(metadata.getSecondaryIndexNames())) {
          throw new IllegalStateException("There are missing index keys in the RDB table schema");
        }

        Set<IndexColumn> indexColumnsWithDescOrder =
            rdbTableMetadata.indexColumns.stream()
                .filter(c -> c.sortOrder == SortOrder.DESC)
                .collect(Collectors.toSet());
        if (!indexColumnsWithDescOrder.isEmpty()) {
          throw new IllegalStateException(
              String.format(
                  "There are missing index keys in the RDB table schema. Index columns: %s",
                  indexColumnsWithDescOrder));
        }
      }

      // Check the columns.
      for (String columnName : metadata.getColumnNames()) {
        if (!rdbTableMetadata.columns.containsKey(columnName)) {
          throw new IllegalStateException(
              String.format("Column '%s' doesn't exist in the RDB table schema", columnName));
        }

        DataType columnDataType = metadata.getColumnDataType(columnName);
        DataType columnDataTypeOfRawTable = rdbTableMetadata.columns.get(columnName);
        if (columnDataType == DataType.FLOAT || columnDataType == DataType.DOUBLE) {
          // Some RDBMS internally use the same data type for ScalarDB FLOAT and DOUBLE.
          // If either data type is returned from the underlying RDBMS, ScalarDB can't distinguish
          // which ScalarDB data type was actually specified in the table schema. Therefore, we
          // treat FLOAT and DOUBLE as the same group.
          if (columnDataTypeOfRawTable == DataType.FLOAT
              || columnDataTypeOfRawTable == DataType.DOUBLE) {
            continue;
          }
        } else if (columnDataType == DataType.BOOLEAN
            || columnDataType == DataType.INT
            || columnDataType == DataType.BIGINT) {
          // Handle BOOLEAN, INT and BIGINT similarly to FLOAT and DOUBLE. Regarding BOOLEAN,
          // some RDBMS use a numeric type as BOOLEAN. Therefore, we leniently treat BOOLEAN as the
          // same group here.
          if (columnDataTypeOfRawTable == DataType.BOOLEAN
              || columnDataTypeOfRawTable == DataType.INT
              || columnDataTypeOfRawTable == DataType.BIGINT) {
            continue;
          }
        } else if (columnDataType.equals(columnDataTypeOfRawTable)) {
          continue;
        }

        throw new IllegalStateException(
            String.format("The data type of column '%s' are different", columnName));
      }
    } catch (IllegalStateException e) {
      throw new IllegalStateException(
          String.format(
              "Failed to repair table since the RDB table with inconsistent schema exists. Namespace:%s, Table:%s, ScalarDB metadata:%s, Raw table schema:%s, Details:%s",
              namespace, table, metadata, rdbTableMetadata, e.getMessage()),
          e);
    }
  }

  @Override
  public void repairTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {

    checkRawTableSchemaForRepairTable(namespace, table, metadata);

    try (Connection connection = dataSource.getConnection()) {
      createTableInternal(connection, namespace, table, metadata, true);
      addTableMetadata(connection, namespace, table, metadata, true, true);
    } catch (SQLException e) {
      throw new ExecutionException(
          "Repairing the " + getFullTableName(namespace, table) + " table failed ", e);
    }
  }

  @Override
  public void addNewColumnToTable(
      String namespace, String table, String columnName, DataType columnType)
      throws ExecutionException {
    try {
      TableMetadata currentTableMetadata = getTableMetadata(namespace, table);
      TableMetadata updatedTableMetadata =
          TableMetadata.newBuilder(currentTableMetadata).addColumn(columnName, columnType).build();
      String addNewColumnStatement =
          "ALTER TABLE "
              + encloseFullTableName(namespace, table)
              + " ADD "
              + enclose(columnName)
              + " "
              + getVendorDbColumnType(updatedTableMetadata, columnName);
      try (Connection connection = dataSource.getConnection()) {
        execute(connection, addNewColumnStatement);
        addTableMetadata(connection, namespace, table, updatedTableMetadata, false, true);
      }
    } catch (SQLException e) {
      throw new ExecutionException(
          String.format(
              "Adding the new %s column to the %s table failed",
              columnName, getFullTableName(namespace, table)),
          e);
    }
  }

  @Override
  public void addRawColumnToTable(
      String namespace, String table, String columnName, DataType columnType)
      throws ExecutionException {
    try (Connection connection = dataSource.getConnection()) {
      if (!tableExistsInternal(connection, namespace, table)) {
        throw new IllegalArgumentException(
            CoreError.TABLE_NOT_FOUND.buildMessage(getFullTableName(namespace, table)));
      }

      String addNewColumnStatement =
          "ALTER TABLE "
              + encloseFullTableName(namespace, table)
              + " ADD "
              + enclose(columnName)
              + " "
              + rdbEngine.getDataTypeForEngine(columnType);

      execute(connection, addNewColumnStatement);
    } catch (SQLException e) {
      throw new ExecutionException(
          String.format(
              "Adding the new %s column to the %s table failed",
              columnName, getFullTableName(namespace, table)),
          e);
    }
  }

  private void createIndex(
      Connection connection, String schema, String table, String indexedColumn, boolean ifNotExists)
      throws SQLException {
    String indexName = getIndexName(schema, table, indexedColumn);
    String createIndexStatement =
        "CREATE INDEX "
            + enclose(indexName)
            + " ON "
            + encloseFullTableName(schema, table)
            + " ("
            + enclose(indexedColumn)
            + ")";
    if (ifNotExists) {
      createIndexStatement = rdbEngine.tryAddIfNotExistsToCreateIndexSql(createIndexStatement);
    }
    try {
      execute(connection, createIndexStatement);
    } catch (SQLException e) {
      // Suppress the exception thrown when the index already exists
      if (!(ifNotExists && rdbEngine.isDuplicateIndexError(e))) {
        throw e;
      }
    }
  }

  private void dropIndex(Connection connection, String schema, String table, String indexedColumn)
      throws SQLException {
    String indexName = getIndexName(schema, table, indexedColumn);
    String sql = rdbEngine.dropIndexSql(schema, table, indexName);
    execute(connection, sql);
  }

  private String getIndexName(String schema, String table, String indexedColumn) {
    return String.join("_", INDEX_NAME_PREFIX, schema, table, indexedColumn);
  }

  private void updateTableMetadata(
      Connection connection, String schema, String table, String columnName, boolean indexed)
      throws SQLException {
    String updateStatement =
        "UPDATE "
            + encloseFullTableName(metadataSchema, METADATA_TABLE)
            + " SET "
            + enclose(METADATA_COL_INDEXED)
            + "="
            + computeBooleanValue(indexed)
            + " WHERE "
            + enclose(METADATA_COL_FULL_TABLE_NAME)
            + "='"
            + getFullTableName(schema, table)
            + "' AND "
            + enclose(METADATA_COL_COLUMN_NAME)
            + "='"
            + columnName
            + "'";
    execute(connection, updateStatement);
  }

  static void execute(Connection connection, String sql) throws SQLException {
    if (Strings.isNullOrEmpty(sql)) {
      return;
    }
    try (Statement stmt = connection.createStatement()) {
      stmt.execute(sql);
    }
  }

  static void execute(Connection connection, String[] sqls) throws SQLException {
    for (String sql : sqls) {
      execute(connection, sql);
    }
  }

  private String enclose(String name) {
    return rdbEngine.enclose(name);
  }

  private String encloseFullTableName(String schema, String table) {
    return rdbEngine.encloseFullTableName(schema, table);
  }

  @Override
  public Set<String> getNamespaceNames() throws ExecutionException {
    try (Connection connection = dataSource.getConnection()) {
      String selectQuery =
          "SELECT * FROM " + encloseFullTableName(metadataSchema, NAMESPACES_TABLE);
      Set<String> namespaces = new HashSet<>();
      try (PreparedStatement preparedStatement = connection.prepareStatement(selectQuery);
          ResultSet resultSet = preparedStatement.executeQuery()) {
        while (resultSet.next()) {
          namespaces.add(resultSet.getString(NAMESPACE_COL_NAMESPACE_NAME));
        }
        return namespaces;
      }
    } catch (SQLException e) {
      // An exception will be thrown if the namespace table does not exist when executing the select
      // query
      if (rdbEngine.isUndefinedTableError(e)) {
        return Collections.emptySet();
      }
      throw new ExecutionException("Getting the existing schema names failed", e);
    }
  }

  @VisibleForTesting
  void createNamespacesTableIfNotExists(Connection connection) throws ExecutionException {
    if (tableExistsInternal(connection, metadataSchema, NAMESPACES_TABLE)) {
      return;
    }

    try {
      createSchemaIfNotExists(connection, metadataSchema);
      String createTableStatement =
          "CREATE TABLE "
              + encloseFullTableName(metadataSchema, NAMESPACES_TABLE)
              + "("
              + enclose(NAMESPACE_COL_NAMESPACE_NAME)
              + " "
              + getTextType(128)
              + ", "
              + "PRIMARY KEY ("
              + enclose(NAMESPACE_COL_NAMESPACE_NAME)
              + "))";
      createTable(connection, createTableStatement, true);

      // Insert the system namespace to the namespaces table
      insertIntoNamespacesTable(connection, metadataSchema);
    } catch (SQLException e) {
      throw new ExecutionException("Creating the namespace table failed", e);
    }
  }

  private void insertIntoNamespacesTable(Connection connection, String namespaceName)
      throws SQLException {
    String insertStatement =
        "INSERT INTO " + encloseFullTableName(metadataSchema, NAMESPACES_TABLE) + " VALUES (?)";
    try (PreparedStatement preparedStatement = connection.prepareStatement(insertStatement)) {
      preparedStatement.setString(1, namespaceName);
      preparedStatement.execute();
    }
  }

  @VisibleForTesting
  void upsertIntoNamespacesTable(Connection connection, String namespace) throws SQLException {
    try {
      insertIntoNamespacesTable(connection, namespace);
    } catch (SQLException e) {
      // ignore if the schema already exists
      if (!rdbEngine.isDuplicateKeyError(e)) {
        throw e;
      }
    }
  }

  private void deleteFromNamespacesTable(Connection connection, String namespaceName)
      throws SQLException {
    String deleteStatement =
        "DELETE FROM "
            + encloseFullTableName(metadataSchema, NAMESPACES_TABLE)
            + " WHERE "
            + enclose(NAMESPACE_COL_NAMESPACE_NAME)
            + " = ?";
    try (PreparedStatement preparedStatement = connection.prepareStatement(deleteStatement)) {
      preparedStatement.setString(1, namespaceName);
      preparedStatement.execute();
    }
  }

  private void deleteNamespacesTableAndMetadataSchemaIfEmpty(Connection connection)
      throws SQLException {
    if (areNamespacesTableAndMetadataSchemaEmpty(connection)) {
      deleteTable(connection, encloseFullTableName(metadataSchema, NAMESPACES_TABLE));
      deleteMetadataSchema(connection);
    }
  }

  private boolean areNamespacesTableAndMetadataSchemaEmpty(Connection connection)
      throws SQLException {
    String selectAllTables =
        "SELECT * FROM " + encloseFullTableName(metadataSchema, NAMESPACES_TABLE);

    Set<String> namespaces = new HashSet<>();
    try (Statement statement = connection.createStatement();
        ResultSet results = statement.executeQuery(selectAllTables)) {
      int count = 0;
      while (results.next()) {
        namespaces.add(results.getString(NAMESPACE_COL_NAMESPACE_NAME));
        // Only need to fetch the first two rows
        if (count++ == 2) {
          break;
        }
      }
    }

    boolean onlyMetadataNamespaceLeft =
        namespaces.size() == 1 && namespaces.contains(metadataSchema);
    if (!onlyMetadataNamespaceLeft) {
      return false;
    }

    // Check if the metadata table exists. If it does not, the metadata schema is empty.
    String sql =
        rdbEngine.tableExistsInternalTableCheckSql(
            encloseFullTableName(metadataSchema, METADATA_TABLE));
    try {
      execute(connection, sql);
      return false;
    } catch (SQLException e) {
      // An exception will be thrown if the table does not exist when executing the select
      // query
      if (rdbEngine.isUndefinedTableError(e)) {
        return true;
      }
      throw e;
    }
  }

  @Override
  public void upgrade(Map<String, String> options) throws ExecutionException {
    try (Connection connection = dataSource.getConnection()) {
      if (tableExistsInternal(connection, metadataSchema, METADATA_TABLE)) {
        createNamespacesTableIfNotExists(connection);
        importNamespaceNamesOfExistingTables(connection);
      }
    } catch (SQLException e) {
      throw new ExecutionException("Upgrading the ScalarDB environment failed", e);
    }
  }

  private void importNamespaceNamesOfExistingTables(Connection connection)
      throws ExecutionException {
    String selectAllTableNames =
        "SELECT DISTINCT "
            + enclose(METADATA_COL_FULL_TABLE_NAME)
            + " FROM "
            + encloseFullTableName(metadataSchema, METADATA_TABLE);
    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(selectAllTableNames)) {
      Set<String> namespaceOfExistingTables = new HashSet<>();
      while (rs.next()) {
        String fullTableName = rs.getString(METADATA_COL_FULL_TABLE_NAME);
        String namespaceName = fullTableName.substring(0, fullTableName.indexOf('.'));
        namespaceOfExistingTables.add(namespaceName);
      }
      for (String namespace : namespaceOfExistingTables) {
        upsertIntoNamespacesTable(connection, namespace);
      }
    } catch (SQLException e) {
      throw new ExecutionException("Importing the namespace names of existing tables failed", e);
    }
  }
}
