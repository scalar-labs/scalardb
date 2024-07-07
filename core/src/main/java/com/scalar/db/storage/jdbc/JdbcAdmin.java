package com.scalar.db.storage.jdbc;

import static com.scalar.db.storage.jdbc.JdbcUtils.getJdbcType;
import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
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
    metadataSchema =
        config.getTableMetadataSchema().orElse(DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME);
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public JdbcAdmin(BasicDataSource dataSource, JdbcConfig config) {
    rdbEngine = RdbEngineFactory.create(config);
    this.dataSource = dataSource;
    metadataSchema =
        config.getTableMetadataSchema().orElse(DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME);
  }

  @Override
  public void createNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    String fullNamespace = enclose(namespace);
    try (Connection connection = dataSource.getConnection()) {
      execute(connection, rdbEngine.createNamespaceSqls(fullNamespace));
    } catch (SQLException e) {
      throw new ExecutionException("Creating the schema failed", e);
    }
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    if (!rdbEngine.isValidTableName(table)) {
      throw new ExecutionException("Table name is not acceptable: " + table);
    }

    try (Connection connection = dataSource.getConnection()) {
      // Create the metadata schema and table first if they do not exist
      createMetadataSchemaAndTableIfNotExists(connection);

      createTableInternal(connection, namespace, table, metadata);
      createIndex(connection, namespace, table, metadata);
      addTableMetadata(connection, namespace, table, metadata, false);
    } catch (SQLException e) {
      throw new ExecutionException(
          "Creating the table failed: " + getFullTableName(namespace, table), e);
    }
  }

  private void createTableInternal(
      Connection connection, String schema, String table, TableMetadata metadata)
      throws SQLException {
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
    boolean hasDescClusteringOrder = hasDescClusteringOrder(metadata);
    createTableStatement +=
        ", " + rdbEngine.createTableInternalPrimaryKeyClause(hasDescClusteringOrder, metadata);
    execute(connection, createTableStatement);

    String[] sqls =
        rdbEngine.createTableInternalSqlsAfterCreateTable(
            hasDifferentClusteringOrders(metadata), schema, table, metadata);
    execute(connection, sqls);
  }

  private void createIndex(
      Connection connection, String schema, String table, TableMetadata metadata)
      throws SQLException {
    for (String indexedColumn : metadata.getSecondaryIndexNames()) {
      createIndex(connection, schema, table, indexedColumn);
    }
  }

  private void addTableMetadata(
      Connection connection,
      String namespace,
      String table,
      TableMetadata metadata,
      boolean createMetadataTableIfNotExists)
      throws SQLException {
    if (createMetadataTableIfNotExists) {
      createMetadataSchemaAndTableIfNotExists(connection);
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
    createMetadataSchemaIfNotExists(connection);
    createMetadataTableIfNotExists(connection);
  }

  private void createMetadataSchemaIfNotExists(Connection connection) throws SQLException {
    String[] sqls = rdbEngine.createMetadataSchemaIfNotExistsSql(metadataSchema);
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

    String stmt = rdbEngine.tryAddIfNotExistsToCreateTableSql(createTableStatement);
    try {
      execute(connection, stmt);
    } catch (SQLException e) {
      // Suppress the exception thrown when the table already exists
      if (!rdbEngine.isDuplicateTableError(e)) {
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
    } catch (SQLException e) {
      throw new ExecutionException(
          "Dropping the table failed: " + getFullTableName(namespace, table), e);
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
      deleteMetadataSchemaAndTableIfEmpty(connection);
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

  private void deleteMetadataSchemaAndTableIfEmpty(Connection connection) throws SQLException {
    if (isMetadataTableEmpty(connection)) {
      deleteMetadataTable(connection);
      deleteMetadataSchema(connection);
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

  private void deleteMetadataTable(Connection connection) throws SQLException {
    String dropTableStatement =
        "DROP TABLE " + encloseFullTableName(metadataSchema, METADATA_TABLE);

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
          "Truncating the table failed: " + getFullTableName(namespace, table), e);
    }
  }

  @Override
  public TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException {
    TableMetadata.Builder builder = TableMetadata.newBuilder();
    boolean tableExists = false;

    try (Connection connection = dataSource.getConnection()) {
      if (!namespaceExistsInternal(connection, metadataSchema)) {
        return null;
      }

      try (PreparedStatement preparedStatement =
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
      }
    } catch (SQLException e) {
      throw new ExecutionException("Getting a table metadata failed", e);
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
      throw new ExecutionException("Getting a table metadata failed", e);
    }

    return builder.build();
  }

  @Override
  public void importTable(String namespace, String table, Map<String, String> options)
      throws ExecutionException {
    TableMetadata tableMetadata = getImportTableMetadata(namespace, table);

    // add ScalarDB metadata
    repairTable(namespace, table, tableMetadata, options);
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
      throw new ExecutionException("Retrieving the namespace table names failed", e);
    }
  }

  @Override
  public boolean namespaceExists(String namespace) throws ExecutionException {
    if (metadataSchema.equals(namespace)) {
      return true;
    }

    try (Connection connection = dataSource.getConnection()) {
      return namespaceExistsInternal(connection, namespace);
    } catch (SQLException e) {
      throw new ExecutionException("Checking if the namespace exists failed", e);
    }
  }

  private boolean namespaceExistsInternal(Connection connection, String namespace)
      throws SQLException {
    String namespaceExistsStatement = rdbEngine.namespaceExistsStatement();
    try (PreparedStatement preparedStatement =
        connection.prepareStatement(namespaceExistsStatement)) {
      preparedStatement.setString(1, rdbEngine.namespaceExistsPlaceholder(namespace));
      return preparedStatement.executeQuery().next();
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
      createIndex(connection, namespace, table, columnName);
      updateTableMetadata(connection, namespace, table, columnName, true);
    } catch (ExecutionException | SQLException e) {
      throw new ExecutionException("Creating the secondary index failed", e);
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
      throw new ExecutionException("Dropping the secondary index failed", e);
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
  public void repairTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    try (Connection connection = dataSource.getConnection()) {
      if (!tableExistsInternal(connection, namespace, table)) {
        throw new IllegalArgumentException(
            CoreError.TABLE_NOT_FOUND.buildMessage(getFullTableName(namespace, table)));
      }

      if (tableExistsInternal(connection, metadataSchema, METADATA_TABLE)) {
        // Delete then add the metadata for the table
        execute(connection, getDeleteTableMetadataStatement(namespace, table));
        addTableMetadata(connection, namespace, table, metadata, false);
      } else {
        // Create the metadata table then add the metadata for the table
        addTableMetadata(connection, namespace, table, metadata, true);
      }
    } catch (ExecutionException | SQLException e) {
      throw new ExecutionException(
          String.format("Repairing the table %s.%s failed", namespace, table), e);
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
        execute(connection, getDeleteTableMetadataStatement(namespace, table));
        addTableMetadata(connection, namespace, table, updatedTableMetadata, false);
      }
    } catch (SQLException e) {
      throw new ExecutionException(
          String.format(
              "Adding the new column %s to the %s.%s table failed", columnName, namespace, table),
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
              "Adding the new column %s to the %s.%s table failed", columnName, namespace, table),
          e);
    }
  }

  @Override
  public Set<String> getNamespaceNames() throws ExecutionException {
    String selectAllTableNames =
        "SELECT DISTINCT "
            + enclose(METADATA_COL_FULL_TABLE_NAME)
            + " FROM "
            + encloseFullTableName(metadataSchema, METADATA_TABLE);
    try (Connection connection = dataSource.getConnection();
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(selectAllTableNames)) {
      Set<String> namespaceOfExistingTables = new HashSet<>();
      while (rs.next()) {
        String fullTableName = rs.getString(METADATA_COL_FULL_TABLE_NAME);
        String namespaceName = fullTableName.substring(0, fullTableName.indexOf('.'));
        namespaceOfExistingTables.add(namespaceName);
      }
      namespaceOfExistingTables.add(metadataSchema);

      return namespaceOfExistingTables;
    } catch (SQLException e) {
      // An exception will be thrown if the namespace table does not exist when executing the select
      // query
      if (rdbEngine.isUndefinedTableError(e)) {
        return Collections.singleton(metadataSchema);
      }
      throw new ExecutionException("Getting the existing schema names failed", e);
    }
  }

  private void createIndex(Connection connection, String schema, String table, String indexedColumn)
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
    execute(connection, createIndexStatement);
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
}
