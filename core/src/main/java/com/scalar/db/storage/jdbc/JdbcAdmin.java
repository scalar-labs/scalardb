package com.scalar.db.storage.jdbc;

import static com.scalar.db.storage.jdbc.JdbcUtils.getJdbcType;
import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.StorageInfo;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.CoreError;
import com.scalar.db.common.StorageInfoImpl;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
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

@SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
@ThreadSafe
public class JdbcAdmin implements DistributedStorageAdmin {
  private static final Logger logger = LoggerFactory.getLogger(JdbcAdmin.class);

  @VisibleForTesting static final String JDBC_COL_COLUMN_NAME = "COLUMN_NAME";
  @VisibleForTesting static final String JDBC_COL_DATA_TYPE = "DATA_TYPE";
  @VisibleForTesting static final String JDBC_COL_TYPE_NAME = "TYPE_NAME";
  @VisibleForTesting static final String JDBC_COL_COLUMN_SIZE = "COLUMN_SIZE";
  @VisibleForTesting static final String JDBC_COL_DECIMAL_DIGITS = "DECIMAL_DIGITS";

  private static final String INDEX_NAME_PREFIX = "index";
  private static final StorageInfo STORAGE_INFO =
      new StorageInfoImpl(
          "jdbc",
          StorageInfo.MutationAtomicityUnit.STORAGE,
          // No limit on the number of mutations
          Integer.MAX_VALUE);

  private final RdbEngineStrategy rdbEngine;
  private final BasicDataSource dataSource;
  private final String metadataSchema;
  private final TableMetadataService tableMetadataService;

  @Inject
  public JdbcAdmin(DatabaseConfig databaseConfig) {
    JdbcConfig config = new JdbcConfig(databaseConfig);
    rdbEngine = RdbEngineFactory.create(config);
    dataSource = JdbcUtils.initDataSourceForAdmin(config, rdbEngine);
    metadataSchema =
        config.getTableMetadataSchema().orElse(DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME);
    tableMetadataService = new TableMetadataService(metadataSchema, rdbEngine);
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public JdbcAdmin(BasicDataSource dataSource, JdbcConfig config) {
    rdbEngine = RdbEngineFactory.create(config);
    this.dataSource = dataSource;
    metadataSchema =
        config.getTableMetadataSchema().orElse(DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME);
    tableMetadataService = new TableMetadataService(metadataSchema, rdbEngine);
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
  public void createNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    rdbEngine.throwIfInvalidNamespaceName(namespace);

    try (Connection connection = dataSource.getConnection()) {
      execute(connection, rdbEngine.createSchemaSqls(namespace));
    } catch (SQLException e) {
      throw new ExecutionException("Creating the " + namespace + " schema failed", e);
    }
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    try (Connection connection = dataSource.getConnection()) {
      createMetadataSchemaIfNotExists(connection);
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
    rdbEngine.throwIfInvalidTableName(table);

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
      execute(connection, stmts, ifNotExists ? null : rdbEngine::throwIfDuplicatedIndexWarning);
    } catch (SQLException e) {
      if (!(ifNotExists && rdbEngine.isDuplicateIndexError(e))) {
        throw e;
      }
    }
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

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    try (Connection connection = dataSource.getConnection()) {
      dropTableInternal(connection, namespace, table);
      tableMetadataService.deleteTableMetadata(connection, namespace, table, true);
      deleteMetadataSchemaIfEmpty(connection);
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

  @Override
  public void dropNamespace(String namespace) throws ExecutionException {
    try (Connection connection = dataSource.getConnection()) {
      Set<String> remainingTables = getInternalTableNames(connection, namespace);
      if (!remainingTables.isEmpty()) {
        throw new IllegalArgumentException(
            CoreError.NAMESPACE_WITH_NON_SCALARDB_TABLES_CANNOT_BE_DROPPED.buildMessage(
                namespace, remainingTables));
      }
      execute(connection, rdbEngine.dropNamespaceSql(namespace));
    } catch (SQLException e) {
      rdbEngine.dropNamespaceTranslateSQLException(e, namespace);
    }
  }

  private Set<String> getInternalTableNames(Connection connection, String namespace)
      throws SQLException {
    String sql = rdbEngine.getTableNamesInNamespaceSql();
    if (Strings.isNullOrEmpty(sql)) {
      return Collections.emptySet();
    }
    Set<String> tableNames = new HashSet<>();
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, namespace);
      try (ResultSet resultSet = statement.executeQuery()) {
        while (resultSet.next()) {
          tableNames.add(resultSet.getString(1));
        }
      }
    }
    return tableNames;
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
    try (Connection connection = dataSource.getConnection()) {
      rdbEngine.setConnectionToReadOnly(connection, true);
      return tableMetadataService.getTableMetadata(connection, namespace, table);
    } catch (SQLException e) {
      throw new ExecutionException(
          "Getting a table metadata for the "
              + getFullTableName(namespace, table)
              + " table failed",
          e);
    }
  }

  @VisibleForTesting
  TableMetadata getImportTableMetadata(
      String namespace, String table, Map<String, DataType> overrideColumnsType)
      throws ExecutionException {
    TableMetadata.Builder builder = TableMetadata.newBuilder();
    boolean primaryKeyExists = false;

    try (Connection connection = dataSource.getConnection()) {
      rdbEngine.setConnectionToReadOnly(connection, true);

      String catalogName = rdbEngine.getCatalogName(namespace);
      String schemaName = rdbEngine.getSchemaName(namespace);

      if (!internalTableExists(connection, namespace, table)) {
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
        DataType overrideDataType = overrideColumnsType.get(columnName);
        DataType dataType =
            rdbEngine.getDataTypeForScalarDb(
                getJdbcType(resultSet.getInt(JDBC_COL_DATA_TYPE)),
                resultSet.getString(JDBC_COL_TYPE_NAME),
                resultSet.getInt(JDBC_COL_COLUMN_SIZE),
                resultSet.getInt(JDBC_COL_DECIMAL_DIGITS),
                getFullTableName(namespace, table) + " " + columnName,
                overrideDataType);
        builder.addColumn(columnName, dataType);
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
  public void importTable(
      String namespace,
      String table,
      Map<String, String> options,
      Map<String, DataType> overrideColumnsType)
      throws ExecutionException {
    rdbEngine.throwIfImportNotSupported();

    try (Connection connection = dataSource.getConnection()) {
      TableMetadata tableMetadata = getImportTableMetadata(namespace, table, overrideColumnsType);
      createMetadataSchemaIfNotExists(connection);
      addTableMetadata(connection, namespace, table, tableMetadata, true, false);
    } catch (SQLException | ExecutionException e) {
      throw new ExecutionException(
          String.format("Importing the %s table failed", getFullTableName(namespace, table)), e);
    }
  }

  @Override
  public Set<String> getNamespaceTableNames(String namespace) throws ExecutionException {
    try (Connection connection = dataSource.getConnection()) {
      rdbEngine.setConnectionToReadOnly(connection, true);
      return tableMetadataService.getNamespaceTableNames(connection, namespace);
    } catch (SQLException e) {
      throw new ExecutionException(
          "Getting the list of tables of the " + namespace + " schema failed", e);
    }
  }

  @Override
  public boolean namespaceExists(String namespace) throws ExecutionException {
    if (metadataSchema.equals(namespace)) {
      return true;
    }

    try (Connection connection = dataSource.getConnection()) {
      rdbEngine.setConnectionToReadOnly(connection, true);

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
    DataType scalarDbColumnType = metadata.getColumnDataType(columnName);

    String dataType = rdbEngine.getDataTypeForEngine(scalarDbColumnType);
    if (metadata.getPartitionKeyNames().contains(columnName)
        || metadata.getClusteringKeyNames().contains(columnName)) {
      String keyDataType = rdbEngine.getDataTypeForKey(scalarDbColumnType);
      return Optional.ofNullable(keyDataType).orElse(dataType);
    } else if (metadata.getSecondaryIndexNames().contains(columnName)) {
      String indexDataType = rdbEngine.getDataTypeForSecondaryIndex(scalarDbColumnType);
      return Optional.ofNullable(indexDataType).orElse(dataType);
    } else {
      return dataType;
    }
  }

  @Override
  public void createIndex(
      String namespace, String table, String columnName, Map<String, String> options)
      throws ExecutionException {
    try (Connection connection = dataSource.getConnection()) {
      alterToIndexColumnTypeIfNecessary(connection, namespace, table, columnName);
      createIndex(connection, namespace, table, columnName, false);
      tableMetadataService.updateTableMetadata(connection, namespace, table, columnName, true);
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
    String columnTypeForKey = rdbEngine.getDataTypeForSecondaryIndex(indexType);
    if (columnTypeForKey == null) {
      // The column type does not need to be altered to be compatible with being secondary index
      return;
    }

    String[] sqls = rdbEngine.alterColumnTypeSql(namespace, table, columnName, columnTypeForKey);
    for (String sql : sqls) {
      execute(connection, sql);
    }
  }

  private void alterToRegularColumnTypeIfNecessary(
      Connection connection, String namespace, String table, String columnName)
      throws ExecutionException, SQLException {
    DataType indexType = getTableMetadata(namespace, table).getColumnDataType(columnName);
    String columnTypeForKey = rdbEngine.getDataTypeForSecondaryIndex(indexType);
    if (columnTypeForKey == null) {
      // The column type is already the type for a regular column. It was not altered to be
      // compatible with being a secondary index, so no alteration is necessary.
      return;
    }

    String columnType = rdbEngine.getDataTypeForEngine(indexType);
    String[] sqls = rdbEngine.alterColumnTypeSql(namespace, table, columnName, columnType);
    for (String sql : sqls) {
      execute(connection, sql);
    }
  }

  @Override
  public void dropIndex(String namespace, String table, String columnName)
      throws ExecutionException {
    try (Connection connection = dataSource.getConnection()) {
      dropIndex(connection, namespace, table, columnName);
      alterToRegularColumnTypeIfNecessary(connection, namespace, table, columnName);
      tableMetadataService.updateTableMetadata(connection, namespace, table, columnName, false);
    } catch (SQLException e) {
      throw new ExecutionException(
          String.format(
              "Dropping the secondary index on the %s column for the %s table failed",
              columnName, getFullTableName(namespace, table)),
          e);
    }
  }

  @Override
  public void repairTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    rdbEngine.throwIfInvalidNamespaceName(table);

    try (Connection connection = dataSource.getConnection()) {
      if (!internalTableExists(connection, namespace, table)) {
        throw new IllegalArgumentException(
            CoreError.TABLE_NOT_FOUND.buildMessage(getFullTableName(namespace, table)));
      }

      createMetadataSchemaIfNotExists(connection);
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
  public void dropColumnFromTable(String namespace, String table, String columnName)
      throws ExecutionException {
    try {
      TableMetadata currentTableMetadata = getTableMetadata(namespace, table);
      TableMetadata updatedTableMetadata =
          TableMetadata.newBuilder(currentTableMetadata).removeColumn(columnName).build();
      String[] dropColumnStatements = rdbEngine.dropColumnSql(namespace, table, columnName);
      try (Connection connection = dataSource.getConnection()) {
        for (String dropColumnStatement : dropColumnStatements) {
          execute(connection, dropColumnStatement);
        }
        addTableMetadata(connection, namespace, table, updatedTableMetadata, false, true);
      }
    } catch (SQLException e) {
      throw new ExecutionException(
          String.format(
              "Dropping the %s column from the %s table failed",
              columnName, getFullTableName(namespace, table)),
          e);
    }
  }

  @Override
  public void renameColumn(
      String namespace, String table, String oldColumnName, String newColumnName)
      throws ExecutionException {
    try {
      TableMetadata currentTableMetadata = getTableMetadata(namespace, table);
      assert currentTableMetadata != null;
      rdbEngine.throwIfRenameColumnNotSupported(oldColumnName, currentTableMetadata);
      TableMetadata.Builder tableMetadataBuilder =
          TableMetadata.newBuilder(currentTableMetadata).renameColumn(oldColumnName, newColumnName);
      if (currentTableMetadata.getPartitionKeyNames().contains(oldColumnName)) {
        tableMetadataBuilder.renamePartitionKey(oldColumnName, newColumnName);
      }
      if (currentTableMetadata.getClusteringKeyNames().contains(oldColumnName)) {
        tableMetadataBuilder.renameClusteringKey(oldColumnName, newColumnName);
      }
      if (currentTableMetadata.getSecondaryIndexNames().contains(oldColumnName)) {
        tableMetadataBuilder.renameSecondaryIndex(oldColumnName, newColumnName);
      }
      TableMetadata updatedTableMetadata = tableMetadataBuilder.build();
      String renameColumnStatement =
          rdbEngine.renameColumnSql(
              namespace,
              table,
              oldColumnName,
              newColumnName,
              getVendorDbColumnType(updatedTableMetadata, newColumnName));
      try (Connection connection = dataSource.getConnection()) {
        execute(connection, renameColumnStatement);
        if (currentTableMetadata.getSecondaryIndexNames().contains(oldColumnName)) {
          String oldIndexName = getIndexName(namespace, table, oldColumnName);
          String newIndexName = getIndexName(namespace, table, newColumnName);
          renameIndexInternal(
              connection, namespace, table, newColumnName, oldIndexName, newIndexName);
        }
        addTableMetadata(connection, namespace, table, updatedTableMetadata, false, true);
      }
    } catch (SQLException e) {
      throw new ExecutionException(
          String.format(
              "Renaming the %s column to %s in the %s table failed",
              oldColumnName, newColumnName, getFullTableName(namespace, table)),
          e);
    }
  }

  @Override
  public void alterColumnType(
      String namespace, String table, String columnName, DataType newColumnType)
      throws ExecutionException {
    try {
      TableMetadata currentTableMetadata = getTableMetadata(namespace, table);
      assert currentTableMetadata != null;
      DataType currentColumnType = currentTableMetadata.getColumnDataType(columnName);
      rdbEngine.throwIfAlterColumnTypeNotSupported(currentColumnType, newColumnType);

      TableMetadata updatedTableMetadata =
          TableMetadata.newBuilder(currentTableMetadata)
              .removeColumn(columnName)
              .addColumn(columnName, newColumnType)
              .build();
      String newStorageColumnType = getVendorDbColumnType(updatedTableMetadata, columnName);
      String[] alterColumnTypeStatements =
          rdbEngine.alterColumnTypeSql(namespace, table, columnName, newStorageColumnType);

      try (Connection connection = dataSource.getConnection()) {
        for (String alterColumnTypeStatement : alterColumnTypeStatements) {
          execute(connection, alterColumnTypeStatement);
        }
        addTableMetadata(connection, namespace, table, updatedTableMetadata, false, true);
      }
    } catch (SQLException e) {
      throw new ExecutionException(
          String.format(
              "Altering the %s column type to %s in the %s table failed",
              columnName, newColumnType, getFullTableName(namespace, table)),
          e);
    }
  }

  @Override
  public void renameTable(String namespace, String oldTableName, String newTableName)
      throws ExecutionException {
    rdbEngine.throwIfInvalidTableName(newTableName);

    try {
      TableMetadata tableMetadata = getTableMetadata(namespace, oldTableName);
      assert tableMetadata != null;
      String renameTableStatement = rdbEngine.renameTableSql(namespace, oldTableName, newTableName);
      try (Connection connection = dataSource.getConnection()) {
        execute(connection, renameTableStatement);
        tableMetadataService.deleteTableMetadata(connection, namespace, oldTableName, false);
        for (String indexedColumnName : tableMetadata.getSecondaryIndexNames()) {
          String oldIndexName = getIndexName(namespace, oldTableName, indexedColumnName);
          String newIndexName = getIndexName(namespace, newTableName, indexedColumnName);
          renameIndexInternal(
              connection, namespace, newTableName, indexedColumnName, oldIndexName, newIndexName);
        }
        addTableMetadata(connection, namespace, newTableName, tableMetadata, false, false);
      }
    } catch (SQLException e) {
      throw new ExecutionException(
          String.format(
              "Renaming the %s table to %s failed",
              getFullTableName(namespace, oldTableName), getFullTableName(namespace, newTableName)),
          e);
    }
  }

  private void renameIndexInternal(
      Connection connection,
      String schema,
      String table,
      String column,
      String oldIndexName,
      String newIndexName)
      throws SQLException {
    String[] sqls = rdbEngine.renameIndexSqls(schema, table, column, oldIndexName, newIndexName);
    for (String sql : sqls) {
      execute(connection, sql);
    }
  }

  @VisibleForTesting
  void createIndex(
      Connection connection, String schema, String table, String indexedColumn, boolean ifNotExists)
      throws SQLException {
    String indexName = getIndexName(schema, table, indexedColumn);
    String createIndexStatement = rdbEngine.createIndexSql(schema, table, indexName, indexedColumn);
    if (ifNotExists) {
      createIndexStatement = rdbEngine.tryAddIfNotExistsToCreateIndexSql(createIndexStatement);
    }
    try {
      execute(
          connection,
          createIndexStatement,
          ifNotExists ? null : rdbEngine::throwIfDuplicatedIndexWarning);
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

  @Override
  public Set<String> getNamespaceNames() throws ExecutionException {
    try (Connection connection = dataSource.getConnection()) {
      rdbEngine.setConnectionToReadOnly(connection, true);
      return tableMetadataService.getNamespaceNames(connection);
    } catch (SQLException e) {
      throw new ExecutionException("Getting the existing schema names failed", e);
    }
  }

  @Override
  public StorageInfo getStorageInfo(String namespace) {
    return STORAGE_INFO;
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
    tableMetadataService.addTableMetadata(
        connection, namespace, table, metadata, createMetadataTable, overwriteMetadata);
  }

  @VisibleForTesting
  void createTableMetadataTableIfNotExists(Connection connection) throws SQLException {
    tableMetadataService.createTableMetadataTableIfNotExists(connection);
  }

  @VisibleForTesting
  void createMetadataSchemaIfNotExists(Connection connection) throws SQLException {
    createSchemaIfNotExists(connection, metadataSchema);
  }

  private void deleteMetadataSchemaIfEmpty(Connection connection) throws SQLException {
    Set<String> internalTableNames = getInternalTableNames(connection, metadataSchema);
    if (!internalTableNames.isEmpty()) {
      return;
    }

    String sql = rdbEngine.dropNamespaceSql(metadataSchema);
    execute(connection, sql);
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

  private boolean internalTableExists(Connection connection, String namespace, String table)
      throws ExecutionException {
    String fullTableName = encloseFullTableName(namespace, table);
    String sql = rdbEngine.internalTableExistsCheckSql(fullTableName);
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

  private String enclose(String name) {
    return rdbEngine.enclose(name);
  }

  private String encloseFullTableName(String schema, String table) {
    return rdbEngine.encloseFullTableName(schema, table);
  }

  static void execute(Connection connection, String sql) throws SQLException {
    execute(connection, sql, null);
  }

  static void execute(Connection connection, String sql, @Nullable SqlWarningHandler handler)
      throws SQLException {
    if (Strings.isNullOrEmpty(sql)) {
      return;
    }
    try (Statement stmt = connection.createStatement()) {
      stmt.execute(sql);
      throwSqlWarningIfNeeded(handler, stmt);
    }
  }

  private static void throwSqlWarningIfNeeded(SqlWarningHandler handler, Statement stmt)
      throws SQLException {
    if (handler == null) {
      return;
    }
    SQLWarning warning = stmt.getWarnings();
    while (warning != null) {
      handler.throwSqlWarningIfNeeded(warning);
      warning = warning.getNextWarning();
    }
  }

  static void execute(Connection connection, String[] sqls) throws SQLException {
    execute(connection, sqls, null);
  }

  static void execute(
      Connection connection, String[] sqls, @Nullable SqlWarningHandler warningHandler)
      throws SQLException {
    for (String sql : sqls) {
      execute(connection, sql, warningHandler);
    }
  }

  @FunctionalInterface
  interface SqlWarningHandler {
    void throwSqlWarningIfNeeded(SQLWarning warning) throws SQLException;
  }
}
