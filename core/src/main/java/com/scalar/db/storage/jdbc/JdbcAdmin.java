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
import com.scalar.db.api.VirtualTableInfo;
import com.scalar.db.api.VirtualTableJoinType;
import com.scalar.db.common.CoreError;
import com.scalar.db.common.StorageInfoImpl;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.util.ThrowableConsumer;
import com.scalar.db.util.ThrowableFunction;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import javax.sql.DataSource;
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

  private final RdbEngineStrategy rdbEngine;
  private final BasicDataSource dataSource;
  private final String metadataSchema;
  private final TableMetadataService tableMetadataService;
  private final NamespaceMetadataService namespaceMetadataService;
  private final VirtualTableMetadataService virtualTableMetadataService;
  private final boolean requiresExplicitCommit;

  @Inject
  public JdbcAdmin(DatabaseConfig databaseConfig) {
    JdbcConfig config = new JdbcConfig(databaseConfig);
    rdbEngine = RdbEngineFactory.create(config);
    dataSource = JdbcUtils.initDataSourceForAdmin(config, rdbEngine);
    metadataSchema = config.getMetadataSchema();
    requiresExplicitCommit = JdbcUtils.requiresExplicitCommit(dataSource, rdbEngine);
    tableMetadataService =
        new TableMetadataService(metadataSchema, rdbEngine, requiresExplicitCommit);
    namespaceMetadataService =
        new NamespaceMetadataService(metadataSchema, rdbEngine, requiresExplicitCommit);
    virtualTableMetadataService =
        new VirtualTableMetadataService(metadataSchema, rdbEngine, requiresExplicitCommit);
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public JdbcAdmin(BasicDataSource dataSource, JdbcConfig config) {
    rdbEngine = RdbEngineFactory.create(config);
    this.dataSource = dataSource;
    metadataSchema = config.getMetadataSchema();
    requiresExplicitCommit = JdbcUtils.requiresExplicitCommit(dataSource, rdbEngine);
    tableMetadataService =
        new TableMetadataService(metadataSchema, rdbEngine, requiresExplicitCommit);
    namespaceMetadataService =
        new NamespaceMetadataService(metadataSchema, rdbEngine, requiresExplicitCommit);
    virtualTableMetadataService =
        new VirtualTableMetadataService(metadataSchema, rdbEngine, requiresExplicitCommit);
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public JdbcAdmin(BasicDataSource dataSource, JdbcConfig config, boolean requiresExplicitCommit) {
    rdbEngine = RdbEngineFactory.create(config);
    this.dataSource = dataSource;
    metadataSchema = config.getMetadataSchema();
    this.requiresExplicitCommit = requiresExplicitCommit;
    tableMetadataService =
        new TableMetadataService(metadataSchema, rdbEngine, requiresExplicitCommit);
    namespaceMetadataService =
        new NamespaceMetadataService(metadataSchema, rdbEngine, requiresExplicitCommit);
    virtualTableMetadataService =
        new VirtualTableMetadataService(metadataSchema, rdbEngine, requiresExplicitCommit);
  }

  @VisibleForTesting
  JdbcAdmin(
      BasicDataSource dataSource,
      JdbcConfig config,
      VirtualTableMetadataService virtualTableMetadataService,
      boolean requiresExplicitCommit) {
    rdbEngine = RdbEngineFactory.create(config);
    this.dataSource = dataSource;
    metadataSchema = config.getMetadataSchema();
    this.requiresExplicitCommit = requiresExplicitCommit;
    tableMetadataService =
        new TableMetadataService(metadataSchema, rdbEngine, requiresExplicitCommit);
    namespaceMetadataService =
        new NamespaceMetadataService(metadataSchema, rdbEngine, requiresExplicitCommit);
    this.virtualTableMetadataService = virtualTableMetadataService;
  }

  @VisibleForTesting
  JdbcAdmin(
      BasicDataSource dataSource,
      JdbcConfig config,
      TableMetadataService tableMetadataService,
      NamespaceMetadataService namespaceMetadataService,
      VirtualTableMetadataService virtualTableMetadataService,
      boolean requiresExplicitCommit) {
    rdbEngine = RdbEngineFactory.create(config);
    this.dataSource = dataSource;
    metadataSchema = config.getMetadataSchema();
    this.tableMetadataService = tableMetadataService;
    this.namespaceMetadataService = namespaceMetadataService;
    this.virtualTableMetadataService = virtualTableMetadataService;
    this.requiresExplicitCommit = requiresExplicitCommit;
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

    try {
      withConnection(
          dataSource,
          requiresExplicitCommit,
          connection -> {
            execute(connection, rdbEngine.createSchemaSqls(namespace), requiresExplicitCommit);
            createMetadataSchemaIfNotExists(connection);
            createNamespacesTableIfNotExists(connection);
            namespaceMetadataService.insertIntoNamespacesTable(connection, namespace);
          });
    } catch (SQLException e) {
      throw new ExecutionException("Creating the " + namespace + " schema failed", e);
    }
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    try {
      withConnection(
          dataSource,
          requiresExplicitCommit,
          connection -> {
            createMetadataSchemaIfNotExists(connection);
            createTableInternal(connection, namespace, table, metadata, false);
            addTableMetadata(connection, namespace, table, metadata, true, false);
          });
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
      execute(
          connection,
          stmts,
          requiresExplicitCommit,
          ifNotExists ? null : rdbEngine::throwIfDuplicatedIndexWarning);
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
    try {
      withConnection(
          dataSource,
          requiresExplicitCommit,
          connection -> {
            VirtualTableInfo virtualTableInfo =
                virtualTableMetadataService.getVirtualTableInfo(connection, namespace, table);
            if (virtualTableInfo != null) {
              // For a virtual table

              // Drop the virtual table view
              dropVirtualTableView(connection, namespace, table);

              // Delete its metadata
              virtualTableMetadataService.deleteFromVirtualTablesTable(
                  connection, namespace, table);
              virtualTableMetadataService.deleteVirtualTablesTableIfEmpty(connection);
              return;
            }

            List<VirtualTableInfo> virtualTableInfos =
                virtualTableMetadataService.getVirtualTableInfosBySourceTable(
                    connection, namespace, table);
            if (!virtualTableInfos.isEmpty()) {
              // For a source table of virtual tables
              throw new IllegalArgumentException(
                  CoreError.SOURCE_TABLES_CANNOT_BE_DROPPED_WHILE_VIRTUAL_TABLES_EXIST.buildMessage(
                      getFullTableName(namespace, table),
                      virtualTableInfos.stream()
                          .map(v -> getFullTableName(v.getNamespaceName(), v.getTableName()))
                          .collect(Collectors.joining(","))));
            }

            // For a regular table
            dropTableInternal(connection, namespace, table);
            tableMetadataService.deleteTableMetadata(connection, namespace, table, true);
          });
    } catch (SQLException e) {
      throw new ExecutionException(
          "Dropping the " + getFullTableName(namespace, table) + " table failed", e);
    }
  }

  private void dropTableInternal(Connection connection, String schema, String table)
      throws SQLException {
    String dropTableStatement = "DROP TABLE " + encloseFullTableName(schema, table);
    execute(connection, dropTableStatement, requiresExplicitCommit);
  }

  @Override
  public void dropNamespace(String namespace) throws ExecutionException {
    try {
      withConnection(
          dataSource,
          requiresExplicitCommit,
          connection -> {
            Set<String> remainingTables = getInternalTableNames(connection, namespace);
            if (!remainingTables.isEmpty()) {
              throw new IllegalArgumentException(
                  CoreError.NAMESPACE_WITH_NON_SCALARDB_TABLES_CANNOT_BE_DROPPED.buildMessage(
                      namespace, remainingTables));
            }
            execute(connection, rdbEngine.dropNamespaceSql(namespace), requiresExplicitCommit);
            namespaceMetadataService.deleteFromNamespacesTable(connection, namespace);
            namespaceMetadataService.deleteNamespacesTableIfEmpty(connection);
            deleteMetadataSchemaIfEmpty(connection);
          });
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
    return executeQuery(
        connection,
        sql,
        requiresExplicitCommit,
        ps -> ps.setString(1, namespace),
        rs -> {
          Set<String> tableNames = new HashSet<>();
          while (rs.next()) {
            tableNames.add(rs.getString(1));
          }
          return tableNames;
        });
  }

  @Override
  public void truncateTable(String namespace, String table) throws ExecutionException {
    try {
      withConnection(
          dataSource,
          requiresExplicitCommit,
          connection -> {
            VirtualTableInfo virtualTableInfo =
                virtualTableMetadataService.getVirtualTableInfo(connection, namespace, table);
            if (virtualTableInfo != null) {
              // For a virtual table

              // truncate the source tables
              execute(
                  connection,
                  rdbEngine.truncateTableSql(
                      virtualTableInfo.getLeftSourceNamespaceName(),
                      virtualTableInfo.getLeftSourceTableName()),
                  requiresExplicitCommit);
              execute(
                  connection,
                  rdbEngine.truncateTableSql(
                      virtualTableInfo.getRightSourceNamespaceName(),
                      virtualTableInfo.getRightSourceTableName()),
                  requiresExplicitCommit);
              return;
            }

            // For a regular table
            String truncateTableStatement = rdbEngine.truncateTableSql(namespace, table);
            execute(connection, truncateTableStatement, requiresExplicitCommit);
          });
    } catch (SQLException e) {
      throw new ExecutionException(
          "Truncating the " + getFullTableName(namespace, table) + " table failed", e);
    }
  }

  @Nullable
  @Override
  public TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException {
    try {
      return withConnection(
          dataSource,
          requiresExplicitCommit,
          connection -> {
            rdbEngine.setConnectionToReadOnly(connection, true);

            // If it's a regular table, return its metadata
            TableMetadata tableMetadata =
                tableMetadataService.getTableMetadata(connection, namespace, table);
            if (tableMetadata != null) {
              return tableMetadata;
            }

            // If it's a virtual table, merge the source table metadata and return it
            VirtualTableInfo virtualTableInfo =
                virtualTableMetadataService.getVirtualTableInfo(connection, namespace, table);
            if (virtualTableInfo != null) {
              TableMetadata leftSourceTableMetadata =
                  tableMetadataService.getTableMetadata(
                      connection,
                      virtualTableInfo.getLeftSourceNamespaceName(),
                      virtualTableInfo.getLeftSourceTableName());
              TableMetadata rightSourceTableMetadata =
                  tableMetadataService.getTableMetadata(
                      connection,
                      virtualTableInfo.getRightSourceNamespaceName(),
                      virtualTableInfo.getRightSourceTableName());
              assert leftSourceTableMetadata != null && rightSourceTableMetadata != null;
              return mergeSourceTableMetadata(leftSourceTableMetadata, rightSourceTableMetadata);
            }

            return null;
          });
    } catch (SQLException e) {
      throw new ExecutionException(
          "Getting a table metadata for the "
              + getFullTableName(namespace, table)
              + " table failed",
          e);
    }
  }

  private TableMetadata mergeSourceTableMetadata(
      TableMetadata leftSourceTableMetadata, TableMetadata rightSourceTableMetadata) {
    TableMetadata.Builder builder = TableMetadata.newBuilder();

    // Add partition keys from the left source table (both tables should have the same partition
    // keys)
    for (String partitionKey : leftSourceTableMetadata.getPartitionKeyNames()) {
      builder.addPartitionKey(partitionKey);
    }

    // Add clustering keys with their ordering from the left source table (both tables should have
    // the same clustering keys)
    for (String clusteringKey : leftSourceTableMetadata.getClusteringKeyNames()) {
      builder.addClusteringKey(
          clusteringKey, leftSourceTableMetadata.getClusteringOrder(clusteringKey));
    }

    // Get primary key columns to avoid duplicates
    Set<String> primaryKeyColumns =
        Sets.newHashSet(
            Iterables.concat(
                leftSourceTableMetadata.getPartitionKeyNames(),
                leftSourceTableMetadata.getClusteringKeyNames()));

    // Add all columns from the left source table
    for (String columnName : leftSourceTableMetadata.getColumnNames()) {
      builder.addColumn(columnName, leftSourceTableMetadata.getColumnDataType(columnName));
    }

    // Add non-primary key columns from the right source table
    for (String columnName : rightSourceTableMetadata.getColumnNames()) {
      if (!primaryKeyColumns.contains(columnName)) {
        builder.addColumn(columnName, rightSourceTableMetadata.getColumnDataType(columnName));
      }
    }

    // Add secondary indexes from both tables
    for (String secondaryIndex : leftSourceTableMetadata.getSecondaryIndexNames()) {
      builder.addSecondaryIndex(secondaryIndex);
    }
    for (String secondaryIndex : rightSourceTableMetadata.getSecondaryIndexNames()) {
      builder.addSecondaryIndex(secondaryIndex);
    }

    return builder.build();
  }

  @VisibleForTesting
  TableMetadata getImportTableMetadata(
      String namespace, String table, Map<String, DataType> overrideColumnsType)
      throws ExecutionException {
    try {
      return withConnection(
          dataSource,
          requiresExplicitCommit,
          connection -> {
            rdbEngine.setConnectionToReadOnly(connection, true);

            String catalogName = rdbEngine.getCatalogName(namespace);
            String schemaName = rdbEngine.getSchemaName(namespace);

            if (!internalTableExists(connection, namespace, table)) {
              throw new IllegalArgumentException(
                  CoreError.TABLE_NOT_FOUND.buildMessage(getFullTableName(namespace, table)));
            }

            TableMetadata.Builder builder = TableMetadata.newBuilder();
            boolean primaryKeyExists = false;

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

            return builder.build();
          });
    } catch (SQLException e) {
      throw new ExecutionException(
          "Getting a table metadata for the "
              + getFullTableName(namespace, table)
              + " table failed",
          e);
    }
  }

  @Override
  public void importTable(
      String namespace,
      String table,
      Map<String, String> options,
      Map<String, DataType> overrideColumnsType)
      throws ExecutionException {
    rdbEngine.throwIfImportNotSupported();

    try {
      TableMetadata tableMetadata = getImportTableMetadata(namespace, table, overrideColumnsType);
      withConnection(
          dataSource,
          requiresExplicitCommit,
          connection -> {
            createMetadataSchemaIfNotExists(connection);
            createNamespacesTableIfNotExists(connection);
            upsertIntoNamespacesTable(connection, namespace);
            addTableMetadata(connection, namespace, table, tableMetadata, true, false);
          });
    } catch (SQLException | ExecutionException e) {
      throw new ExecutionException(
          String.format("Importing the %s table failed", getFullTableName(namespace, table)), e);
    }
  }

  @Override
  public Set<String> getNamespaceTableNames(String namespace) throws ExecutionException {
    try {
      return withConnection(
          dataSource,
          requiresExplicitCommit,
          connection -> {
            rdbEngine.setConnectionToReadOnly(connection, true);

            // Get both regular and virtual table names
            Set<String> tableNames = new HashSet<>();
            tableNames.addAll(tableMetadataService.getNamespaceTableNames(connection, namespace));
            tableNames.addAll(
                virtualTableMetadataService.getNamespaceTableNames(connection, namespace));
            return tableNames;
          });
    } catch (SQLException e) {
      throw new ExecutionException(
          "Getting the list of tables of the " + namespace + " schema failed", e);
    }
  }

  @Override
  public boolean namespaceExists(String namespace) throws ExecutionException {
    try {
      return withConnection(
          dataSource,
          requiresExplicitCommit,
          connection -> {
            rdbEngine.setConnectionToReadOnly(connection, true);
            return namespaceMetadataService.namespaceExists(connection, namespace);
          });
    } catch (SQLException e) {
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
    try {
      withConnection(
          dataSource,
          requiresExplicitCommit,
          connection -> {
            VirtualTableInfo virtualTableInfo =
                virtualTableMetadataService.getVirtualTableInfo(connection, namespace, table);
            if (virtualTableInfo != null) {
              // For a virtual table

              TableMetadata leftSourceTableMetadata =
                  tableMetadataService.getTableMetadata(
                      connection,
                      virtualTableInfo.getLeftSourceNamespaceName(),
                      virtualTableInfo.getLeftSourceTableName());
              if (leftSourceTableMetadata.getColumnNames().contains(columnName)) {
                // If the column exists in the left source table, create the index there
                createIndexInternal(
                    connection,
                    virtualTableInfo.getLeftSourceNamespaceName(),
                    virtualTableInfo.getLeftSourceTableName(),
                    columnName,
                    leftSourceTableMetadata.getColumnDataType(columnName));
              }

              TableMetadata rightSourceTableMetadata =
                  tableMetadataService.getTableMetadata(
                      connection,
                      virtualTableInfo.getRightSourceNamespaceName(),
                      virtualTableInfo.getRightSourceTableName());
              if (rightSourceTableMetadata.getColumnNames().contains(columnName)) {
                // If the column exists in the right source table, create the index there
                createIndexInternal(
                    connection,
                    virtualTableInfo.getRightSourceNamespaceName(),
                    virtualTableInfo.getRightSourceTableName(),
                    columnName,
                    rightSourceTableMetadata.getColumnDataType(columnName));
              }

              return;
            }

            // For a regular table
            TableMetadata tableMetadata =
                tableMetadataService.getTableMetadata(connection, namespace, table);
            createIndexInternal(
                connection,
                namespace,
                table,
                columnName,
                tableMetadata.getColumnDataType(columnName));
          });
    } catch (SQLException e) {
      throw new ExecutionException(
          String.format(
              "Creating the secondary index on the %s column for the %s table failed",
              columnName, getFullTableName(namespace, table)),
          e);
    }
  }

  private void createIndexInternal(
      Connection connection, String namespace, String table, String columnName, DataType dataType)
      throws SQLException {
    alterToIndexColumnTypeIfNecessary(connection, namespace, table, columnName, dataType);
    createIndex(connection, namespace, table, columnName, false);
    tableMetadataService.updateTableMetadata(connection, namespace, table, columnName, true);
  }

  private void alterToIndexColumnTypeIfNecessary(
      Connection connection, String namespace, String table, String columnName, DataType dataType)
      throws SQLException {
    String columnTypeForKey = rdbEngine.getDataTypeForSecondaryIndex(dataType);
    if (columnTypeForKey == null) {
      // The column type does not need to be altered to be compatible with being secondary index
      return;
    }

    String[] sqls = rdbEngine.alterColumnTypeSql(namespace, table, columnName, columnTypeForKey);
    execute(connection, sqls, requiresExplicitCommit);
  }

  private void alterToRegularColumnTypeIfNecessary(
      Connection connection, String namespace, String table, String columnName, DataType dataType)
      throws SQLException {
    String columnTypeForKey = rdbEngine.getDataTypeForSecondaryIndex(dataType);
    if (columnTypeForKey == null) {
      // The column type is already the type for a regular column. It was not altered to be
      // compatible with being a secondary index, so no alteration is necessary.
      return;
    }

    String columnType = rdbEngine.getDataTypeForEngine(dataType);
    String[] sqls = rdbEngine.alterColumnTypeSql(namespace, table, columnName, columnType);
    execute(connection, sqls, requiresExplicitCommit);
  }

  @Override
  public void dropIndex(String namespace, String table, String columnName)
      throws ExecutionException {
    try {
      withConnection(
          dataSource,
          requiresExplicitCommit,
          connection -> {
            VirtualTableInfo virtualTableInfo =
                virtualTableMetadataService.getVirtualTableInfo(connection, namespace, table);
            if (virtualTableInfo != null) {
              // For a virtual table

              TableMetadata leftSourceTableMetadata =
                  tableMetadataService.getTableMetadata(
                      connection,
                      virtualTableInfo.getLeftSourceNamespaceName(),
                      virtualTableInfo.getLeftSourceTableName());
              if (leftSourceTableMetadata.getColumnNames().contains(columnName)) {
                // If the column exists in the left source table, drop the index there
                dropIndexInternal(
                    connection,
                    virtualTableInfo.getLeftSourceNamespaceName(),
                    virtualTableInfo.getLeftSourceTableName(),
                    columnName,
                    leftSourceTableMetadata.getColumnDataType(columnName));
              }

              TableMetadata rightSourceTableMetadata =
                  tableMetadataService.getTableMetadata(
                      connection,
                      virtualTableInfo.getRightSourceNamespaceName(),
                      virtualTableInfo.getRightSourceTableName());
              if (rightSourceTableMetadata.getColumnNames().contains(columnName)) {
                // If the column exists in the right source table, drop the index there
                dropIndexInternal(
                    connection,
                    virtualTableInfo.getRightSourceNamespaceName(),
                    virtualTableInfo.getRightSourceTableName(),
                    columnName,
                    rightSourceTableMetadata.getColumnDataType(columnName));
              }

              return;
            }

            // For a regular table
            TableMetadata tableMetadata =
                tableMetadataService.getTableMetadata(connection, namespace, table);
            dropIndexInternal(
                connection,
                namespace,
                table,
                columnName,
                tableMetadata.getColumnDataType(columnName));
          });
    } catch (SQLException e) {
      throw new ExecutionException(
          String.format(
              "Dropping the secondary index on the %s column for the %s table failed",
              columnName, getFullTableName(namespace, table)),
          e);
    }
  }

  private void dropIndexInternal(
      Connection connection, String namespace, String table, String columnName, DataType dataType)
      throws SQLException {
    dropIndex(connection, namespace, table, columnName);
    alterToRegularColumnTypeIfNecessary(connection, namespace, table, columnName, dataType);
    tableMetadataService.updateTableMetadata(connection, namespace, table, columnName, false);
  }

  @Override
  public void repairNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    rdbEngine.throwIfInvalidNamespaceName(namespace);

    try {
      withConnection(
          dataSource,
          requiresExplicitCommit,
          connection -> {
            createSchemaIfNotExists(connection, namespace);
            createMetadataSchemaIfNotExists(connection);
            createNamespacesTableIfNotExists(connection);
            upsertIntoNamespacesTable(connection, namespace);
          });
    } catch (SQLException e) {
      throw new ExecutionException(String.format("Repairing the %s schema failed", namespace), e);
    }
  }

  @Override
  public void repairTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    try {
      withConnection(
          dataSource,
          requiresExplicitCommit,
          connection -> {
            throwIfVirtualTableOrSourceTable(connection, namespace, table, "repairTable()");

            createMetadataSchemaIfNotExists(connection);
            createTableInternal(connection, namespace, table, metadata, true);
            addTableMetadata(connection, namespace, table, metadata, true, true);
          });
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
      withConnection(
          dataSource,
          requiresExplicitCommit,
          connection -> {
            throwIfVirtualTableOrSourceTable(connection, namespace, table, "addNewColumnToTable()");

            TableMetadata currentTableMetadata =
                tableMetadataService.getTableMetadata(connection, namespace, table);
            TableMetadata updatedTableMetadata =
                TableMetadata.newBuilder(currentTableMetadata)
                    .addColumn(columnName, columnType)
                    .build();
            String addNewColumnStatement =
                "ALTER TABLE "
                    + encloseFullTableName(namespace, table)
                    + " ADD "
                    + enclose(columnName)
                    + " "
                    + getVendorDbColumnType(updatedTableMetadata, columnName);
            execute(connection, addNewColumnStatement, requiresExplicitCommit);
            addTableMetadata(connection, namespace, table, updatedTableMetadata, false, true);
          });
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
      withConnection(
          dataSource,
          requiresExplicitCommit,
          connection -> {
            throwIfVirtualTableOrSourceTable(connection, namespace, table, "dropColumnFromTable()");

            TableMetadata currentTableMetadata =
                tableMetadataService.getTableMetadata(connection, namespace, table);
            TableMetadata updatedTableMetadata =
                TableMetadata.newBuilder(currentTableMetadata).removeColumn(columnName).build();
            String[] dropColumnStatements = rdbEngine.dropColumnSql(namespace, table, columnName);

            execute(connection, dropColumnStatements, requiresExplicitCommit);
            addTableMetadata(connection, namespace, table, updatedTableMetadata, false, true);
          });
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
      withConnection(
          dataSource,
          requiresExplicitCommit,
          connection -> {
            throwIfVirtualTableOrSourceTable(connection, namespace, table, "renameColumn()");

            TableMetadata currentTableMetadata =
                tableMetadataService.getTableMetadata(connection, namespace, table);
            assert currentTableMetadata != null;
            rdbEngine.throwIfRenameColumnNotSupported(oldColumnName, currentTableMetadata);
            TableMetadata.Builder tableMetadataBuilder =
                TableMetadata.newBuilder(currentTableMetadata)
                    .renameColumn(oldColumnName, newColumnName);
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

            execute(connection, renameColumnStatement, requiresExplicitCommit);
            if (currentTableMetadata.getSecondaryIndexNames().contains(oldColumnName)) {
              String oldIndexName = getIndexName(namespace, table, oldColumnName);
              String newIndexName = getIndexName(namespace, table, newColumnName);
              renameIndexInternal(
                  connection, namespace, table, newColumnName, oldIndexName, newIndexName);
            }
            addTableMetadata(connection, namespace, table, updatedTableMetadata, false, true);
          });
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
      withConnection(
          dataSource,
          requiresExplicitCommit,
          connection -> {
            throwIfVirtualTableOrSourceTable(connection, namespace, table, "alterColumnType()");

            TableMetadata currentTableMetadata =
                tableMetadataService.getTableMetadata(connection, namespace, table);
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

            execute(connection, alterColumnTypeStatements, requiresExplicitCommit);
            addTableMetadata(connection, namespace, table, updatedTableMetadata, false, true);
          });
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
      withConnection(
          dataSource,
          requiresExplicitCommit,
          connection -> {
            throwIfVirtualTableOrSourceTable(connection, namespace, oldTableName, "renameTable()");

            TableMetadata tableMetadata =
                tableMetadataService.getTableMetadata(connection, namespace, oldTableName);
            assert tableMetadata != null;
            String renameTableStatement =
                rdbEngine.renameTableSql(namespace, oldTableName, newTableName);

            execute(connection, renameTableStatement, requiresExplicitCommit);
            tableMetadataService.deleteTableMetadata(connection, namespace, oldTableName, false);
            for (String indexedColumnName : tableMetadata.getSecondaryIndexNames()) {
              String oldIndexName = getIndexName(namespace, oldTableName, indexedColumnName);
              String newIndexName = getIndexName(namespace, newTableName, indexedColumnName);
              renameIndexInternal(
                  connection,
                  namespace,
                  newTableName,
                  indexedColumnName,
                  oldIndexName,
                  newIndexName);
            }
            addTableMetadata(connection, namespace, newTableName, tableMetadata, false, false);
          });
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
    execute(connection, sqls, requiresExplicitCommit);
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
          requiresExplicitCommit,
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
    execute(connection, sql, requiresExplicitCommit);
  }

  private String getIndexName(String schema, String table, String indexedColumn) {
    return String.join("_", INDEX_NAME_PREFIX, schema, table, indexedColumn);
  }

  @Override
  public Set<String> getNamespaceNames() throws ExecutionException {
    try {
      return withConnection(
          dataSource,
          requiresExplicitCommit,
          connection -> {
            rdbEngine.setConnectionToReadOnly(connection, true);
            return namespaceMetadataService.getNamespaceNames(connection);
          });
    } catch (SQLException e) {
      throw new ExecutionException("Getting the existing schema names failed", e);
    }
  }

  @Override
  public void upgrade(Map<String, String> options) throws ExecutionException {
    try {
      withConnection(
          dataSource,
          requiresExplicitCommit,
          connection -> {
            Set<String> namespaceNamesOfExistingTables = new HashSet<>();

            // Get namespaces of existing tables (both regular and virtual tables)
            namespaceNamesOfExistingTables.addAll(
                tableMetadataService.getNamespaceNamesOfExistingTables(connection));
            namespaceNamesOfExistingTables.addAll(
                virtualTableMetadataService.getNamespaceNamesOfExistingTables(connection));

            if (namespaceNamesOfExistingTables.isEmpty()) {
              // No existing tables, so no need to upgrade
              return;
            }

            createMetadataSchemaIfNotExists(connection);
            createNamespacesTableIfNotExists(connection);
            for (String namespace : namespaceNamesOfExistingTables) {
              upsertIntoNamespacesTable(connection, namespace);
            }
          });
    } catch (SQLException e) {
      throw new ExecutionException("Upgrading the ScalarDB environment failed", e);
    }
  }

  @Override
  public StorageInfo getStorageInfo(String namespace) throws ExecutionException {
    try {
      boolean consistentVirtualTableReadGuaranteed =
          withConnection(
              dataSource,
              requiresExplicitCommit,
              connection -> {
                int isolationLevel = connection.getTransactionIsolation();
                return isolationLevel
                    >= rdbEngine.getMinimumIsolationLevelForConsistentVirtualTableRead();
              });

      return new StorageInfoImpl(
          "jdbc",
          StorageInfo.MutationAtomicityUnit.STORAGE,
          // No limit on the number of mutations
          Integer.MAX_VALUE,
          consistentVirtualTableReadGuaranteed);
    } catch (SQLException e) {
      throw new ExecutionException("Getting the transaction isolation level failed", e);
    }
  }

  @Override
  public void createVirtualTable(
      String namespace,
      String table,
      String leftSourceNamespace,
      String leftSourceTable,
      String rightSourceNamespace,
      String rightSourceTable,
      VirtualTableJoinType joinType,
      Map<String, String> options)
      throws ExecutionException {
    rdbEngine.throwIfInvalidTableName(table);

    try {
      withConnection(
          dataSource,
          requiresExplicitCommit,
          connection -> {
            // Create View
            createVirtualTableView(
                connection,
                namespace,
                table,
                leftSourceNamespace,
                leftSourceTable,
                rightSourceNamespace,
                rightSourceTable,
                joinType);

            // Add metadata
            createMetadataSchemaIfNotExists(connection);
            virtualTableMetadataService.createVirtualTablesTableIfNotExists(connection);
            virtualTableMetadataService.insertIntoVirtualTablesTable(
                connection,
                namespace,
                table,
                leftSourceNamespace,
                leftSourceTable,
                rightSourceNamespace,
                rightSourceTable,
                joinType,
                "");
          });
    } catch (SQLException e) {
      throw new ExecutionException(
          "Creating the virtual table "
              + getFullTableName(namespace, table)
              + " from source tables "
              + getFullTableName(leftSourceNamespace, leftSourceTable)
              + " and "
              + getFullTableName(rightSourceNamespace, rightSourceTable)
              + " failed",
          e);
    }
  }

  @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
  private void createVirtualTableView(
      Connection connection,
      String namespace,
      String table,
      String leftSourceNamespace,
      String leftSourceTable,
      String rightSourceNamespace,
      String rightSourceTable,
      VirtualTableJoinType joinType)
      throws SQLException {
    TableMetadata leftSourceTableMetadata =
        tableMetadataService.getTableMetadata(connection, leftSourceNamespace, leftSourceTable);
    TableMetadata rightSourceTableMetadata =
        tableMetadataService.getTableMetadata(connection, rightSourceNamespace, rightSourceTable);
    assert leftSourceTableMetadata != null && rightSourceTableMetadata != null;

    StringBuilder createViewSql = new StringBuilder("CREATE VIEW ");
    createViewSql.append(encloseFullTableName(namespace, table));
    createViewSql.append(" AS SELECT ");

    // Add primary key columns from the left source table
    for (String pkColumn : leftSourceTableMetadata.getPartitionKeyNames()) {
      createViewSql.append("t1.").append(enclose(pkColumn));
      createViewSql.append(" AS ").append(enclose(pkColumn)).append(", ");
    }
    for (String pkColumn : leftSourceTableMetadata.getClusteringKeyNames()) {
      createViewSql.append("t1.").append(enclose(pkColumn));
      createViewSql.append(" AS ").append(enclose(pkColumn)).append(", ");
    }

    // Get primary key columns (same for both tables)
    Set<String> primaryKeyColumns =
        Sets.newHashSet(
            Iterables.concat(
                leftSourceTableMetadata.getPartitionKeyNames(),
                leftSourceTableMetadata.getClusteringKeyNames()));

    // Add non-primary key columns from the left source table
    for (String column : leftSourceTableMetadata.getColumnNames()) {
      if (!primaryKeyColumns.contains(column)) {
        createViewSql.append("t1.").append(enclose(column));
        createViewSql.append(" AS ").append(enclose(column)).append(", ");
      }
    }

    // Add non-primary key columns from the right source table
    for (String column : rightSourceTableMetadata.getColumnNames()) {
      if (!primaryKeyColumns.contains(column)) {
        createViewSql.append("t2.").append(enclose(column));
        createViewSql.append(" AS ").append(enclose(column)).append(", ");
      }
    }

    // Remove trailing comma and space
    createViewSql.setLength(createViewSql.length() - 2);

    // Add FROM clause
    createViewSql.append(" FROM ");
    createViewSql.append(encloseFullTableName(leftSourceNamespace, leftSourceTable));
    createViewSql.append(" t1 ");

    // Add JOIN type based on joinType parameter
    switch (joinType) {
      case INNER:
        createViewSql.append("INNER JOIN ");
        break;
      case LEFT_OUTER:
        createViewSql.append("LEFT OUTER JOIN ");
        break;
      default:
        throw new AssertionError("Unexpected join type: " + joinType);
    }

    createViewSql.append(encloseFullTableName(rightSourceNamespace, rightSourceTable));
    createViewSql.append(" t2 ON ");

    // Add JOIN conditions for partition keys
    boolean firstCondition = true;
    for (String pkColumn : leftSourceTableMetadata.getPartitionKeyNames()) {
      if (!firstCondition) {
        createViewSql.append(" AND ");
      }
      createViewSql.append("t1.").append(enclose(pkColumn));
      createViewSql.append(" = ");
      createViewSql.append("t2.").append(enclose(pkColumn));
      firstCondition = false;
    }

    // Add JOIN conditions for clustering keys
    for (String ckColumn : leftSourceTableMetadata.getClusteringKeyNames()) {
      if (!firstCondition) {
        createViewSql.append(" AND ");
      }
      createViewSql.append("t1.").append(enclose(ckColumn));
      createViewSql.append(" = ");
      createViewSql.append("t2.").append(enclose(ckColumn));
      firstCondition = false;
    }

    execute(connection, createViewSql.toString(), requiresExplicitCommit);
  }

  private void dropVirtualTableView(Connection connection, String namespace, String table)
      throws SQLException {
    String dropViewStatement = "DROP VIEW " + encloseFullTableName(namespace, table);
    execute(connection, dropViewStatement, requiresExplicitCommit);
  }

  @Override
  public Optional<VirtualTableInfo> getVirtualTableInfo(String namespace, String table)
      throws ExecutionException {
    try {
      return withConnection(
          dataSource,
          requiresExplicitCommit,
          connection -> {
            rdbEngine.setConnectionToReadOnly(connection, true);
            return Optional.ofNullable(
                virtualTableMetadataService.getVirtualTableInfo(connection, namespace, table));
          });
    } catch (SQLException e) {
      throw new ExecutionException(
          "Getting virtual table info for the "
              + getFullTableName(namespace, table)
              + " table failed",
          e);
    }
  }

  private void throwIfVirtualTableOrSourceTable(
      Connection connection, String namespace, String table, String method) throws SQLException {
    VirtualTableInfo virtualTableInfo =
        virtualTableMetadataService.getVirtualTableInfo(connection, namespace, table);
    if (virtualTableInfo != null) {
      throw new UnsupportedOperationException(
          "Currently, " + method + " is not supported for virtual tables and their source tables");
    }

    List<VirtualTableInfo> virtualTableInfos =
        virtualTableMetadataService.getVirtualTableInfosBySourceTable(connection, namespace, table);
    if (!virtualTableInfos.isEmpty()) {
      throw new UnsupportedOperationException(
          "Currently, " + method + " is not supported for virtual tables and their source tables");
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
    tableMetadataService.addTableMetadata(
        connection, namespace, table, metadata, createMetadataTable, overwriteMetadata);
  }

  @VisibleForTesting
  void createTableMetadataTableIfNotExists(Connection connection) throws SQLException {
    tableMetadataService.createTableMetadataTableIfNotExists(connection);
  }

  @VisibleForTesting
  void createNamespacesTableIfNotExists(Connection connection) throws SQLException {
    namespaceMetadataService.createNamespacesTableIfNotExists(connection);
  }

  @VisibleForTesting
  void upsertIntoNamespacesTable(Connection connection, String namespace) throws SQLException {
    namespaceMetadataService.upsertIntoNamespacesTable(connection, namespace);
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

    String sql = rdbEngine.deleteMetadataSchemaSql(metadataSchema);
    execute(connection, sql, requiresExplicitCommit);
  }

  private void createTable(Connection connection, String createTableStatement, boolean ifNotExists)
      throws SQLException {
    String stmt = createTableStatement;
    if (ifNotExists) {
      stmt = rdbEngine.tryAddIfNotExistsToCreateTableSql(createTableStatement);
    }
    try {
      execute(connection, stmt, requiresExplicitCommit);
    } catch (SQLException e) {
      // Suppress the exception thrown when the table already exists
      if (!(ifNotExists && rdbEngine.isDuplicateTableError(e))) {
        throw e;
      }
    }
  }

  private boolean internalTableExists(Connection connection, String namespace, String table)
      throws SQLException {
    String fullTableName = encloseFullTableName(namespace, table);
    String sql = rdbEngine.internalTableExistsCheckSql(fullTableName);
    try {
      execute(connection, sql, requiresExplicitCommit);
      return true;
    } catch (SQLException e) {
      // An exception will be thrown if the table does not exist when executing the select query
      if (rdbEngine.isUndefinedTableError(e)) {
        return false;
      }
      throw e;
    }
  }

  private void createSchemaIfNotExists(Connection connection, String schema) throws SQLException {
    String[] sqls = rdbEngine.createSchemaIfNotExistsSqls(schema);
    try {
      execute(connection, sqls, requiresExplicitCommit);
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

  static void withConnection(
      DataSource dataSource,
      boolean requiresExplicitCommit,
      ThrowableConsumer<Connection, SQLException> consumer)
      throws SQLException {
    withConnection(
        dataSource,
        requiresExplicitCommit,
        connection -> {
          consumer.accept(connection);
          return null;
        });
  }

  static <T> T withConnection(
      DataSource dataSource,
      boolean requiresExplicitCommit,
      ThrowableFunction<Connection, T, SQLException> function)
      throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      if (requiresExplicitCommit) {
        connection.setAutoCommit(false);
      }
      try {
        return function.apply(connection);
      } catch (SQLException e) {
        if (requiresExplicitCommit) {
          try {
            connection.rollback();
          } catch (SQLException rollbackEx) {
            e.addSuppressed(rollbackEx);
          }
        }
        throw e;
      }
    }
  }

  static void execute(Connection connection, String sql, boolean requiresExplicitCommit)
      throws SQLException {
    execute(connection, sql, requiresExplicitCommit, null);
  }

  static void execute(
      Connection connection,
      String sql,
      boolean requiresExplicitCommit,
      @Nullable SqlWarningHandler handler)
      throws SQLException {
    if (Strings.isNullOrEmpty(sql)) {
      return;
    }

    try (Statement stmt = connection.createStatement()) {
      stmt.execute(sql);
      if (requiresExplicitCommit) {
        connection.commit();
      }

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

  static void execute(Connection connection, String[] sqls, boolean requiresExplicitCommit)
      throws SQLException {
    execute(connection, sqls, requiresExplicitCommit, null);
  }

  static void execute(
      Connection connection,
      String[] sqls,
      boolean requiresExplicitCommit,
      @Nullable SqlWarningHandler warningHandler)
      throws SQLException {
    for (String sql : sqls) {
      execute(connection, sql, requiresExplicitCommit, warningHandler);
    }
  }

  static void executeUpdate(
      Connection connection,
      String sql,
      boolean requiresExplicitCommit,
      ThrowableConsumer<PreparedStatement, SQLException> paramSetter)
      throws SQLException {
    try (PreparedStatement ps = connection.prepareStatement(sql)) {
      paramSetter.accept(ps);
      ps.executeUpdate();
      if (requiresExplicitCommit) {
        connection.commit();
      }
    }
  }

  static <T> T executeQuery(
      Connection connection,
      String sql,
      boolean requiresExplicitCommit,
      ThrowableFunction<ResultSet, T, SQLException> resultMapper)
      throws SQLException {
    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(sql)) {
      T result = resultMapper.apply(rs);
      if (requiresExplicitCommit) {
        connection.commit();
      }
      return result;
    }
  }

  static <T> T executeQuery(
      Connection connection,
      String sql,
      boolean requiresExplicitCommit,
      ThrowableConsumer<PreparedStatement, SQLException> paramSetter,
      ThrowableFunction<ResultSet, T, SQLException> resultMapper)
      throws SQLException {
    try (PreparedStatement ps = connection.prepareStatement(sql)) {
      paramSetter.accept(ps);
      try (ResultSet rs = ps.executeQuery()) {
        T result = resultMapper.apply(rs);
        if (requiresExplicitCommit) {
          connection.commit();
        }
        return result;
      }
    }
  }

  @FunctionalInterface
  interface SqlWarningHandler {
    void throwSqlWarningIfNeeded(SQLWarning warning) throws SQLException;
  }
}
