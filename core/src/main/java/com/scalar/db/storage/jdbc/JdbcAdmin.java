package com.scalar.db.storage.jdbc;

import static com.scalar.db.storage.jdbc.query.QueryUtils.enclosedFullTableName;
import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.query.QueryUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressFBWarnings({"OBL_UNSATISFIED_OBLIGATION", "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE"})
@ThreadSafe
public class JdbcAdmin implements DistributedStorageAdmin {

  public static final String METADATA_SCHEMA = "scalardb";
  public static final String METADATA_TABLE = "metadata";
  @VisibleForTesting static final String METADATA_COL_FULL_TABLE_NAME = "full_table_name";
  @VisibleForTesting static final String METADATA_COL_COLUMN_NAME = "column_name";
  @VisibleForTesting static final String METADATA_COL_DATA_TYPE = "data_type";
  @VisibleForTesting static final String METADATA_COL_KEY_TYPE = "key_type";
  @VisibleForTesting static final String METADATA_COL_CLUSTERING_ORDER = "clustering_order";
  @VisibleForTesting static final String METADATA_COL_INDEXED = "indexed";
  @VisibleForTesting static final String METADATA_COL_ORDINAL_POSITION = "ordinal_position";
  private static final Logger logger = LoggerFactory.getLogger(JdbcAdmin.class);
  private static final ImmutableMap<RdbEngine, ImmutableMap<DataType, String>> DATA_TYPE_MAPPING =
      ImmutableMap.<RdbEngine, ImmutableMap<DataType, String>>builder()
          .put(
              RdbEngine.MYSQL,
              ImmutableMap.<DataType, String>builder()
                  .put(DataType.INT, "INT")
                  .put(DataType.BIGINT, "BIGINT")
                  .put(DataType.TEXT, "LONGTEXT")
                  .put(DataType.FLOAT, "DOUBLE")
                  .put(DataType.DOUBLE, "DOUBLE")
                  .put(DataType.BOOLEAN, "BOOLEAN")
                  .put(DataType.BLOB, "LONGBLOB")
                  .build())
          .put(
              RdbEngine.POSTGRESQL,
              ImmutableMap.<DataType, String>builder()
                  .put(DataType.INT, "INT")
                  .put(DataType.BIGINT, "BIGINT")
                  .put(DataType.TEXT, "TEXT")
                  .put(DataType.FLOAT, "FLOAT")
                  .put(DataType.DOUBLE, "DOUBLE PRECISION")
                  .put(DataType.BOOLEAN, "BOOLEAN")
                  .put(DataType.BLOB, "BYTEA")
                  .build())
          .put(
              RdbEngine.ORACLE,
              ImmutableMap.<DataType, String>builder()
                  .put(DataType.INT, "INT")
                  .put(DataType.BIGINT, "NUMBER(19)")
                  .put(DataType.TEXT, "VARCHAR2(4000)")
                  .put(DataType.FLOAT, "BINARY_FLOAT")
                  .put(DataType.DOUBLE, "BINARY_DOUBLE")
                  .put(DataType.BOOLEAN, "NUMBER(1)")
                  .put(DataType.BLOB, "RAW(2000)")
                  .build())
          .put(
              RdbEngine.SQL_SERVER,
              ImmutableMap.<DataType, String>builder()
                  .put(DataType.INT, "INT")
                  .put(DataType.BIGINT, "BIGINT")
                  .put(DataType.TEXT, "VARCHAR(8000) COLLATE Latin1_General_BIN")
                  .put(DataType.FLOAT, "FLOAT(24)")
                  .put(DataType.DOUBLE, "FLOAT")
                  .put(DataType.BOOLEAN, "BIT")
                  .put(DataType.BLOB, "VARBINARY(8000)")
                  .build())
          .build();
  private static final ImmutableMap<RdbEngine, ImmutableMap<DataType, String>>
      DATA_TYPE_MAPPING_FOR_KEY =
          ImmutableMap.<RdbEngine, ImmutableMap<DataType, String>>builder()
              .put(
                  RdbEngine.MYSQL,
                  ImmutableMap.<DataType, String>builder()
                      .put(DataType.TEXT, "VARCHAR(64)")
                      .put(DataType.BLOB, "VARBINARY(64)")
                      .build())
              .put(
                  RdbEngine.POSTGRESQL,
                  ImmutableMap.<DataType, String>builder()
                      .put(DataType.TEXT, "VARCHAR(10485760)")
                      .build())
              .put(
                  RdbEngine.ORACLE,
                  ImmutableMap.<DataType, String>builder()
                      .put(DataType.TEXT, "VARCHAR2(64)")
                      .put(DataType.BLOB, "RAW(64)")
                      .build())
              .put(RdbEngine.SQL_SERVER, ImmutableMap.<DataType, String>builder().build())
              .build();
  private static final String INDEX_NAME_PREFIX = "index";

  private final BasicDataSource dataSource;
  private final RdbEngine rdbEngine;
  private final String metadataSchema;

  @Inject
  public JdbcAdmin(DatabaseConfig databaseConfig) {
    JdbcConfig config = new JdbcConfig(databaseConfig);
    dataSource = JdbcUtils.initDataSourceForAdmin(config);
    rdbEngine = JdbcUtils.getRdbEngine(config.getJdbcUrl());
    metadataSchema = config.getTableMetadataSchema().orElse(METADATA_SCHEMA);
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public JdbcAdmin(BasicDataSource dataSource, JdbcConfig config) {
    this.dataSource = dataSource;
    rdbEngine = JdbcUtils.getRdbEngine(config.getJdbcUrl());
    metadataSchema = config.getTableMetadataSchema().orElse(METADATA_SCHEMA);
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  @VisibleForTesting
  JdbcAdmin(BasicDataSource dataSource, RdbEngine rdbEngine, JdbcConfig config) {
    this.dataSource = dataSource;
    this.rdbEngine = rdbEngine;
    metadataSchema = config.getTableMetadataSchema().orElse(METADATA_SCHEMA);
  }

  @Override
  public void createNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    String fullNamespace = enclose(namespace);
    try (Connection connection = dataSource.getConnection()) {
      if (rdbEngine == RdbEngine.ORACLE) {
        execute(connection, "CREATE USER " + fullNamespace + " IDENTIFIED BY \"Oracle1234!@#$\"");
        execute(connection, "ALTER USER " + fullNamespace + " quota unlimited on USERS");
      } else if (rdbEngine == RdbEngine.MYSQL) {
        execute(
            connection, "CREATE SCHEMA " + fullNamespace + " character set utf8 COLLATE utf8_bin");
      } else {
        execute(connection, "CREATE SCHEMA " + fullNamespace);
      }
    } catch (SQLException e) {
      throw new ExecutionException("creating the schema failed", e);
    }
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    try (Connection connection = dataSource.getConnection()) {
      createTableInternal(connection, namespace, table, metadata);
      createIndex(connection, namespace, table, metadata);
      addTableMetadata(connection, namespace, table, metadata, true);
    } catch (SQLException e) {
      throw new ExecutionException(
          "creating the table failed: " + getFullTableName(namespace, table), e);
    }
  }

  private void createTableInternal(
      Connection connection, String schema, String table, TableMetadata metadata)
      throws SQLException {
    String createTableStatement =
        "CREATE TABLE " + enclosedFullTableName(schema, table, rdbEngine) + "(";
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

    boolean hasDescClusteringOrder = hasDescClusteringOrder(metadata);

    // Add primary key definition
    if (hasDescClusteringOrder
        && (rdbEngine == RdbEngine.MYSQL || rdbEngine == RdbEngine.SQL_SERVER)) {
      // For MySQL and SQL Server, specify the clustering orders in the primary key definition
      createTableStatement +=
          ", PRIMARY KEY ("
              + Stream.concat(
                      metadata.getPartitionKeyNames().stream().map(c -> enclose(c) + " ASC"),
                      metadata.getClusteringKeyNames().stream()
                          .map(c -> enclose(c) + " " + metadata.getClusteringOrder(c)))
                  .collect(Collectors.joining(","))
              + "))";
    } else {
      createTableStatement +=
          ", PRIMARY KEY ("
              + Stream.concat(
                      metadata.getPartitionKeyNames().stream(),
                      metadata.getClusteringKeyNames().stream())
                  .map(this::enclose)
                  .collect(Collectors.joining(","))
              + "))";
    }
    if (rdbEngine == RdbEngine.ORACLE) {
      // For Oracle Database, add ROWDEPENDENCIES to the table to improve the performance
      createTableStatement += " ROWDEPENDENCIES";
    }
    execute(connection, createTableStatement);

    if (rdbEngine == RdbEngine.ORACLE) {
      // For Oracle Database, set INITRANS to 3 and MAXTRANS to 255 for the table to improve the
      // performance
      String alterTableStatement =
          "ALTER TABLE "
              + enclosedFullTableName(schema, table, rdbEngine)
              + " INITRANS 3 MAXTRANS 255";
      execute(connection, alterTableStatement);
    }

    if (hasDescClusteringOrder
        && (rdbEngine == RdbEngine.POSTGRESQL || rdbEngine == RdbEngine.ORACLE)) {
      // For PostgreSQL and Oracle, create a unique index for the clustering orders
      String createUniqueIndexStatement =
          "CREATE UNIQUE INDEX "
              + enclose(getFullTableName(schema, table) + "_clustering_order_idx")
              + " ON "
              + enclosedFullTableName(schema, table, rdbEngine)
              + " ("
              + Stream.concat(
                      metadata.getPartitionKeyNames().stream().map(c -> enclose(c) + " ASC"),
                      metadata.getClusteringKeyNames().stream()
                          .map(c -> enclose(c) + " " + metadata.getClusteringOrder(c)))
                  .collect(Collectors.joining(","))
              + ")";
      execute(connection, createUniqueIndexStatement);
    }
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
    switch (rdbEngine) {
      case MYSQL:
      case POSTGRESQL:
        execute(connection, "CREATE SCHEMA IF NOT EXISTS " + enclose(metadataSchema));
        break;
      case SQL_SERVER:
        try {
          execute(connection, "CREATE SCHEMA " + enclose(metadataSchema));
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
              "CREATE USER " + enclose(metadataSchema) + " IDENTIFIED BY \"Oracle1234!@#$\"");
        } catch (SQLException e) {
          // Suppress the exception thrown when the user already exists
          if (e.getErrorCode() != 1920) {
            throw e;
          }
        }
        execute(connection, "ALTER USER " + enclose(metadataSchema) + " quota unlimited on USERS");
        break;
    }
  }

  private void createMetadataTableIfNotExists(Connection connection) throws SQLException {
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

  private String getTextType(int charLength) {
    if (rdbEngine == RdbEngine.ORACLE) {
      return String.format("VARCHAR2(%s)", charLength);
    }
    return String.format("VARCHAR(%s)", charLength);
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
    switch (rdbEngine) {
      case ORACLE:
      case SQL_SERVER:
        return value ? "1" : "0";
      default:
        return value ? "true" : "false";
    }
  }

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    try (Connection connection = dataSource.getConnection()) {
      dropTableInternal(connection, namespace, table);
      deleteTableMetadata(connection, namespace, table);
    } catch (SQLException e) {
      throw new ExecutionException(
          "dropping the table failed: " + getFullTableName(namespace, table), e);
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
    String dropStatement;
    if (rdbEngine == RdbEngine.ORACLE) {
      dropStatement = "DROP USER " + enclose(metadataSchema);
    } else {
      dropStatement = "DROP SCHEMA " + enclose(metadataSchema);
    }

    execute(connection, dropStatement);
  }

  @Override
  public void dropNamespace(String namespace) throws ExecutionException {
    String dropStatement;
    if (rdbEngine == RdbEngine.ORACLE) {
      dropStatement = "DROP USER " + enclose(namespace);
    } else {
      dropStatement = "DROP SCHEMA " + enclose(namespace);
    }

    try (Connection connection = dataSource.getConnection()) {
      execute(connection, dropStatement);
    } catch (SQLException e) {
      throw new ExecutionException(
          String.format(
              "error dropping the %s %s",
              rdbEngine == RdbEngine.ORACLE ? "user" : "schema", namespace),
          e);
    }
  }

  @Override
  public void truncateTable(String namespace, String table) throws ExecutionException {
    String truncateTableStatement = "TRUNCATE TABLE " + encloseFullTableName(namespace, table);
    try (Connection connection = dataSource.getConnection()) {
      execute(connection, truncateTableStatement);
    } catch (SQLException e) {
      throw new ExecutionException(
          "error truncating the table " + getFullTableName(namespace, table), e);
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
              throw new AssertionError("invalid key type: " + keyType);
          }
        }
      }
    } catch (SQLException e) {
      throw new ExecutionException("getting a table metadata failed", e);
    }

    if (!tableExists) {
      return null;
    }

    return builder.build();
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
      if ((rdbEngine == RdbEngine.MYSQL && (e.getErrorCode() == 1049 || e.getErrorCode() == 1146))
          || (rdbEngine == RdbEngine.POSTGRESQL && "42P01".equals(e.getSQLState()))
          || (rdbEngine == RdbEngine.ORACLE && e.getErrorCode() == 942)
          || (rdbEngine == RdbEngine.SQL_SERVER && e.getErrorCode() == 208)) {
        return Collections.emptySet();
      }
      throw new ExecutionException("retrieving the namespace table names failed", e);
    }
  }

  @Override
  public boolean namespaceExists(String namespace) throws ExecutionException {
    String namespaceExistsStatement = "";
    switch (rdbEngine) {
      case POSTGRESQL:
      case MYSQL:
        namespaceExistsStatement =
            "SELECT 1 FROM "
                + encloseFullTableName("information_schema", "schemata")
                + " WHERE "
                + enclose("schema_name")
                + " = ?";
        break;
      case ORACLE:
        namespaceExistsStatement =
            "SELECT 1 FROM " + enclose("ALL_USERS") + " WHERE " + enclose("USERNAME") + " = ?";
        break;
      case SQL_SERVER:
        namespaceExistsStatement =
            "SELECT 1 FROM "
                + encloseFullTableName("sys", "schemas")
                + " WHERE "
                + enclose("name")
                + " = ?";
        break;
    }
    try (Connection connection = dataSource.getConnection();
        PreparedStatement preparedStatement =
            connection.prepareStatement(namespaceExistsStatement)) {
      preparedStatement.setString(1, namespace);
      return preparedStatement.executeQuery().next();
    } catch (SQLException e) {
      throw new ExecutionException("checking if the namespace exists failed", e);
    }
  }

  @Override
  public void close() {
    try {
      dataSource.close();
    } catch (SQLException e) {
      logger.error("failed to close the dataSource", e);
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

    ImmutableMap<DataType, String> typeNameMap = DATA_TYPE_MAPPING.get(rdbEngine);
    assert typeNameMap != null;

    if (keysAndIndexes.contains(columnName)) {
      ImmutableMap<DataType, String> typeNameMapForKey = DATA_TYPE_MAPPING_FOR_KEY.get(rdbEngine);
      assert typeNameMapForKey != null;

      return typeNameMapForKey.getOrDefault(
          scalarDbColumnType, typeNameMap.get(scalarDbColumnType));
    } else {
      return typeNameMap.get(scalarDbColumnType);
    }
  }

  private boolean hasDescClusteringOrder(TableMetadata metadata) {
    return metadata.getClusteringKeyNames().stream()
        .anyMatch(c -> metadata.getClusteringOrder(c) == Order.DESC);
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
      throw new ExecutionException("creating the secondary index failed", e);
    }
  }

  private void alterToIndexColumnTypeIfNecessary(
      Connection connection, String namespace, String table, String columnName)
      throws ExecutionException, SQLException {
    DataType indexType = getTableMetadata(namespace, table).getColumnDataType(columnName);
    ImmutableMap<DataType, String> typeMappingForIndexes = DATA_TYPE_MAPPING_FOR_KEY.get(rdbEngine);
    assert typeMappingForIndexes != null;

    String indexedColumnType = typeMappingForIndexes.get(indexType);
    if (indexedColumnType == null) {
      // The column type does not need to be altered to be compatible with being secondary index
      return;
    }
    String alterColumnStatement =
        getAlterColumnTypeStatement(namespace, table, columnName, indexedColumnType);

    execute(connection, alterColumnStatement);
  }

  private void alterToRegularColumnTypeIfNecessary(
      Connection connection, String namespace, String table, String columnName)
      throws ExecutionException, SQLException {
    DataType indexType = getTableMetadata(namespace, table).getColumnDataType(columnName);
    ImmutableMap<DataType, String> typeMappingForIndexes = DATA_TYPE_MAPPING_FOR_KEY.get(rdbEngine);
    assert typeMappingForIndexes != null;

    if (typeMappingForIndexes.get(indexType) == null) {
      // The column type is already the type for a regular column. It was not altered to be
      // compatible with being a secondary index, so no alteration is necessary.
      return;
    }

    ImmutableMap<DataType, String> typeMappingForColumns = DATA_TYPE_MAPPING.get(rdbEngine);
    assert typeMappingForColumns != null;
    String regularColumnType = typeMappingForColumns.get(indexType);

    String alterColumnStatement =
        getAlterColumnTypeStatement(namespace, table, columnName, regularColumnType);

    execute(connection, alterColumnStatement);
  }

  private String getAlterColumnTypeStatement(
      String namespace, String table, String columnName, String newType) {
    String alterColumnStatement;
    switch (rdbEngine) {
      case MYSQL:
        alterColumnStatement =
            "ALTER TABLE "
                + encloseFullTableName(namespace, table)
                + " MODIFY"
                + enclose(columnName)
                + " "
                + newType;
        break;
      case POSTGRESQL:
        alterColumnStatement =
            "ALTER TABLE "
                + encloseFullTableName(namespace, table)
                + " ALTER COLUMN"
                + enclose(columnName)
                + " TYPE "
                + newType;
        break;
      case ORACLE:
        alterColumnStatement =
            "ALTER TABLE "
                + encloseFullTableName(namespace, table)
                + " MODIFY ( "
                + enclose(columnName)
                + " "
                + newType
                + " )";
        break;
      default:
        throw new AssertionError();
    }
    return alterColumnStatement;
  }

  @Override
  public void dropIndex(String namespace, String table, String columnName)
      throws ExecutionException {
    try (Connection connection = dataSource.getConnection()) {
      dropIndex(connection, namespace, table, columnName);
      alterToRegularColumnTypeIfNecessary(connection, namespace, table, columnName);
      updateTableMetadata(connection, namespace, table, columnName, false);
    } catch (SQLException e) {
      throw new ExecutionException("dropping the secondary index failed", e);
    }
  }

  private boolean tableExistsInternal(Connection connection, String namespace, String table)
      throws ExecutionException {
    String fullTableName = encloseFullTableName(namespace, table);
    String tableExistsStatement;
    switch (rdbEngine) {
      case POSTGRESQL:
      case MYSQL:
        tableExistsStatement = "SELECT 1 FROM " + fullTableName + " LIMIT 1";
        break;
      case ORACLE:
        tableExistsStatement = "SELECT 1 FROM " + fullTableName + " FETCH FIRST 1 ROWS ONLY";
        break;
      case SQL_SERVER:
        tableExistsStatement = "SELECT TOP 1 1 FROM " + fullTableName;
        break;
      default:
        throw new AssertionError();
    }
    try {
      execute(connection, tableExistsStatement);
      return true;
    } catch (SQLException e) {
      // An exception will be thrown if the table does not exist when executing the select
      // query
      if ((rdbEngine == RdbEngine.MYSQL && (e.getErrorCode() == 1049 || e.getErrorCode() == 1146))
          || (rdbEngine == RdbEngine.POSTGRESQL && "42P01".equals(e.getSQLState()))
          || (rdbEngine == RdbEngine.ORACLE && e.getErrorCode() == 942)
          || (rdbEngine == RdbEngine.SQL_SERVER && e.getErrorCode() == 208)) {
        return false;
      }
      throw new ExecutionException(
          String.format("checking if the table %s.%s exists failed", namespace, table), e);
    }
  }

  @Override
  public void repairTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    try (Connection connection = dataSource.getConnection()) {
      if (!tableExistsInternal(connection, namespace, table)) {
        throw new IllegalArgumentException(
            "The table " + getFullTableName(namespace, table) + "  does not exist");
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
          String.format("repairing the table %s.%s failed", namespace, table), e);
    }
  }

  @Override
  public void addNewColumnToTable(
      String namespace, String table, String columnName, DataType columnType)
      throws ExecutionException {
    try {
      TableMetadata currentTableMetadata = getTableMetadata(namespace, table);
      if (currentTableMetadata.getColumnNames().contains(columnName)) {
        throw new IllegalArgumentException(
            String.format("The column %s already exists", columnName));
      }

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

    String dropIndexStatement;
    switch (rdbEngine) {
      case MYSQL:
      case SQL_SERVER:
        dropIndexStatement =
            "DROP INDEX " + enclose(indexName) + " ON " + encloseFullTableName(schema, table);
        break;
      case POSTGRESQL:
        dropIndexStatement = "DROP INDEX " + enclose(schema) + "." + enclose(indexName);
        break;
      case ORACLE:
        dropIndexStatement = "DROP INDEX " + enclose(indexName);
        break;
      default:
        throw new AssertionError();
    }

    execute(connection, dropIndexStatement);
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

  private void execute(Connection connection, String sql) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute(sql);
    }
  }

  private String enclose(String name) {
    return QueryUtils.enclose(name, rdbEngine);
  }

  private String encloseFullTableName(String schema, String table) {
    return enclosedFullTableName(schema, table, rdbEngine);
  }
}
