package com.scalar.db.storage.jdbc;

import static com.scalar.db.storage.jdbc.query.QueryUtils.enclosedFullTableName;
import static com.scalar.db.util.Utility.getFullNamespaceName;
import static com.scalar.db.util.Utility.getFullTableName;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.StorageRuntimeException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.query.QueryUtils;
import com.scalar.db.util.Utility;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class JdbcDatabaseAdmin implements DistributedStorageAdmin {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcDatabaseAdmin.class);
  private static final ImmutableMap<RdbEngine, ImmutableMap<DataType, String>> DATA_TYPE_MAPPING =
      ImmutableMap.<RdbEngine, ImmutableMap<DataType, String>>builder()
          .put(
              RdbEngine.MYSQL,
              ImmutableMap.<DataType, String>builder()
                  .put(DataType.INT, "INT")
                  .put(DataType.BIGINT, "BIGINT")
                  .put(DataType.TEXT, "LONGTEXT")
                  .put(DataType.FLOAT, "FLOAT")
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
                  .put(DataType.BLOB, "BLOB")
                  .build())
          .put(
              RdbEngine.SQL_SERVER,
              ImmutableMap.<DataType, String>builder()
                  .put(DataType.INT, "INT")
                  .put(DataType.BIGINT, "BIGINT")
                  .put(DataType.TEXT, "VARCHAR(8000)")
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
  private final BasicDataSource dataSource;
  private final Optional<String> schemaPrefix;
  private final JdbcTableMetadataManager metadataManager;
  private final RdbEngine rdbEngine;

  @Inject
  public JdbcDatabaseAdmin(JdbcConfig config) {
    dataSource = JdbcUtils.initDataSource(config);
    schemaPrefix = config.getNamespacePrefix();
    rdbEngine = JdbcUtils.getRdbEngine(config.getContactPoints().get(0));
    metadataManager = new JdbcTableMetadataManager(dataSource, schemaPrefix, rdbEngine);
  }

  @VisibleForTesting
  public JdbcDatabaseAdmin(
      BasicDataSource dataSource,
      JdbcTableMetadataManager metadataManager,
      Optional<String> namespacePrefix,
      RdbEngine rdbEngine) {
    this.dataSource = dataSource;
    this.metadataManager = metadataManager;
    this.schemaPrefix = namespacePrefix;
    this.rdbEngine = rdbEngine;
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);
      connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
      try {
        createSchemaIfNotExists(connection, namespace);
        createTableInternal(connection, namespace, table, metadata);
        createIndex(connection, namespace, table, metadata);
      } catch (SQLException e) {
        connection.rollback();
        throw e;
      }
      connection.commit();
    } catch (SQLException e) {
      throw new ExecutionException("creating the table failed", e);
    }
    metadataManager.addTableMetadata(namespace, table, metadata);
  }

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    dropTableInternal(namespace, table);
    metadataManager.deleteTableMetadata(namespace, table);
    if (metadataManager.getTableNames(namespace).isEmpty()) {
      dropSchema(namespace);
    }
  }

  @Override
  public void truncateTable(String namespace, String table) throws ExecutionException {
    String truncateTableStatement =
        "TRUNCATE TABLE "
            + encloseFullTableName(Utility.getFullNamespaceName(schemaPrefix, namespace), table);
    try (Connection connection = dataSource.getConnection()) {
      execute(connection, truncateTableStatement);
    } catch (SQLException e) {
      throw new ExecutionException(
          "error truncating the table " + getFullTableName(schemaPrefix, namespace, table), e);
    }
  }

  @Override
  public TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException {
    try {
      return metadataManager.getTableMetadata(fullNamespace(namespace), table);
    } catch (StorageRuntimeException e) {
      throw new ExecutionException("getting a table metadata failed", e);
    }
  }

  private String fullNamespace(String namespace) {
    return schemaPrefix.map(s -> s + namespace).orElse(namespace);
  }

  @Override
  public void close() {
    try {
      dataSource.getConnection().close();
    } catch (SQLException e) {
      LOGGER.error("failed to close the dataSource", e);
    }
  }

  private void createSchemaIfNotExists(Connection connection, String schema) throws SQLException {
    switch (rdbEngine) {
      case MYSQL:
      case POSTGRESQL:
        execute(
            connection,
            "CREATE SCHEMA IF NOT EXISTS " + enclose(getFullNamespaceName(schemaPrefix, schema)));
        break;
      case SQL_SERVER:
        try {
          execute(
              connection, "CREATE SCHEMA " + enclose(getFullNamespaceName(schemaPrefix, schema)));
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
              "CREATE USER "
                  + enclose(getFullNamespaceName(schemaPrefix, schema))
                  + " IDENTIFIED BY \"oracle\"");
        } catch (SQLException e) {
          // Suppress the exception thrown when the user already exists
          if (e.getErrorCode() != 1920) {
            throw e;
          }
        }
        execute(
            connection,
            "ALTER USER "
                + enclose(getFullNamespaceName(schemaPrefix, schema))
                + " quota unlimited on USERS");
        break;
    }
  }

  @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
  private void createTableInternal(
      Connection connection, String schema, String table, TableMetadata metadata)
      throws SQLException {
    String createTableStatement =
        "CREATE TABLE "
            + enclosedFullTableName(getFullNamespaceName(schemaPrefix, schema), table, rdbEngine)
            + "(";
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
        ", PRIMARY KEY ("
            + Stream.concat(
                    metadata.getPartitionKeyNames().stream(),
                    metadata.getClusteringKeyNames().stream())
                .map(this::enclose)
                .collect(Collectors.joining(","))
            + "))";
    execute(connection, createTableStatement);
  }

  /**
   * Get the vendor DB data type equivalent that is equivalent to the Scalar DB data type
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
    if (keysAndIndexes.contains(columnName)) {
      return DATA_TYPE_MAPPING_FOR_KEY
          .get(rdbEngine)
          .getOrDefault(
              scalarDbColumnType, DATA_TYPE_MAPPING.get(rdbEngine).get(scalarDbColumnType));
    } else {
      return DATA_TYPE_MAPPING.get(rdbEngine).get(scalarDbColumnType);
    }
  }

  private void createIndex(
      Connection connection, String schema, String table, TableMetadata metadata)
      throws SQLException {
    LinkedHashSet<String> sortedIndexedColumns = new LinkedHashSet<>(metadata.getColumnNames());
    sortedIndexedColumns.retainAll(metadata.getSecondaryIndexNames());
    for (String indexedColumn : sortedIndexedColumns) {
      String indexName =
          String.join(
              "_",
              INDEX_NAME_PREFIX,
              getFullNamespaceName(schemaPrefix, schema),
              table,
              indexedColumn);
      String createIndexStatement =
          "CREATE INDEX "
              + indexName
              + " ON "
              + encloseFullTableName(getFullNamespaceName(schemaPrefix, schema), table)
              + " ("
              + enclose(indexedColumn)
              + ")";
      execute(connection, createIndexStatement);
    }
  }

  @SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
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

  private void dropTableInternal(String schema, String table) throws ExecutionException {
    String dropTableStatement =
        "DROP TABLE " + encloseFullTableName(getFullNamespaceName(schemaPrefix, schema), table);
    try (Connection connection = dataSource.getConnection()) {
      execute(connection, dropTableStatement);
    } catch (SQLException e) {
      throw new ExecutionException(
          "error dropping the table " + getFullTableName(schemaPrefix, schema, table), e);
    }
  }

  private void dropSchema(String schema) throws ExecutionException {
    String dropStatement;
    if (rdbEngine == RdbEngine.ORACLE) {
      dropStatement = "DROP USER " + enclose(getFullNamespaceName(schemaPrefix, schema));
    } else {
      dropStatement = "DROP SCHEMA " + enclose(getFullNamespaceName(schemaPrefix, schema));
    }

    try (Connection connection = dataSource.getConnection()) {
      execute(connection, dropStatement);
    } catch (SQLException e) {
      throw new ExecutionException(
          String.format(
              "error dropping the %s %s",
              rdbEngine == RdbEngine.ORACLE ? "user" : "schema",
              getFullNamespaceName(schemaPrefix, schema)),
          e);
    }
  }
}
