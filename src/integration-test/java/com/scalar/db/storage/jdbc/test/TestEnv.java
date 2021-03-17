package com.scalar.db.storage.jdbc.test;

import com.scalar.db.api.Scan;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.common.metadata.DataType;
import com.scalar.db.storage.jdbc.JdbcDatabaseConfig;
import com.scalar.db.storage.jdbc.JdbcUtils;
import com.scalar.db.storage.jdbc.RdbEngine;
import com.scalar.db.storage.jdbc.metadata.JdbcTableMetadata;
import com.scalar.db.storage.jdbc.metadata.TableMetadataManager;
import com.scalar.db.storage.jdbc.query.QueryUtils;
import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.BasicDataSource;

public class TestEnv implements Closeable {

  private static final Map<RdbEngine, Map<DataType, String>> DATA_TYPE_MAPPING =
      new HashMap<RdbEngine, Map<DataType, String>>() {
        {
          put(
              RdbEngine.MYSQL,
              new HashMap<DataType, String>() {
                {
                  put(DataType.INT, "INT");
                  put(DataType.BIGINT, "BIGINT");
                  put(DataType.TEXT, "LONGTEXT");
                  put(DataType.FLOAT, "FLOAT");
                  put(DataType.DOUBLE, "DOUBLE");
                  put(DataType.BOOLEAN, "BOOLEAN");
                  put(DataType.BLOB, "LONGBLOB");
                }
              });
          put(
              RdbEngine.POSTGRESQL,
              new HashMap<DataType, String>() {
                {
                  put(DataType.INT, "INT");
                  put(DataType.BIGINT, "BIGINT");
                  put(DataType.TEXT, "TEXT");
                  put(DataType.FLOAT, "FLOAT");
                  put(DataType.DOUBLE, "DOUBLE PRECISION");
                  put(DataType.BOOLEAN, "BOOLEAN");
                  put(DataType.BLOB, "BYTEA");
                }
              });
          put(
              RdbEngine.ORACLE,
              new HashMap<DataType, String>() {
                {
                  put(DataType.INT, "INT");
                  put(DataType.BIGINT, "NUMBER(19)");
                  put(DataType.TEXT, "VARCHAR(4000)");
                  put(DataType.FLOAT, "BINARY_FLOAT");
                  put(DataType.DOUBLE, "BINARY_DOUBLE");
                  put(DataType.BOOLEAN, "NUMBER(1)");
                  put(DataType.BLOB, "BLOB");
                }
              });
          put(
              RdbEngine.SQL_SERVER,
              new HashMap<DataType, String>() {
                {
                  put(DataType.INT, "INT");
                  put(DataType.BIGINT, "BIGINT");
                  put(DataType.TEXT, "VARCHAR(8000)");
                  put(DataType.FLOAT, "FLOAT(24)");
                  put(DataType.DOUBLE, "FLOAT");
                  put(DataType.BOOLEAN, "BIT");
                  put(DataType.BLOB, "VARBINARY(8000)");
                }
              });
        }
      };

  private static final Map<RdbEngine, Map<DataType, String>> DATA_TYPE_MAPPING_FOR_KEY =
      new HashMap<RdbEngine, Map<DataType, String>>() {
        {
          put(
              RdbEngine.MYSQL,
              new HashMap<DataType, String>() {
                {
                  put(DataType.TEXT, "VARCHAR(64)");
                  put(DataType.BLOB, "VARBINARY(64)");
                }
              });
          put(
              RdbEngine.POSTGRESQL,
              new HashMap<DataType, String>() {
                {
                  put(DataType.TEXT, "VARCHAR(10485760)");
                }
              });
          put(
              RdbEngine.ORACLE,
              new HashMap<DataType, String>() {
                {
                  put(DataType.TEXT, "VARCHAR(64)");
                  put(DataType.BLOB, "RAW(64)");
                }
              });
          put(RdbEngine.SQL_SERVER, new HashMap<DataType, String>() {});
        }
      };

  private static final String PROP_JDBC_URL = "scalardb.jdbc.url";
  private static final String PROP_JDBC_USERNAME = "scalardb.jdbc.username";
  private static final String PROP_JDBC_PASSWORD = "scalardb.jdbc.password";
  private static final String PROP_NAMESPACE_PREFIX = "scalardb.namespace_prefix";

  private final RdbEngine rdbEngine;
  private final BasicDataSource dataSource;
  private final JdbcDatabaseConfig config;

  private final List<JdbcTableMetadata> metadataList;

  public TestEnv() {
    this(
        System.getProperty(PROP_JDBC_URL),
        System.getProperty(PROP_JDBC_USERNAME, ""),
        System.getProperty(PROP_JDBC_PASSWORD, ""),
        Optional.ofNullable(System.getProperty(PROP_NAMESPACE_PREFIX)));
  }

  public TestEnv(
      String jdbcUrl, String username, String password, Optional<String> namespacePrefix) {
    rdbEngine = JdbcUtils.getRdbEngine(jdbcUrl);

    dataSource = new BasicDataSource();
    dataSource.setUrl(jdbcUrl);
    dataSource.setUsername(username);
    dataSource.setPassword(password);
    dataSource.setMinIdle(5);
    dataSource.setMaxIdle(10);
    dataSource.setMaxTotal(25);

    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, jdbcUrl);
    props.setProperty(DatabaseConfig.USERNAME, username);
    props.setProperty(DatabaseConfig.PASSWORD, password);
    props.setProperty(DatabaseConfig.STORAGE, "jdbc");
    namespacePrefix.ifPresent(s -> props.setProperty(DatabaseConfig.NAMESPACE_PREFIX, s));
    config = new JdbcDatabaseConfig(props);

    metadataList = new ArrayList<>();
  }

  public void register(
      String schema,
      String table,
      List<String> partitionKeyNames,
      List<String> clusteringKeyNames,
      Map<String, Scan.Ordering.Order> clusteringOrders,
      Map<String, DataType> columnDataTypes,
      List<String> secondaryIndexNames,
      Map<String, Scan.Ordering.Order> secondaryIndexOrders) {
    metadataList.add(
        new JdbcTableMetadata(
            namespacePrefix() + schema,
            table,
            partitionKeyNames,
            clusteringKeyNames,
            clusteringOrders,
            columnDataTypes,
            secondaryIndexNames,
            secondaryIndexOrders));
  }

  public void register(
      String schema,
      String table,
      List<String> partitionKeyNames,
      List<String> clusteringKeyNames,
      Map<String, Scan.Ordering.Order> clusteringOrders,
      Map<String, DataType> columnDataTypes) {
    register(
        schema,
        table,
        partitionKeyNames,
        clusteringKeyNames,
        clusteringOrders,
        columnDataTypes,
        new ArrayList<>(),
        new HashMap<>());
  }

  private String namespacePrefix() {
    return config.getNamespacePrefix().orElse("");
  }

  private void execute(String sql) throws SQLException {
    try (Connection connection = dataSource.getConnection();
        Statement stmt = connection.createStatement()) {
      stmt.execute(sql);
    }
  }

  private String enclose(String name) {
    return QueryUtils.enclose(name, rdbEngine);
  }

  private String enclosedFullTableName(String schema, String table) {
    return QueryUtils.enclosedFullTableName(schema, table, rdbEngine);
  }

  private String getMetadataSchema() {
    return namespacePrefix() + TableMetadataManager.SCHEMA;
  }

  private String enclosedMetadataTableName() {
    return enclosedFullTableName(getMetadataSchema(), TableMetadataManager.TABLE);
  }

  private void createSchema(String schema) throws SQLException {
    if (rdbEngine == RdbEngine.ORACLE) {
      execute("CREATE USER " + enclose(schema) + " IDENTIFIED BY \"oracle\"");
      execute("ALTER USER " + enclose(schema) + " quota unlimited on USERS");
    } else {
      execute("CREATE SCHEMA " + enclose(schema));
    }
  }

  private void dropSchema(String schema) throws SQLException {
    if (rdbEngine == RdbEngine.ORACLE) {
      execute("DROP USER " + enclose(schema) + " CASCADE");
    } else {
      execute("DROP SCHEMA " + enclose(schema));
    }
  }

  private void createTable(JdbcTableMetadata metadata) throws SQLException {
    execute(
        "CREATE TABLE "
            + enclosedFullTableName(metadata.getSchema(), metadata.getTable())
            + "("
            + metadata.getColumnNames().stream()
                .map(c -> enclose(c) + " " + getColumnDataType(c, metadata))
                .collect(Collectors.joining(","))
            + ",PRIMARY KEY ("
            + Stream.concat(
                    metadata.getPartitionKeyNames().stream(),
                    metadata.getClusteringKeyNames().stream())
                .map(this::enclose)
                .collect(Collectors.joining(","))
            + "))");
  }

  private String getColumnDataType(String column, JdbcTableMetadata metadata) {
    boolean isPrimaryKey =
        metadata.getPartitionKeyNames().contains(column)
            || metadata.getClusteringKeyNames().contains(column);
    boolean isIndexed = metadata.getSecondaryIndexNames().contains(column);

    DataType dataType = metadata.getColumnDataType(column);
    if (isPrimaryKey || isIndexed) {
      return getDataTypeMappingForKey().getOrDefault(dataType, getDataTypeMapping().get(dataType));
    }

    return getDataTypeMapping().get(dataType);
  }

  private Map<DataType, String> getDataTypeMappingForKey() {
    return DATA_TYPE_MAPPING_FOR_KEY.get(rdbEngine);
  }

  private Map<DataType, String> getDataTypeMapping() {
    return DATA_TYPE_MAPPING.get(rdbEngine);
  }

  private void createIndex(JdbcTableMetadata metadata) throws SQLException {
    for (String indexedColumn : metadata.getSecondaryIndexNames()) {
      String indexName =
          "index_" + metadata.getSchema() + "_" + metadata.getTable() + "_" + indexedColumn;
      execute(
          "CREATE INDEX "
              + enclose(indexName)
              + " ON "
              + enclosedFullTableName(metadata.getSchema(), metadata.getTable())
              + "("
              + enclose(indexedColumn)
              + ")");
    }
  }

  public void insertMetadata() throws SQLException {
    for (JdbcTableMetadata metadata : metadataList) {
      insertMetadata(metadata);
    }
  }

  private void insertMetadata(JdbcTableMetadata metadata) throws SQLException {
    int ordinalPosition = 1;
    for (String partitionKeyName : metadata.getPartitionKeyNames()) {
      insertMetadata(partitionKeyName, ordinalPosition++, metadata);
    }
    for (String clusteringKeyName : metadata.getClusteringKeyNames()) {
      insertMetadata(clusteringKeyName, ordinalPosition++, metadata);
    }
    for (String column : metadata.getColumnNames()) {
      if (metadata.getPartitionKeyNames().contains(column)
          || metadata.getClusteringKeyNames().contains(column)) {
        continue;
      }
      insertMetadata(column, ordinalPosition++, metadata);
    }
  }

  private void insertMetadata(String column, int ordinalPosition, JdbcTableMetadata metadata)
      throws SQLException {
    String keyType = getKeyType(column, metadata);
    Scan.Ordering.Order keyOrder = metadata.getClusteringOrder(column);
    Scan.Ordering.Order indexOrder = metadata.getSecondaryIndexOrder(column);
    execute(
        String.format(
            "INSERT INTO %s VALUES('%s','%s','%s',%s,%s,%s,%s,%d)",
            enclosedMetadataTableName(),
            metadata.getFullTableName(),
            column,
            metadata.getColumnDataType(column),
            keyType != null ? "'" + keyType + "'" : "NULL",
            keyOrder != null ? "'" + keyOrder + "'" : "NULL",
            booleanValue(metadata.getSecondaryIndexNames().contains(column)),
            indexOrder != null ? "'" + indexOrder + "'" : "NULL",
            ordinalPosition));
  }

  private String getKeyType(String column, JdbcTableMetadata metadata) {
    if (metadata.getPartitionKeyNames().contains(column)) {
      return "PARTITION";
    } else if (metadata.getClusteringKeyNames().contains(column)) {
      return "CLUSTERING";
    }
    return null;
  }

  private String booleanValue(boolean value) {
    switch (rdbEngine) {
      case ORACLE:
      case SQL_SERVER:
        return value ? "1" : "0";
      default:
        return value ? "true" : "false";
    }
  }

  private void dropTable(JdbcTableMetadata metadata) throws SQLException {
    execute("DROP TABLE " + enclosedFullTableName(metadata.getSchema(), metadata.getTable()));
  }

  public void createMetadataTable() throws SQLException {
    createSchema(getMetadataSchema());

    // create the metadata table
    execute(
        "CREATE TABLE "
            + enclosedMetadataTableName()
            + "("
            + enclose("full_table_name")
            + " VARCHAR(128),"
            + enclose("column_name")
            + " VARCHAR(128),"
            + enclose("data_type")
            + " VARCHAR(20) NOT NULL,"
            + enclose("key_type")
            + " VARCHAR(20),"
            + enclose("clustering_order")
            + " VARCHAR(10),"
            + enclose("indexed")
            + " "
            + booleanType()
            + " NOT NULL,"
            + enclose("index_order")
            + " VARCHAR(10),"
            + enclose("ordinal_position")
            + " INTEGER NOT NULL,"
            + "PRIMARY KEY ("
            + enclose("full_table_name")
            + ", "
            + enclose("column_name")
            + "))");
  }

  private String booleanType() {
    switch (rdbEngine) {
      case ORACLE:
        return "NUMBER(1)";
      case SQL_SERVER:
        return "BIT";
      default:
        return "BOOLEAN";
    }
  }

  public void dropMetadataTable() throws SQLException {
    // drop the metadata table
    execute("DROP TABLE " + enclosedMetadataTableName());

    dropSchema(getMetadataSchema());
  }

  public void createTables() throws SQLException {
    Set<String> schemas =
        metadataList.stream().map(JdbcTableMetadata::getSchema).collect(Collectors.toSet());
    for (String schema : schemas) {
      createSchema(schema);
    }

    for (JdbcTableMetadata metadata : metadataList) {
      createTable(metadata);
    }

    for (JdbcTableMetadata metadata : metadataList) {
      createIndex(metadata);
    }
  }

  public void deleteTableData() throws SQLException {
    for (JdbcTableMetadata metadata : metadataList) {
      deleteTableData(metadata);
    }
  }

  private void deleteTableData(JdbcTableMetadata metadata) throws SQLException {
    execute("DELETE FROM " + enclosedFullTableName(metadata.getSchema(), metadata.getTable()));
  }

  public void dropTables() throws SQLException {
    for (JdbcTableMetadata metadata : metadataList) {
      dropTable(metadata);
    }

    Set<String> schemas =
        metadataList.stream().map(JdbcTableMetadata::getSchema).collect(Collectors.toSet());
    for (String schema : schemas) {
      dropSchema(schema);
    }
  }

  public DataSource getDataSource() {
    return dataSource;
  }

  public JdbcDatabaseConfig getJdbcDatabaseConfig() {
    return config;
  }

  public RdbEngine getRdbEngine() {
    return rdbEngine;
  }

  @Override
  public void close() throws IOException {
    try {
      dataSource.close();
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }
}
