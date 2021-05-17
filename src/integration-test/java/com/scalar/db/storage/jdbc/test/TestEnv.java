package com.scalar.db.storage.jdbc.test;

import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.JdbcDatabaseConfig;
import com.scalar.db.storage.jdbc.JdbcTableMetadataManager;
import com.scalar.db.storage.jdbc.JdbcUtils;
import com.scalar.db.storage.jdbc.RdbEngine;
import com.scalar.db.storage.jdbc.query.QueryUtils;
import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
                  put(DataType.TEXT, "VARCHAR2(4000)");
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
                  put(DataType.TEXT, "VARCHAR2(64)");
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

  private final List<String> schemaList;
  private final List<String> tableList;
  private final List<TableMetadata> metadataList;

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

    schemaList = new ArrayList<>();
    tableList = new ArrayList<>();
    metadataList = new ArrayList<>();
  }

  public void register(String schema, String table, TableMetadata metadata) {
    schemaList.add(schema);
    tableList.add(table);
    metadataList.add(metadata);
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
    return namespacePrefix() + JdbcTableMetadataManager.SCHEMA;
  }

  private String enclosedMetadataTableName() {
    return enclosedFullTableName(getMetadataSchema(), JdbcTableMetadataManager.TABLE);
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

  private void createTable(String schema, String table, TableMetadata metadata)
      throws SQLException {
    execute(
        "CREATE TABLE "
            + enclosedFullTableName(schema, table)
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

  private String getColumnDataType(String column, TableMetadata metadata) {
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

  private void createIndex(String schema, String table, TableMetadata metadata)
      throws SQLException {
    for (String indexedColumn : metadata.getSecondaryIndexNames()) {
      String indexName = "index_" + schema + "_" + table + "_" + indexedColumn;
      execute(
          "CREATE INDEX "
              + enclose(indexName)
              + " ON "
              + enclosedFullTableName(schema, table)
              + "("
              + enclose(indexedColumn)
              + ")");
    }
  }

  public void insertMetadata() throws SQLException {
    for (int i = 0; i < metadataList.size(); i++) {
      insertMetadata(schemaList.get(i), tableList.get(i), metadataList.get(i));
    }
  }

  private void insertMetadata(String schema, String table, TableMetadata metadata)
      throws SQLException {
    int ordinalPosition = 1;
    for (String partitionKeyName : metadata.getPartitionKeyNames()) {
      insertMetadata(partitionKeyName, ordinalPosition++, schema, table, metadata);
    }
    for (String clusteringKeyName : metadata.getClusteringKeyNames()) {
      insertMetadata(clusteringKeyName, ordinalPosition++, schema, table, metadata);
    }
    for (String column : metadata.getColumnNames()) {
      if (metadata.getPartitionKeyNames().contains(column)
          || metadata.getClusteringKeyNames().contains(column)) {
        continue;
      }
      insertMetadata(column, ordinalPosition++, schema, table, metadata);
    }
  }

  private void insertMetadata(
      String column, int ordinalPosition, String schema, String table, TableMetadata metadata)
      throws SQLException {
    String keyType = getKeyType(column, metadata);
    Scan.Ordering.Order keyOrder = metadata.getClusteringOrder(column);
    execute(
        String.format(
            "INSERT INTO %s VALUES('%s','%s','%s',%s,%s,%s,%d)",
            enclosedMetadataTableName(),
            schema + "." + table,
            column,
            metadata.getColumnDataType(column),
            keyType != null ? "'" + keyType + "'" : "NULL",
            keyOrder != null ? "'" + keyOrder + "'" : "NULL",
            booleanValue(metadata.getSecondaryIndexNames().contains(column)),
            ordinalPosition));
  }

  private String getKeyType(String column, TableMetadata metadata) {
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

  private void dropTable(String schema, String table) throws SQLException {
    execute("DROP TABLE " + enclosedFullTableName(schema, table));
  }

  public void createMetadataTable() throws SQLException {
    createSchema(getMetadataSchema());

    // create the metadata table
    execute(
        "CREATE TABLE "
            + enclosedMetadataTableName()
            + "("
            + enclose("full_table_name")
            + " "
            + textType(128)
            + ","
            + enclose("column_name")
            + " "
            + textType(128)
            + ","
            + enclose("data_type")
            + " "
            + textType(20)
            + " NOT NULL,"
            + enclose("key_type")
            + " "
            + textType(20)
            + ","
            + enclose("clustering_order")
            + " "
            + textType(10)
            + ","
            + enclose("indexed")
            + " "
            + booleanType()
            + " NOT NULL,"
            + enclose("ordinal_position")
            + " INTEGER NOT NULL,"
            + "PRIMARY KEY ("
            + enclose("full_table_name")
            + ", "
            + enclose("column_name")
            + "))");
  }

  private String textType(int length) {
    String textType;
    switch (rdbEngine) {
      case ORACLE:
        textType = "VARCHAR2";
        break;
      default:
        textType = "VARCHAR";
        break;
    }
    return textType + "(" + length + ")";
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
    Set<String> schemas = new HashSet<>(schemaList);
    for (String schema : schemas) {
      createSchema(schema);
    }

    for (int i = 0; i < metadataList.size(); i++) {
      createTable(schemaList.get(i), tableList.get(i), metadataList.get(i));
    }

    for (int i = 0; i < metadataList.size(); i++) {
      createIndex(schemaList.get(i), tableList.get(i), metadataList.get(i));
    }
  }

  public void deleteTableData() throws SQLException {
    for (int i = 0; i < metadataList.size(); i++) {
      deleteTableData(schemaList.get(i), tableList.get(i));
    }
  }

  private void deleteTableData(String schema, String table) throws SQLException {
    execute("DELETE FROM " + enclosedFullTableName(schema, table));
  }

  public void dropTables() throws SQLException {
    for (int i = 0; i < metadataList.size(); i++) {
      dropTable(schemaList.get(i), tableList.get(i));
    }

    Set<String> schemas = new HashSet<>(schemaList);
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
