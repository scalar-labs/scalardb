package com.scalar.db.storage.jdbc;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.commons.dbcp2.BasicDataSource;

public class JdbcAdminImportTestUtils {
  static final List<String> UNSUPPORTED_DATA_TYPES_MYSQL =
      Arrays.asList(
          "BIGINT UNSIGNED",
          "BIT(8)",
          "DATE",
          "DATETIME",
          "DECIMAL(8,2)",
          "ENUM('a','b')",
          "SET('a','b')",
          "GEOMETRY",
          "JSON", // we remove this for MariaDB because it is an alias of a supported type, LONGTEXT
          "NUMERIC",
          "TIME",
          "TIMESTAMP",
          "YEAR");
  static final List<String> UNSUPPORTED_DATA_TYPES_PGSQL =
      Arrays.asList(
          "bigserial",
          "bit(8)",
          "bit varying(8)",
          "box",
          "cidr",
          "circle",
          "date",
          "inet",
          "interval",
          "json",
          "jsonb",
          "line",
          "lseg",
          "macaddr",
          "macaddr8",
          "money",
          "numeric(8,2)",
          "path",
          "pg_lsn",
          "pg_snapshot", // after v14
          "point",
          "polygon",
          "serial",
          "smallserial",
          "time",
          "time with time zone",
          "timestamp",
          "timestamp with time zone",
          "tsquery",
          "tsvector",
          "txid_snapshot",
          "uuid",
          "xml");
  static final List<String> UNSUPPORTED_DATA_TYPES_ORACLE =
      Arrays.asList(
          "BFILE",
          "DATE",
          "FLOAT(54)",
          "INT",
          "INTERVAL YEAR(3) TO MONTH",
          "INTERVAL DAY(2) TO SECOND",
          "JSON",
          "NUMBER(16,0)",
          "ROWID",
          "TIMESTAMP",
          "TIMESTAMP WITH TIME ZONE",
          "TIMESTAMP WITH LOCAL TIME ZONE",
          "UROWID");
  static final List<String> UNSUPPORTED_DATA_TYPES_MSSQL =
      Arrays.asList(
          "date",
          "datetime",
          "datetime2",
          "datetimeoffset",
          "decimal(8,2)",
          "hierarchyid",
          "money",
          "numeric(8,2)",
          "rowversion",
          "smalldatetime",
          "smallmoney",
          "sql_variant",
          "time",
          "uniqueidentifier",
          "xml");

  private final JdbcConfig config;
  private final RdbEngineStrategy rdbEngine;

  public JdbcAdminImportTestUtils(Properties properties) {
    config = new JdbcConfig(new DatabaseConfig(properties));
    rdbEngine = RdbEngineFactory.create(config);
  }

  public Map<String, TableMetadata> createExistingDatabaseWithAllDataTypes(String namespace)
      throws SQLException {
    Map<String, TableMetadata> results = new HashMap<>();
    List<String> sqls = new ArrayList<>();
    LinkedHashMap<String, String> goodTableColumns;
    TableMetadata goodTableMetadata;
    Map<String, String> badTables;
    if (rdbEngine instanceof RdbEngineMysql) {
      goodTableColumns = prepareColumnsForMysql();
      goodTableMetadata = prepareTableMetadataForMysql();
      if (JdbcEnv.isMariaDB()) {
        badTables =
            prepareCreateNonImportableTableSql(
                namespace,
                UNSUPPORTED_DATA_TYPES_MYSQL.stream()
                    .filter(type -> !type.equalsIgnoreCase("JSON"))
                    .collect(Collectors.toList()));
      } else {
        badTables = prepareCreateNonImportableTableSql(namespace, UNSUPPORTED_DATA_TYPES_MYSQL);
      }
    } else if (rdbEngine instanceof RdbEnginePostgresql) {
      goodTableColumns = prepareColumnsForPostgresql();
      goodTableMetadata = prepareTableMetadataForPostgresql();
      badTables = prepareCreateNonImportableTableSql(namespace, UNSUPPORTED_DATA_TYPES_PGSQL);
    } else if (rdbEngine instanceof RdbEngineOracle) {
      goodTableColumns = prepareColumnsForOracle();
      goodTableMetadata = prepareTableMetadataForOracle();
      badTables = prepareCreateNonImportableTableSql(namespace, UNSUPPORTED_DATA_TYPES_ORACLE);

      // LONG columns must be tested with separated tables since they cannot be coexisted
      TableMetadata longRawMetadata = prepareTableMetadataForOracleForLongRaw();
      sqls.add(
          prepareCreateTableSql(
              namespace,
              "good_table_long_raw",
              prepareColumnsForOracleLongRaw(),
              longRawMetadata.getPartitionKeyNames()));
      results.put("good_table_long_raw", longRawMetadata);
    } else if (rdbEngine instanceof RdbEngineSqlServer) {
      goodTableColumns = prepareColumnsForSqlServer();
      goodTableMetadata = prepareTableMetadataForSqlServer();
      badTables = prepareCreateNonImportableTableSql(namespace, UNSUPPORTED_DATA_TYPES_MSSQL);
    } else {
      throw new RuntimeException();
    }

    // table with all supported columns
    sqls.add(
        prepareCreateTableSql(
            namespace, "good_table", goodTableColumns, goodTableMetadata.getPartitionKeyNames()));
    results.put("good_table", goodTableMetadata);

    // tables with an unsupported column
    badTables.forEach(
        (table, sql) -> {
          sqls.add(sql);
          results.put(table, null);
        });

    execute(sqls.toArray(new String[0]));
    return results;
  }

  public void dropTable(String namespace, String table) throws SQLException {
    String dropTable = "DROP TABLE " + rdbEngine.encloseFullTableName(namespace, table);
    execute(dropTable);
  }

  public void execute(String sql) throws SQLException {
    try (BasicDataSource dataSource = JdbcUtils.initDataSourceForAdmin(config, rdbEngine);
        Connection connection = dataSource.getConnection()) {
      JdbcAdmin.execute(connection, sql);
    }
  }

  public void execute(String[] sql) throws SQLException {
    try (BasicDataSource dataSource = JdbcUtils.initDataSourceForAdmin(config, rdbEngine);
        Connection connection = dataSource.getConnection()) {
      JdbcAdmin.execute(connection, sql);
    }
  }

  private LinkedHashMap<String, String> prepareColumnsForMysql() {
    LinkedHashMap<String, String> columns = new LinkedHashMap<>();
    columns.put("pk1", "INT");
    columns.put("pk2", "INT");
    columns.put("col01", "BOOLEAN");
    columns.put("col02", "INT");
    columns.put("col03", "INT UNSIGNED");
    columns.put("col04", "TINYINT");
    columns.put("col05", "SMALLINT");
    columns.put("col06", "MEDIUMINT");
    columns.put("col07", "BIGINT");
    columns.put("col08", "FLOAT");
    columns.put("col09", "DOUBLE");
    columns.put("col10", "CHAR(8)");
    columns.put("col11", "VARCHAR(512)");
    columns.put("col12", "TEXT");
    columns.put("col13", "TINYTEXT");
    columns.put("col14", "MEDIUMTEXT");
    columns.put("col15", "LONGTEXT");
    columns.put("col16", "VARBINARY(1024)");
    columns.put("col17", "BLOB");
    columns.put("col18", "TINYBLOB");
    columns.put("col19", "MEDIUMBLOB");
    columns.put("col20", "LONGBLOB");
    columns.put("col21", "BINARY(255)");
    if (JdbcEnv.isMariaDB()) {
      columns.put("col22", "JSON");
    }
    return columns;
  }

  private TableMetadata prepareTableMetadataForMysql() {
    TableMetadata.Builder builder =
        TableMetadata.newBuilder()
            .addColumn("pk1", DataType.INT)
            .addColumn("pk2", DataType.INT)
            .addColumn("col01", DataType.BOOLEAN)
            .addColumn("col02", DataType.INT)
            .addColumn("col03", DataType.BIGINT)
            .addColumn("col04", DataType.INT)
            .addColumn("col05", DataType.INT)
            .addColumn("col06", DataType.INT)
            .addColumn("col07", DataType.BIGINT)
            .addColumn("col08", DataType.FLOAT)
            .addColumn("col09", DataType.DOUBLE)
            .addColumn("col10", DataType.TEXT)
            .addColumn("col11", DataType.TEXT)
            .addColumn("col12", DataType.TEXT)
            .addColumn("col13", DataType.TEXT)
            .addColumn("col14", DataType.TEXT)
            .addColumn("col15", DataType.TEXT)
            .addColumn("col16", DataType.BLOB)
            .addColumn("col17", DataType.BLOB)
            .addColumn("col18", DataType.BLOB)
            .addColumn("col19", DataType.BLOB)
            .addColumn("col20", DataType.BLOB)
            .addColumn("col21", DataType.BLOB)
            .addPartitionKey("pk1")
            .addPartitionKey("pk2");
    if (JdbcEnv.isMariaDB()) {
      builder.addColumn("col22", DataType.TEXT);
    }
    return builder.build();
  }

  private LinkedHashMap<String, String> prepareColumnsForPostgresql() {
    LinkedHashMap<String, String> columns = new LinkedHashMap<>();
    columns.put("pk1", "integer");
    columns.put("pk2", "integer");
    columns.put("col01", "boolean");
    columns.put("col02", "smallint");
    columns.put("col03", "integer");
    columns.put("col04", "bigint");
    columns.put("col05", "real");
    columns.put("col06", "double precision");
    columns.put("col07", "char(8)");
    columns.put("col08", "varchar(512)");
    columns.put("col09", "text");
    columns.put("col10", "bytea");
    return columns;
  }

  private TableMetadata prepareTableMetadataForPostgresql() {
    return TableMetadata.newBuilder()
        .addColumn("pk1", DataType.INT)
        .addColumn("pk2", DataType.INT)
        .addColumn("col01", DataType.BOOLEAN)
        .addColumn("col02", DataType.INT)
        .addColumn("col03", DataType.INT)
        .addColumn("col04", DataType.BIGINT)
        .addColumn("col05", DataType.FLOAT)
        .addColumn("col06", DataType.DOUBLE)
        .addColumn("col07", DataType.TEXT)
        .addColumn("col08", DataType.TEXT)
        .addColumn("col09", DataType.TEXT)
        .addColumn("col10", DataType.BLOB)
        .addPartitionKey("pk1")
        .addPartitionKey("pk2")
        .build();
  }

  private LinkedHashMap<String, String> prepareColumnsForOracle() {
    LinkedHashMap<String, String> columns = new LinkedHashMap<>();
    columns.put("pk1", "CHAR(8)");
    columns.put("pk2", "CHAR(8)");
    columns.put("col01", "NUMERIC(15,0)");
    columns.put("col02", "NUMERIC(15,2)");
    columns.put("col03", "FLOAT(53)");
    columns.put("col04", "BINARY_FLOAT");
    columns.put("col05", "BINARY_DOUBLE");
    columns.put("col06", "CHAR(8)");
    columns.put("col07", "VARCHAR2(512)");
    columns.put("col08", "NCHAR(8)");
    columns.put("col09", "NVARCHAR2(512)");
    columns.put("col10", "CLOB");
    columns.put("col11", "NCLOB");
    columns.put("col12", "LONG");
    columns.put("col13", "BLOB");
    columns.put("col14", "RAW(1024)");
    return columns;
  }

  private TableMetadata prepareTableMetadataForOracle() {
    return TableMetadata.newBuilder()
        .addColumn("pk1", DataType.TEXT)
        .addColumn("pk2", DataType.TEXT)
        .addColumn("col01", DataType.BIGINT)
        .addColumn("col02", DataType.DOUBLE)
        .addColumn("col03", DataType.DOUBLE)
        .addColumn("col04", DataType.FLOAT)
        .addColumn("col05", DataType.DOUBLE)
        .addColumn("col06", DataType.TEXT)
        .addColumn("col07", DataType.TEXT)
        .addColumn("col08", DataType.TEXT)
        .addColumn("col09", DataType.TEXT)
        .addColumn("col10", DataType.TEXT)
        .addColumn("col11", DataType.TEXT)
        .addColumn("col12", DataType.TEXT)
        .addColumn("col13", DataType.BLOB)
        .addColumn("col14", DataType.BLOB)
        .addPartitionKey("pk1")
        .addPartitionKey("pk2")
        .build();
  }

  private LinkedHashMap<String, String> prepareColumnsForOracleLongRaw() {
    LinkedHashMap<String, String> columns = new LinkedHashMap<>();
    columns.put("pk1", "CHAR(8)");
    columns.put("pk2", "CHAR(8)");
    columns.put("col", "LONG RAW");
    return columns;
  }

  private TableMetadata prepareTableMetadataForOracleForLongRaw() {
    return TableMetadata.newBuilder()
        .addColumn("pk1", DataType.TEXT)
        .addColumn("pk2", DataType.TEXT)
        .addColumn("col", DataType.BLOB)
        .addPartitionKey("pk1")
        .addPartitionKey("pk2")
        .build();
  }

  private LinkedHashMap<String, String> prepareColumnsForSqlServer() {
    LinkedHashMap<String, String> columns = new LinkedHashMap<>();
    columns.put("pk1", "int");
    columns.put("pk2", "int");
    columns.put("col01", "bit");
    columns.put("col02", "tinyint");
    columns.put("col03", "smallint");
    columns.put("col04", "int");
    columns.put("col05", "bigint");
    columns.put("col06", "real");
    columns.put("col07", "float");
    columns.put("col08", "char(8)");
    columns.put("col09", "varchar(512)");
    columns.put("col10", "nchar(8)");
    columns.put("col11", "nvarchar(512)");
    columns.put("col12", "text");
    columns.put("col13", "ntext");
    columns.put("col14", "binary");
    columns.put("col15", "varbinary");
    columns.put("col16", "image");
    return columns;
  }

  private TableMetadata prepareTableMetadataForSqlServer() {
    return TableMetadata.newBuilder()
        .addColumn("pk1", DataType.INT)
        .addColumn("pk2", DataType.INT)
        .addColumn("col01", DataType.BOOLEAN)
        .addColumn("col02", DataType.INT)
        .addColumn("col03", DataType.INT)
        .addColumn("col04", DataType.INT)
        .addColumn("col05", DataType.BIGINT)
        .addColumn("col06", DataType.FLOAT)
        .addColumn("col07", DataType.DOUBLE)
        .addColumn("col08", DataType.TEXT)
        .addColumn("col09", DataType.TEXT)
        .addColumn("col10", DataType.TEXT)
        .addColumn("col11", DataType.TEXT)
        .addColumn("col12", DataType.TEXT)
        .addColumn("col13", DataType.TEXT)
        .addColumn("col14", DataType.BLOB)
        .addColumn("col15", DataType.BLOB)
        .addColumn("col16", DataType.BLOB)
        .addPartitionKey("pk1")
        .addPartitionKey("pk2")
        .build();
  }

  private Map<String, String> prepareCreateNonImportableTableSql(
      String namespace, List<String> types) {
    Map<String, String> tables = new HashMap<>();
    for (int i = 0; i < types.size(); i++) {
      String table = "bad_table" + i;
      tables.put(table, prepareCreateNonImportableTableSql(namespace, table, types.get(i)));
    }
    return tables;
  }

  private String prepareCreateNonImportableTableSql(String namespace, String table, String type) {
    LinkedHashMap<String, String> columns = new LinkedHashMap<>();
    columns.put("pk", "CHAR(8)");
    columns.put("col", type);
    return prepareCreateTableSql(
        namespace, table, columns, new LinkedHashSet<>(Collections.singletonList("pk")));
  }

  private String prepareCreateTableSql(
      String namespace,
      String table,
      LinkedHashMap<String, String> columns,
      LinkedHashSet<String> primaryKeys) {
    return "CREATE TABLE "
        + rdbEngine.encloseFullTableName(namespace, table)
        + "("
        + columns.entrySet().stream()
            .map(entry -> rdbEngine.enclose(entry.getKey()) + " " + entry.getValue())
            .collect(Collectors.joining(","))
        + ", PRIMARY KEY("
        + primaryKeys.stream().map(rdbEngine::enclose).collect(Collectors.joining(","))
        + "))";
  }
}
