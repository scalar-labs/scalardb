package com.scalar.db.storage.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.DistributedStorageAdminImportTableIntegrationTestBase.TestData;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.InsertBuilder;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.commons.dbcp2.BasicDataSource;

public class JdbcAdminImportTestUtils {
  static final String SUPPORTED_TABLE_NAME = "supported_table";
  static final List<String> UNSUPPORTED_DATA_TYPES_MYSQL =
      Arrays.asList(
          "BIGINT UNSIGNED",
          "BIT(8)",
          "DECIMAL(8,2)",
          "ENUM('a','b')",
          "SET('a','b')",
          "GEOMETRY",
          "JSON", // we remove this for MariaDB because it is an alias of a supported type, LONGTEXT
          "NUMERIC",
          "YEAR");
  static final List<String> UNSUPPORTED_DATA_TYPES_PGSQL =
      Arrays.asList(
          "bigserial",
          "bit(8)",
          "bit varying(8)",
          "box",
          "cidr",
          "circle",
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
          "point",
          "polygon",
          "serial",
          "smallserial",
          "time with time zone",
          "tsquery",
          "tsvector",
          "txid_snapshot",
          "uuid",
          "xml");
  static final List<String> UNSUPPORTED_DATA_TYPES_PGSQL_V13_OR_LATER =
      Collections.singletonList("pg_snapshot");
  static final List<String> UNSUPPORTED_DATA_TYPES_ORACLE =
      Arrays.asList(
          "BFILE",
          "FLOAT(54)",
          "INT",
          "INTERVAL YEAR(3) TO MONTH",
          "INTERVAL DAY(2) TO SECOND",
          "NUMBER(16,0)",
          "ROWID",
          "UROWID");
  static final List<String> UNSUPPORTED_DATA_TYPES_ORACLE_V20_OR_LATER =
      Collections.singletonList("JSON");
  static final List<String> UNSUPPORTED_DATA_TYPES_MSSQL =
      Arrays.asList(
          "decimal(8,2)",
          "hierarchyid",
          "money",
          "numeric(8,2)",
          "rowversion",
          "smallmoney",
          "sql_variant",
          "uniqueidentifier",
          "xml",
          "geometry",
          "geography");

  private final RdbEngineStrategy rdbEngine;
  private final int majorVersion;
  private final BasicDataSource dataSource;

  public JdbcAdminImportTestUtils(Properties properties) {
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    rdbEngine = RdbEngineFactory.create(config);
    dataSource = JdbcUtils.initDataSourceForAdmin(config, rdbEngine);
    majorVersion = getMajorVersion();
  }

  // For the SpotBugs warning CT_CONSTRUCTOR_THROW
  @Override
  protected final void finalize() {}

  public List<TestData> createExistingDatabaseWithAllDataTypes(String namespace)
      throws SQLException {
    execute(rdbEngine.createSchemaSqls(namespace));
    if (JdbcTestUtils.isMysql(rdbEngine)) {
      return createExistingMysqlDatabaseWithAllDataTypes(namespace);
    } else if (JdbcTestUtils.isPostgresql(rdbEngine)) {
      return createExistingPostgresDatabaseWithAllDataTypes(namespace);
    } else if (JdbcTestUtils.isOracle(rdbEngine)) {
      return createExistingOracleDatabaseWithAllDataTypes(namespace);
    } else if (JdbcTestUtils.isSqlServer(rdbEngine)) {
      return createExistingSqlServerDatabaseWithAllDataTypes(namespace);
    } else {
      throw new RuntimeException();
    }
  }

  public void dropTable(String namespace, String table) throws SQLException {
    String dropTable = "DROP TABLE " + rdbEngine.encloseFullTableName(namespace, table);
    execute(dropTable);
  }

  public void execute(String sql) throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      JdbcAdmin.execute(connection, sql);
    }
  }

  public void execute(String[] sql) throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
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
    columns.put("col21", "BINARY(5)");
    columns.put("col22", "DATE");
    columns.put("col23", "TIME(6)");
    columns.put("col24", "DATETIME(6)");
    columns.put("col25", "DATETIME(6)"); // override to TIMESTAMPTZ
    // With MySQL 5.7, if a TIMESTAMP column is not explicitly declared with the NULL attribute, it
    // is declared automatically with the NOT NULL attribute and assigned a default value equal to
    // the current timestamp.
    // cf. the "--explicit-defaults-for-timestamp" option documentation
    columns.put("col26", "TIMESTAMP(6) NULL");
    if (isMariaDB()) {
      columns.put("col27", "JSON");
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
            .addColumn("col22", DataType.DATE)
            .addColumn("col23", DataType.TIME)
            .addColumn("col24", DataType.TIMESTAMP)
            .addColumn("col25", DataType.TIMESTAMPTZ)
            .addColumn("col26", DataType.TIMESTAMPTZ)
            .addPartitionKey("pk1")
            .addPartitionKey("pk2");
    if (isMariaDB()) {
      builder.addColumn("col27", DataType.TEXT);
    }
    return builder.build();
  }

  private Map<String, Column<?>> prepareInsertColumnsForMysql(TableMetadata metadata) {
    ImmutableList.Builder<Column<?>> customColumns = new ImmutableList.Builder<>();
    customColumns.add(
        TimestampTZColumn.of(
            "col26", LocalDateTime.of(2005, 10, 11, 8, 35).toInstant(ZoneOffset.UTC)));
    if (isMariaDB()) {
      customColumns.add(TextColumn.of("col27", "{\"foo\": \"bar\"}"));
    }
    return prepareInsertColumnsWithGenericAndCustomValues(metadata, customColumns.build());
  }

  private Map<String, DataType> prepareOverrideColumnsTypeForMysql() {
    return ImmutableMap.of("col25", DataType.TIMESTAMPTZ);
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
    columns.put("col07", "char(3)");
    columns.put("col08", "varchar(512)");
    columns.put("col09", "text");
    columns.put("col10", "bytea");
    columns.put("col11", "date");
    columns.put("col12", "time");
    columns.put("col13", "timestamp");
    columns.put("col14", "timestamp with time zone");
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
        .addColumn("col11", DataType.DATE)
        .addColumn("col12", DataType.TIME)
        .addColumn("col13", DataType.TIMESTAMP)
        .addColumn("col14", DataType.TIMESTAMPTZ)
        .addPartitionKey("pk1")
        .addPartitionKey("pk2")
        .build();
  }

  private LinkedHashMap<String, String> prepareColumnsForOracle() {
    LinkedHashMap<String, String> columns = new LinkedHashMap<>();
    columns.put("pk1", "CHAR(3)");
    columns.put("pk2", "CHAR(3)");
    columns.put("col01", "NUMERIC(15,0)");
    columns.put("col02", "NUMERIC(15,2)");
    columns.put("col03", "FLOAT(53)");
    columns.put("col04", "BINARY_FLOAT");
    columns.put("col05", "BINARY_DOUBLE");
    columns.put("col06", "CHAR(3)");
    columns.put("col07", "VARCHAR2(512)");
    columns.put("col08", "NCHAR(3)");
    columns.put("col09", "NVARCHAR2(512)");
    columns.put("col10", "CLOB");
    columns.put("col11", "NCLOB");
    columns.put("col12", "LONG");
    columns.put("col13", "BLOB");
    columns.put("col14", "RAW(1024)");
    columns.put("col15", "DATE");
    columns.put("col16", "DATE"); // override to TIME
    columns.put("col17", "DATE"); // override to TIMESTAMP
    columns.put("col18", "TIMESTAMP");
    columns.put("col19", "TIMESTAMP"); // override to TIME
    columns.put("col20", "TIMESTAMP WITH TIME ZONE");
    columns.put("col21", "TIMESTAMP WITH LOCAL TIME ZONE");
    return columns;
  }

  private Map<String, DataType> prepareOverrideColumnsTypeForOracle() {
    return ImmutableMap.of(
        "col16", DataType.TIME, "col17", DataType.TIMESTAMP, "col19", DataType.TIME);
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
        .addColumn("col15", DataType.DATE)
        .addColumn("col16", DataType.TIME)
        .addColumn("col17", DataType.TIMESTAMP)
        .addColumn("col18", DataType.TIMESTAMP)
        .addColumn("col19", DataType.TIME)
        .addColumn("col20", DataType.TIMESTAMPTZ)
        .addColumn("col21", DataType.TIMESTAMPTZ)
        .addPartitionKey("pk1")
        .addPartitionKey("pk2")
        .build();
  }

  private Map<String, Column<?>> prepareInsertColumnsForOracle(TableMetadata metadata) {
    List<Column<?>> customColumns =
        ImmutableList.of(
            TimeColumn.of("col16", LocalTime.of(11, 8, 35)),
            TimestampColumn.of("col17", LocalDateTime.of(1905, 10, 11, 8, 35)));
    return prepareInsertColumnsWithGenericAndCustomValues(metadata, customColumns);
  }

  private LinkedHashMap<String, String> prepareColumnsForOracleLongRaw() {
    LinkedHashMap<String, String> columns = new LinkedHashMap<>();
    columns.put("pk1", "CHAR(3)");
    columns.put("pk2", "CHAR(3)");
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
    columns.put("col08", "char(3)");
    columns.put("col09", "varchar(512)");
    columns.put("col10", "nchar(3)");
    columns.put("col11", "nvarchar(512)");
    columns.put("col12", "text");
    columns.put("col13", "ntext");
    columns.put("col14", "binary(5)");
    columns.put("col15", "varbinary(5)");
    columns.put("col16", "image");
    columns.put("col17", "date");
    columns.put("col18", "time");
    columns.put("col19", "datetime");
    columns.put("col20", "datetime2");
    columns.put("col21", "smalldatetime");
    columns.put("col22", "datetimeoffset");

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
        .addColumn("col17", DataType.DATE)
        .addColumn("col18", DataType.TIME)
        .addColumn("col19", DataType.TIMESTAMP)
        .addColumn("col20", DataType.TIMESTAMP)
        .addColumn("col21", DataType.TIMESTAMP)
        .addColumn("col22", DataType.TIMESTAMPTZ)
        .addPartitionKey("pk1")
        .addPartitionKey("pk2")
        .build();
  }

  private Map<String, Column<?>> prepareInsertColumnsForSqlServer(TableMetadata metadata) {
    List<Column<?>> customColumns =
        ImmutableList.of(
            TimestampColumn.of("col19", LocalDateTime.of(1905, 10, 11, 8, 35, 14, 123_000_000)),
            TimestampColumn.of("col21", LocalDateTime.of(1905, 10, 11, 8, 35)));
    return prepareInsertColumnsWithGenericAndCustomValues(metadata, customColumns);
  }

  private List<JdbcTestData> prepareCreateNonImportableTableSql(
      String namespace, List<String> types) {
    ImmutableList.Builder<JdbcTestData> data = new ImmutableList.Builder<>();
    for (int i = 0; i < types.size(); i++) {
      String table = "bad_table" + i;
      data.add(
          JdbcTestData.createNonImportableTable(
              table, prepareCreateNonImportableTableSql(namespace, table, types.get(i))));
    }
    return data.build();
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

  private List<TestData> createExistingMysqlDatabaseWithAllDataTypes(String namespace)
      throws SQLException {
    List<JdbcTestData> data = new ArrayList<>();
    TableMetadata tableMetadata = prepareTableMetadataForMysql();
    String sql =
        prepareCreateTableSql(
            namespace,
            SUPPORTED_TABLE_NAME,
            prepareColumnsForMysql(),
            tableMetadata.getPartitionKeyNames());
    data.add(
        JdbcTestData.createImportableTable(
            SUPPORTED_TABLE_NAME,
            sql,
            tableMetadata,
            prepareOverrideColumnsTypeForMysql(),
            prepareInsertColumnsForMysql(tableMetadata)));

    if (isMariaDB()) {
      data.addAll(
          prepareCreateNonImportableTableSql(
              namespace,
              UNSUPPORTED_DATA_TYPES_MYSQL.stream()
                  .filter(type -> !type.equalsIgnoreCase("JSON"))
                  .collect(Collectors.toList())));
    } else {
      data.addAll(prepareCreateNonImportableTableSql(namespace, UNSUPPORTED_DATA_TYPES_MYSQL));
    }

    executeCreateTableSql(data);

    return ImmutableList.copyOf(data);
  }

  private List<TestData> createExistingPostgresDatabaseWithAllDataTypes(String namespace)
      throws SQLException {
    List<JdbcTestData> data = new ArrayList<>();

    TableMetadata tableMetadata = prepareTableMetadataForPostgresql();
    String sql =
        prepareCreateTableSql(
            namespace,
            SUPPORTED_TABLE_NAME,
            prepareColumnsForPostgresql(),
            tableMetadata.getPartitionKeyNames());
    data.add(
        JdbcTestData.createImportableTable(
            SUPPORTED_TABLE_NAME,
            sql,
            tableMetadata,
            Collections.emptyMap(),
            prepareInsertColumnsWithGenericValues(tableMetadata)));

    data.addAll(
        prepareCreateNonImportableTableSql(
            namespace,
            majorVersion >= 13
                ? Stream.concat(
                        UNSUPPORTED_DATA_TYPES_PGSQL.stream(),
                        UNSUPPORTED_DATA_TYPES_PGSQL_V13_OR_LATER.stream())
                    .collect(Collectors.toList())
                : UNSUPPORTED_DATA_TYPES_PGSQL));

    executeCreateTableSql(data);

    return ImmutableList.copyOf(data);
  }

  private List<TestData> createExistingOracleDatabaseWithAllDataTypes(String namespace)
      throws SQLException {
    List<JdbcTestData> data = new ArrayList<>();

    TableMetadata tableMetadata = prepareTableMetadataForOracle();
    String sql =
        prepareCreateTableSql(
            namespace,
            SUPPORTED_TABLE_NAME,
            prepareColumnsForOracle(),
            tableMetadata.getPartitionKeyNames());
    data.add(
        JdbcTestData.createImportableTable(
            SUPPORTED_TABLE_NAME,
            sql,
            tableMetadata,
            prepareOverrideColumnsTypeForOracle(),
            prepareInsertColumnsForOracle(tableMetadata)));

    // LONG columns must be tested with separated tables since they cannot be coexisted
    TableMetadata longRawTableMetadata = prepareTableMetadataForOracleForLongRaw();
    String longRawSupportedTable = SUPPORTED_TABLE_NAME + "_long_raw";
    String longRawSql =
        prepareCreateTableSql(
            namespace,
            longRawSupportedTable,
            prepareColumnsForOracleLongRaw(),
            longRawTableMetadata.getPartitionKeyNames());
    data.add(
        JdbcTestData.createImportableTable(
            longRawSupportedTable,
            longRawSql,
            longRawTableMetadata,
            Collections.emptyMap(),
            prepareInsertColumnsWithGenericValues(longRawTableMetadata)));

    data.addAll(
        prepareCreateNonImportableTableSql(
            namespace,
            majorVersion >= 20
                ? Stream.concat(
                        UNSUPPORTED_DATA_TYPES_ORACLE.stream(),
                        UNSUPPORTED_DATA_TYPES_ORACLE_V20_OR_LATER.stream())
                    .collect(Collectors.toList())
                : UNSUPPORTED_DATA_TYPES_ORACLE));

    executeCreateTableSql(data);

    return ImmutableList.copyOf(data);
  }

  private List<TestData> createExistingSqlServerDatabaseWithAllDataTypes(String namespace)
      throws SQLException {
    List<JdbcTestData> data = new ArrayList<>();

    TableMetadata tableMetadata = prepareTableMetadataForSqlServer();
    String sql =
        prepareCreateTableSql(
            namespace,
            SUPPORTED_TABLE_NAME,
            prepareColumnsForSqlServer(),
            tableMetadata.getPartitionKeyNames());
    data.add(
        JdbcTestData.createImportableTable(
            SUPPORTED_TABLE_NAME,
            sql,
            tableMetadata,
            Collections.emptyMap(),
            prepareInsertColumnsForSqlServer(tableMetadata)));

    data.addAll(prepareCreateNonImportableTableSql(namespace, UNSUPPORTED_DATA_TYPES_MSSQL));

    executeCreateTableSql(data);

    return ImmutableList.copyOf(data);
  }

  private void executeCreateTableSql(List<JdbcTestData> data) throws SQLException {
    String[] sqls = data.stream().map(JdbcTestData::getCreateTableSql).toArray(String[]::new);
    execute(sqls);
  }

  private boolean isMariaDB() {
    try (Connection connection = dataSource.getConnection()) {
      String version = connection.getMetaData().getDatabaseProductVersion();
      return version.contains("MariaDB");
    } catch (SQLException e) {
      throw new RuntimeException("Get database product version failed");
    }
  }

  private int getMajorVersion() {
    try (Connection connection = dataSource.getConnection()) {
      return connection.getMetaData().getDatabaseMajorVersion();
    } catch (SQLException e) {
      throw new RuntimeException("Get database major version failed");
    }
  }

  private Map<String, Column<?>> prepareInsertColumnsWithGenericAndCustomValues(
      TableMetadata tableMetadata, List<Column<?>> customColumns) {
    Map<String, Column<?>> genericColumnValuesByName =
        prepareInsertColumnsWithGenericValues(tableMetadata);
    customColumns.forEach(column -> genericColumnValuesByName.put(column.getName(), column));

    return genericColumnValuesByName;
  }

  private Map<String, Column<?>> prepareInsertColumnsWithGenericValues(
      TableMetadata tableMetadata) {
    return tableMetadata.getColumnNames().stream()
        .map(
            columnName ->
                prepareGenericColumnValue(columnName, tableMetadata.getColumnDataType(columnName)))
        .collect(Collectors.toMap(Column::getName, column -> column));
  }

  private Column<?> prepareGenericColumnValue(String columnName, DataType columnType) {
    switch (columnType) {
      case INT:
        return IntColumn.of(columnName, 1);
      case TEXT:
        return TextColumn.of(columnName, "foo");
      case BLOB:
        return BlobColumn.of(columnName, "ABCDE".getBytes(StandardCharsets.UTF_8));
      case FLOAT:
        return FloatColumn.of(columnName, 1.2F);
      case DOUBLE:
        return DoubleColumn.of(columnName, 4.23);
      case BIGINT:
        return BigIntColumn.of(columnName, 101);
      case BOOLEAN:
        return BooleanColumn.of(columnName, true);
      case DATE:
        return DateColumn.of(columnName, LocalDate.of(1003, 7, 14));
      case TIME:
        return TimeColumn.of(columnName, LocalTime.of(5, 45, 33, 123_456_000));
      case TIMESTAMP:
        return TimestampColumn.of(columnName, LocalDateTime.of(1003, 3, 2, 8, 35, 12, 123_000_000));
      case TIMESTAMPTZ:
        return TimestampTZColumn.of(
            columnName,
            LocalDateTime.of(1003, 3, 2, 8, 35, 12, 123_000_000).toInstant(ZoneOffset.UTC));
      default:
        throw new AssertionError();
    }
  }

  public void close() throws SQLException {
    dataSource.close();
  }

  @SuppressWarnings("UseCorrectAssertInTests")
  public static class JdbcTestData implements TestData {

    private final String tableName;
    private final String createTableSql;
    private final @Nullable Map<String, DataType> overrideColumnsType;
    private final @Nullable TableMetadata tableMetadata;
    private final @Nullable Map<String, Column<?>> columns;

    private JdbcTestData(
        String tableName,
        String createTableSql,
        @Nullable Map<String, DataType> overrideColumnsType,
        @Nullable TableMetadata tableMetadata,
        @Nullable Map<String, Column<?>> columns) {
      this.tableName = tableName;
      this.createTableSql = createTableSql;
      this.tableMetadata = tableMetadata;
      this.overrideColumnsType = overrideColumnsType;
      this.columns = columns;
    }

    /**
     * Create test data for a table that can be imported.
     *
     * @param tableName the table name
     * @param createTableSql the sql statement to create the table
     * @param tableMetadata the expected table metadata of the table once imported
     * @param overrideColumnsType the columns that needs to override the default data type mapping
     *     when importing the table
     * @param columns the values of columns to perform write-read operation
     * @return the test data
     */
    public static JdbcTestData createImportableTable(
        String tableName,
        String createTableSql,
        TableMetadata tableMetadata,
        Map<String, DataType> overrideColumnsType,
        Map<String, Column<?>> columns) {
      return new JdbcTestData(
          tableName, createTableSql, overrideColumnsType, tableMetadata, columns);
    }

    /**
     * Create test data for a table that cannot be imported.
     *
     * @param tableName the table name
     * @param createTableSql the sql statement to create the table
     * @return the test data
     */
    public static JdbcTestData createNonImportableTable(String tableName, String createTableSql) {
      return new JdbcTestData(tableName, createTableSql, null, null, null);
    }

    @Override
    public boolean isImportableTable() {
      return tableMetadata != null;
    }

    @Override
    public String getTableName() {
      return tableName;
    }

    private String getCreateTableSql() {
      return createTableSql;
    }

    @Override
    public Map<String, DataType> getOverrideColumnsType() {
      assert overrideColumnsType != null;
      return ImmutableMap.copyOf(overrideColumnsType);
    }

    @Override
    public TableMetadata getTableMetadata() {
      assert tableMetadata != null;
      return tableMetadata;
    }

    @Override
    public Insert getInsert(String namespace, String table) {
      assert columns != null;
      assert tableMetadata != null;

      InsertBuilder.Buildable insert =
          Insert.newBuilder().namespace(namespace).table(table).partitionKey(preparePartitionKey());
      columns.forEach(
          (name, column) -> {
            if (!tableMetadata.getPartitionKeyNames().contains(name)) {
              insert.value(column);
            }
          });

      return insert.build();
    }

    @Override
    public Put getPut(String namespace, String table) {
      assert columns != null;
      assert tableMetadata != null;

      PutBuilder.Buildable put =
          Put.newBuilder().namespace(namespace).table(table).partitionKey(preparePartitionKey());
      columns.forEach(
          (name, column) -> {
            if (!tableMetadata.getPartitionKeyNames().contains(name)) {
              put.value(column);
            }
          });

      return put.build();
    }

    @Override
    public Get getGet(String namespace, String table) {
      assert columns != null;

      return Get.newBuilder()
          .namespace(namespace)
          .table(table)
          .partitionKey(preparePartitionKey())
          .build();
    }

    private Key preparePartitionKey() {
      assert tableMetadata != null;
      Key.Builder key = Key.newBuilder();
      tableMetadata
          .getPartitionKeyNames()
          .forEach(
              col -> {
                assert columns != null;
                key.add(columns.get(col));
              });

      return key.build();
    }
  }
}
