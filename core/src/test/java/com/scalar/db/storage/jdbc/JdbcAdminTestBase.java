package com.scalar.db.storage.jdbc;

import static com.scalar.db.storage.jdbc.JdbcAdmin.JDBC_COL_COLUMN_NAME;
import static com.scalar.db.storage.jdbc.JdbcAdmin.JDBC_COL_COLUMN_SIZE;
import static com.scalar.db.storage.jdbc.JdbcAdmin.JDBC_COL_DATA_TYPE;
import static com.scalar.db.storage.jdbc.JdbcAdmin.JDBC_COL_DECIMAL_DIGITS;
import static com.scalar.db.storage.jdbc.JdbcAdmin.JDBC_COL_TYPE_NAME;
import static com.scalar.db.storage.jdbc.JdbcAdmin.hasDifferentClusteringOrders;
import static com.scalar.db.util.ScalarDbUtils.getFullTableName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.description;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.mysql.cj.jdbc.exceptions.CommunicationsException;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.JdbcAdminTestBase.GetColumnsResultSetMocker.Row;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.SQLiteException;

/**
 * Abstraction that defines unit tests for the {@link JdbcAdmin}. The class purpose is to be able to
 * run the {@link JdbcAdmin} unit tests with different values for the {@link JdbcConfig}, notably
 * {@link JdbcConfig#TABLE_METADATA_SCHEMA}.
 */
public abstract class JdbcAdminTestBase {
  private static final String NAMESPACE = "namespace";
  private static final String TABLE = "table";
  private static final String COLUMN_1 = "c1";
  private static final ImmutableMap<RdbEngine, RdbEngineStrategy> RDB_ENGINES =
      ImmutableMap.of(
          RdbEngine.MYSQL, RdbEngineFactory.create("jdbc:mysql:"),
          RdbEngine.ORACLE, RdbEngineFactory.create("jdbc:oracle:"),
          RdbEngine.POSTGRESQL, RdbEngineFactory.create("jdbc:postgresql:"),
          RdbEngine.SQL_SERVER, RdbEngineFactory.create("jdbc:sqlserver:"),
          RdbEngine.SQLITE, RdbEngineFactory.create("jdbc:sqlite:"),
          RdbEngine.YUGABYTE, RdbEngineFactory.create("jdbc:yugabytedb:"));

  @Mock private BasicDataSource dataSource;
  @Mock private Connection connection;
  @Mock private JdbcConfig config;

  private String tableMetadataSchemaName;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    when(config.getTableMetadataSchema()).thenReturn(getTableMetadataSchemaConfig());

    tableMetadataSchemaName = getTableMetadataSchemaConfig().orElse("scalardb");
  }

  private JdbcAdmin createJdbcAdminFor(RdbEngine rdbEngine) {
    // Arrange
    RdbEngineStrategy st = RdbEngine.createRdbEngineStrategy(rdbEngine);
    try (MockedStatic<RdbEngineFactory> mocked = mockStatic(RdbEngineFactory.class)) {
      mocked.when(() -> RdbEngineFactory.create(any(JdbcConfig.class))).thenReturn(st);
      return new JdbcAdmin(dataSource, config);
    }
  }

  private void mockUndefinedTableError(RdbEngine rdbEngine, SQLException sqlException) {
    switch (rdbEngine) {
      case MYSQL:
        when(sqlException.getErrorCode()).thenReturn(1049);
        break;
      case POSTGRESQL:
      case YUGABYTE:
        when(sqlException.getSQLState()).thenReturn("42P01");
        break;
      case ORACLE:
        when(sqlException.getErrorCode()).thenReturn(942);
        break;
      case SQL_SERVER:
        when(sqlException.getErrorCode()).thenReturn(208);
        break;
      case SQLITE:
        when(sqlException.getErrorCode()).thenReturn(1);
        when(sqlException.getMessage()).thenReturn("no such table: ");
        break;
    }
  }

  /**
   * This sets the {@link JdbcConfig#TABLE_METADATA_SCHEMA} value that will be used to run the
   * tests.
   *
   * @return {@link JdbcConfig#TABLE_METADATA_SCHEMA} value
   */
  abstract Optional<String> getTableMetadataSchemaConfig();

  @Test
  public void getTableMetadata_forMysql_ShouldReturnTableMetadata()
      throws SQLException, ExecutionException {
    getTableMetadata_forX_ShouldReturnTableMetadata(
        RdbEngine.MYSQL,
        "SELECT `column_name`,`data_type`,`key_type`,`clustering_order`,`indexed` FROM `"
            + tableMetadataSchemaName
            + "`.`metadata` WHERE `full_table_name`=? ORDER BY `ordinal_position` ASC");
  }

  @Test
  public void getTableMetadata_forPostgresql_ShouldReturnTableMetadata()
      throws SQLException, ExecutionException {
    getTableMetadata_forX_ShouldReturnTableMetadata(
        RdbEngine.POSTGRESQL,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + tableMetadataSchemaName
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC");
  }

  @Test
  public void getTableMetadata_forSqlServer_ShouldReturnTableMetadata()
      throws SQLException, ExecutionException {
    getTableMetadata_forX_ShouldReturnTableMetadata(
        RdbEngine.SQL_SERVER,
        "SELECT [column_name],[data_type],[key_type],[clustering_order],[indexed] FROM ["
            + tableMetadataSchemaName
            + "].[metadata] WHERE [full_table_name]=? ORDER BY [ordinal_position] ASC");
  }

  @Test
  public void getTableMetadata_forOracle_ShouldReturnTableMetadata()
      throws SQLException, ExecutionException {
    getTableMetadata_forX_ShouldReturnTableMetadata(
        RdbEngine.ORACLE,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + tableMetadataSchemaName
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC");
  }

  @Test
  public void getTableMetadata_forSqlite_ShouldReturnTableMetadata()
      throws SQLException, ExecutionException {
    getTableMetadata_forX_ShouldReturnTableMetadata(
        RdbEngine.SQLITE,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + tableMetadataSchemaName
            + "$metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC");
  }

  private void getTableMetadata_forX_ShouldReturnTableMetadata(
      RdbEngine rdbEngine, String expectedSelectStatements)
      throws ExecutionException, SQLException {
    // Arrange
    String namespace = "ns";
    String table = "table";

    PreparedStatement checkStatement = prepareStatementForNamespaceCheck();
    PreparedStatement selectStatement = mock(PreparedStatement.class);
    ResultSet resultSet =
        mockResultSet(
            Arrays.asList(
                new GetColumnsResultSetMocker.Row(
                    "c3", DataType.BOOLEAN.toString(), "PARTITION", null, false),
                new GetColumnsResultSetMocker.Row(
                    "c1", DataType.TEXT.toString(), "CLUSTERING", Order.DESC.toString(), false),
                new GetColumnsResultSetMocker.Row(
                    "c4", DataType.BLOB.toString(), "CLUSTERING", Order.ASC.toString(), true),
                new GetColumnsResultSetMocker.Row(
                    "c2", DataType.BIGINT.toString(), null, null, false),
                new GetColumnsResultSetMocker.Row("c5", DataType.INT.toString(), null, null, false),
                new GetColumnsResultSetMocker.Row(
                    "c6", DataType.DOUBLE.toString(), null, null, false),
                new GetColumnsResultSetMocker.Row(
                    "c7", DataType.FLOAT.toString(), null, null, false)));
    when(selectStatement.executeQuery()).thenReturn(resultSet);
    when(connection.prepareStatement(any())).thenReturn(checkStatement).thenReturn(selectStatement);
    when(dataSource.getConnection()).thenReturn(connection);

    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    // Act
    TableMetadata actualMetadata = admin.getTableMetadata(namespace, table);

    // Assert
    TableMetadata expectedMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("c3")
            .addClusteringKey("c1", Order.DESC)
            .addClusteringKey("c4", Order.ASC)
            .addColumn("c1", DataType.TEXT)
            .addColumn("c2", DataType.BIGINT)
            .addColumn("c3", DataType.BOOLEAN)
            .addColumn("c4", DataType.BLOB)
            .addColumn("c5", DataType.INT)
            .addColumn("c6", DataType.DOUBLE)
            .addColumn("c7", DataType.FLOAT)
            .addSecondaryIndex("c4")
            .build();
    assertThat(actualMetadata).isEqualTo(expectedMetadata);
    verify(connection).prepareStatement(expectedSelectStatements);
  }

  public ResultSet mockResultSet(List<GetColumnsResultSetMocker.Row> rows) throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);
    // Everytime the ResultSet.next() method will be called, the ResultSet.getXXX methods call be
    // mocked to return the current row data
    doAnswer(new GetColumnsResultSetMocker(rows)).when(resultSet).next();
    return resultSet;
  }

  @Test
  public void getTableMetadata_MetadataSchemaNotExistsForX_ShouldReturnNull()
      throws SQLException, ExecutionException {
    for (RdbEngine rdbEngine : RDB_ENGINES.keySet()) {
      getTableMetadata_MetadataSchemaNotExistsForX_ShouldReturnNull(rdbEngine);
    }
  }

  private void getTableMetadata_MetadataSchemaNotExistsForX_ShouldReturnNull(RdbEngine rdbEngine)
      throws SQLException, ExecutionException {
    // Arrange
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    Connection connection = mock(Connection.class);
    PreparedStatement selectStatement = prepareStatementForNamespaceCheck(false);

    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.prepareStatement(any())).thenReturn(selectStatement);

    // Act
    TableMetadata actual = admin.getTableMetadata("my_ns", "my_tbl");

    // Assert
    assertThat(actual).isNull();
  }

  @Test
  public void createNamespace_forMysql_shouldExecuteCreateNamespaceStatement()
      throws ExecutionException, SQLException {
    createNamespace_forX_shouldExecuteCreateNamespaceStatement(
        RdbEngine.MYSQL, "CREATE SCHEMA `my_ns`");
  }

  @Test
  public void createNamespace_forPostgresql_shouldExecuteCreateNamespaceStatement()
      throws ExecutionException, SQLException {
    createNamespace_forX_shouldExecuteCreateNamespaceStatement(
        RdbEngine.POSTGRESQL, "CREATE SCHEMA \"my_ns\"");
  }

  @Test
  public void createNamespace_forSqlServer_shouldExecuteCreateNamespaceStatement()
      throws ExecutionException, SQLException {
    createNamespace_forX_shouldExecuteCreateNamespaceStatement(
        RdbEngine.SQL_SERVER, "CREATE SCHEMA [my_ns]");
  }

  @Test
  public void createNamespace_forOracle_shouldExecuteCreateNamespaceStatement()
      throws ExecutionException, SQLException {
    createNamespace_forX_shouldExecuteCreateNamespaceStatement(
        RdbEngine.ORACLE,
        "CREATE USER \"my_ns\" IDENTIFIED BY \"oracle\"",
        "ALTER USER \"my_ns\" quota unlimited on USERS");
  }

  @Test
  public void createNamespace_forSqlite_shouldExecuteCreateNamespaceStatement() {
    // no sql is executed
  }

  private void createNamespace_forX_shouldExecuteCreateNamespaceStatement(
      RdbEngine rdbEngine, String... expectedSqlStatements)
      throws SQLException, ExecutionException {
    // Arrange
    String namespace = "my_ns";
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);
    List<Statement> mockedStatements = new ArrayList<>();

    for (int i = 0; i < expectedSqlStatements.length; i++) {
      mockedStatements.add(mock(Statement.class));
    }
    when(connection.createStatement())
        .thenReturn(
            mockedStatements.get(0),
            mockedStatements.subList(1, mockedStatements.size()).toArray(new Statement[0]));
    when(dataSource.getConnection()).thenReturn(connection);

    // Act
    admin.createNamespace(namespace);

    // Assert
    for (int i = 0; i < expectedSqlStatements.length; i++) {
      verify(mockedStatements.get(i)).execute(expectedSqlStatements[i]);
    }
  }

  @Test
  public void createTable_forSqlite_withInvalidTableName_shouldThrowExecutionException() {
    // Arrange
    String namespace = "my_ns";
    String table = "foo$table"; // contains namespace separator
    TableMetadata metadata =
        TableMetadata.newBuilder().addPartitionKey("c1").addColumn("c1", DataType.TEXT).build();

    JdbcAdmin admin = createJdbcAdminFor(RdbEngine.SQLITE);

    // Act
    // Assert
    assertThatThrownBy(() -> admin.createTable(namespace, table, metadata, new HashMap<>()))
        .isInstanceOf(ExecutionException.class);
  }

  @Test
  public void createTable_forMysql_shouldExecuteCreateTableStatement()
      throws ExecutionException, SQLException {
    createTable_forX_shouldExecuteCreateTableStatement(
        RdbEngine.MYSQL,
        "CREATE TABLE `my_ns`.`foo_table`(`c3` BOOLEAN,`c1` VARCHAR(64),`c4` VARBINARY(64),`c2` BIGINT,`c5` INT,`c6` DOUBLE,`c7` DOUBLE, PRIMARY KEY (`c3`,`c1`,`c4`))",
        "CREATE INDEX `index_my_ns_foo_table_c4` ON `my_ns`.`foo_table` (`c4`)",
        "CREATE INDEX `index_my_ns_foo_table_c1` ON `my_ns`.`foo_table` (`c1`)",
        "CREATE SCHEMA IF NOT EXISTS `" + tableMetadataSchemaName + "`",
        "CREATE TABLE IF NOT EXISTS `"
            + tableMetadataSchemaName
            + "`.`metadata`("
            + "`full_table_name` VARCHAR(128),"
            + "`column_name` VARCHAR(128),"
            + "`data_type` VARCHAR(20) NOT NULL,"
            + "`key_type` VARCHAR(20),"
            + "`clustering_order` VARCHAR(10),"
            + "`indexed` BOOLEAN NOT NULL,"
            + "`ordinal_position` INTEGER NOT NULL,"
            + "PRIMARY KEY (`full_table_name`, `column_name`))",
        "INSERT INTO `"
            + tableMetadataSchemaName
            + "`.`metadata` VALUES ('my_ns.foo_table','c3','BOOLEAN','PARTITION',NULL,false,1)",
        "INSERT INTO `"
            + tableMetadataSchemaName
            + "`.`metadata` VALUES ('my_ns.foo_table','c1','TEXT','CLUSTERING','ASC',true,2)",
        "INSERT INTO `"
            + tableMetadataSchemaName
            + "`.`metadata` VALUES ('my_ns.foo_table','c4','BLOB','CLUSTERING','ASC',true,3)",
        "INSERT INTO `"
            + tableMetadataSchemaName
            + "`.`metadata` VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,false,4)",
        "INSERT INTO `"
            + tableMetadataSchemaName
            + "`.`metadata` VALUES ('my_ns.foo_table','c5','INT',NULL,NULL,false,5)",
        "INSERT INTO `"
            + tableMetadataSchemaName
            + "`.`metadata` VALUES ('my_ns.foo_table','c6','DOUBLE',NULL,NULL,false,6)",
        "INSERT INTO `"
            + tableMetadataSchemaName
            + "`.`metadata` VALUES ('my_ns.foo_table','c7','FLOAT',NULL,NULL,false,7)");
  }

  @Test
  public void createTable_forPostgresql_shouldExecuteCreateTableStatement()
      throws ExecutionException, SQLException {
    createTable_forX_shouldExecuteCreateTableStatement(
        RdbEngine.POSTGRESQL,
        "CREATE TABLE \"my_ns\".\"foo_table\"(\"c3\" BOOLEAN,\"c1\" VARCHAR(10485760),\"c4\" BYTEA,\"c2\" BIGINT,\"c5\" INT,\"c6\" DOUBLE PRECISION,\"c7\" FLOAT, PRIMARY KEY (\"c3\",\"c1\",\"c4\"))",
        "CREATE INDEX \"index_my_ns_foo_table_c4\" ON \"my_ns\".\"foo_table\" (\"c4\")",
        "CREATE INDEX \"index_my_ns_foo_table_c1\" ON \"my_ns\".\"foo_table\" (\"c1\")",
        "CREATE SCHEMA IF NOT EXISTS \"" + tableMetadataSchemaName + "\"",
        "CREATE TABLE IF NOT EXISTS \""
            + tableMetadataSchemaName
            + "\".\"metadata\"("
            + "\"full_table_name\" VARCHAR(128),"
            + "\"column_name\" VARCHAR(128),"
            + "\"data_type\" VARCHAR(20) NOT NULL,"
            + "\"key_type\" VARCHAR(20),"
            + "\"clustering_order\" VARCHAR(10),"
            + "\"indexed\" BOOLEAN NOT NULL,"
            + "\"ordinal_position\" INTEGER NOT NULL,"
            + "PRIMARY KEY (\"full_table_name\", \"column_name\"))",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c3','BOOLEAN','PARTITION',NULL,false,1)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c1','TEXT','CLUSTERING','ASC',true,2)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c4','BLOB','CLUSTERING','ASC',true,3)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,false,4)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c5','INT',NULL,NULL,false,5)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c6','DOUBLE',NULL,NULL,false,6)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c7','FLOAT',NULL,NULL,false,7)");
  }

  @Test
  public void createTable_forSqlServer_shouldExecuteCreateTableStatement()
      throws ExecutionException, SQLException {
    createTable_forX_shouldExecuteCreateTableStatement(
        RdbEngine.SQL_SERVER,
        "CREATE TABLE [my_ns].[foo_table]([c3] BIT,[c1] VARCHAR(8000),"
            + "[c4] VARBINARY(8000),[c2] BIGINT,[c5] INT,[c6] FLOAT,[c7] FLOAT(24), PRIMARY KEY ([c3],[c1],[c4]))",
        "CREATE INDEX [index_my_ns_foo_table_c4] ON [my_ns].[foo_table] ([c4])",
        "CREATE INDEX [index_my_ns_foo_table_c1] ON [my_ns].[foo_table] ([c1])",
        "CREATE SCHEMA [" + tableMetadataSchemaName + "]",
        "CREATE TABLE ["
            + tableMetadataSchemaName
            + "].[metadata]("
            + "[full_table_name] VARCHAR(128),"
            + "[column_name] VARCHAR(128),"
            + "[data_type] VARCHAR(20) NOT NULL,"
            + "[key_type] VARCHAR(20),"
            + "[clustering_order] VARCHAR(10),"
            + "[indexed] BIT NOT NULL,"
            + "[ordinal_position] INTEGER NOT NULL,"
            + "PRIMARY KEY ([full_table_name], [column_name]))",
        "INSERT INTO ["
            + tableMetadataSchemaName
            + "].[metadata] VALUES ('my_ns.foo_table','c3','BOOLEAN','PARTITION',NULL,0,1)",
        "INSERT INTO ["
            + tableMetadataSchemaName
            + "].[metadata] VALUES ('my_ns.foo_table','c1','TEXT','CLUSTERING','ASC',1,2)",
        "INSERT INTO ["
            + tableMetadataSchemaName
            + "].[metadata] VALUES ('my_ns.foo_table','c4','BLOB','CLUSTERING','ASC',1,3)",
        "INSERT INTO ["
            + tableMetadataSchemaName
            + "].[metadata] VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,0,4)",
        "INSERT INTO ["
            + tableMetadataSchemaName
            + "].[metadata] VALUES ('my_ns.foo_table','c5','INT',NULL,NULL,0,5)",
        "INSERT INTO ["
            + tableMetadataSchemaName
            + "].[metadata] VALUES ('my_ns.foo_table','c6','DOUBLE',NULL,NULL,0,6)",
        "INSERT INTO ["
            + tableMetadataSchemaName
            + "].[metadata] VALUES ('my_ns.foo_table','c7','FLOAT',NULL,NULL,0,7)");
  }

  @Test
  public void createTable_forOracle_shouldExecuteCreateTableStatement()
      throws ExecutionException, SQLException {
    createTable_forX_shouldExecuteCreateTableStatement(
        RdbEngine.ORACLE,
        "CREATE TABLE \"my_ns\".\"foo_table\"(\"c3\" NUMBER(1),\"c1\" VARCHAR2(64),\"c4\" RAW(64),\"c2\" NUMBER(19),\"c5\" INT,\"c6\" BINARY_DOUBLE,\"c7\" BINARY_FLOAT, PRIMARY KEY (\"c3\",\"c1\",\"c4\")) ROWDEPENDENCIES",
        "ALTER TABLE \"my_ns\".\"foo_table\" INITRANS 3 MAXTRANS 255",
        "CREATE INDEX \"index_my_ns_foo_table_c4\" ON \"my_ns\".\"foo_table\" (\"c4\")",
        "CREATE INDEX \"index_my_ns_foo_table_c1\" ON \"my_ns\".\"foo_table\" (\"c1\")",
        "CREATE USER \"" + tableMetadataSchemaName + "\" IDENTIFIED BY \"oracle\"",
        "ALTER USER \"" + tableMetadataSchemaName + "\" quota unlimited on USERS",
        "CREATE TABLE \""
            + tableMetadataSchemaName
            + "\".\"metadata\"(\"full_table_name\" VARCHAR2(128),\"column_name\" VARCHAR2(128),\"data_type\" VARCHAR2(20) NOT NULL,\"key_type\" VARCHAR2(20),\"clustering_order\" VARCHAR2(10),\"indexed\" NUMBER(1) NOT NULL,\"ordinal_position\" INTEGER NOT NULL,PRIMARY KEY (\"full_table_name\", \"column_name\"))",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c3','BOOLEAN','PARTITION',NULL,0,1)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c1','TEXT','CLUSTERING','ASC',1,2)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c4','BLOB','CLUSTERING','ASC',1,3)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,0,4)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c5','INT',NULL,NULL,0,5)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c6','DOUBLE',NULL,NULL,0,6)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c7','FLOAT',NULL,NULL,0,7)");
  }

  @Test
  public void createTable_forSqlite_shouldExecuteCreateTableStatement()
      throws ExecutionException, SQLException {
    createTable_forX_shouldExecuteCreateTableStatement(
        RdbEngine.SQLITE,
        "CREATE TABLE \"my_ns$foo_table\"(\"c3\" BOOLEAN,\"c1\" TEXT,\"c4\" BLOB,\"c2\" BIGINT,\"c5\" INT,\"c6\" DOUBLE,\"c7\" FLOAT, PRIMARY KEY (\"c3\",\"c1\",\"c4\"))",
        "CREATE INDEX \"index_my_ns_foo_table_c4\" ON \"my_ns$foo_table\" (\"c4\")",
        "CREATE INDEX \"index_my_ns_foo_table_c1\" ON \"my_ns$foo_table\" (\"c1\")",
        "CREATE TABLE IF NOT EXISTS \""
            + tableMetadataSchemaName
            + "$metadata\"("
            + "\"full_table_name\" TEXT,"
            + "\"column_name\" TEXT,"
            + "\"data_type\" TEXT NOT NULL,"
            + "\"key_type\" TEXT,"
            + "\"clustering_order\" TEXT,"
            + "\"indexed\" BOOLEAN NOT NULL,"
            + "\"ordinal_position\" INTEGER NOT NULL,"
            + "PRIMARY KEY (\"full_table_name\", \"column_name\"))",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "$metadata\" VALUES ('my_ns.foo_table','c3','BOOLEAN','PARTITION',NULL,FALSE,1)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "$metadata\" VALUES ('my_ns.foo_table','c1','TEXT','CLUSTERING','ASC',TRUE,2)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "$metadata\" VALUES ('my_ns.foo_table','c4','BLOB','CLUSTERING','ASC',TRUE,3)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "$metadata\" VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,FALSE,4)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "$metadata\" VALUES ('my_ns.foo_table','c5','INT',NULL,NULL,FALSE,5)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "$metadata\" VALUES ('my_ns.foo_table','c6','DOUBLE',NULL,NULL,FALSE,6)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "$metadata\" VALUES ('my_ns.foo_table','c7','FLOAT',NULL,NULL,FALSE,7)");
  }

  private void createTable_forX_shouldExecuteCreateTableStatement(
      RdbEngine rdbEngine, String... expectedSqlStatements)
      throws SQLException, ExecutionException {
    // Arrange
    String namespace = "my_ns";
    String table = "foo_table";
    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addPartitionKey("c3")
            .addClusteringKey("c1")
            .addClusteringKey("c4")
            .addColumn("c1", DataType.TEXT)
            .addColumn("c2", DataType.BIGINT)
            .addColumn("c3", DataType.BOOLEAN)
            .addColumn("c4", DataType.BLOB)
            .addColumn("c5", DataType.INT)
            .addColumn("c6", DataType.DOUBLE)
            .addColumn("c7", DataType.FLOAT)
            .addSecondaryIndex("c1")
            .addSecondaryIndex("c4")
            .build();

    List<Statement> mockedStatements = new ArrayList<>();
    for (int i = 0; i < expectedSqlStatements.length; i++) {
      mockedStatements.add(mock(Statement.class));
    }
    when(connection.createStatement())
        .thenReturn(
            mockedStatements.get(0),
            mockedStatements.subList(1, mockedStatements.size()).toArray(new Statement[0]));
    when(dataSource.getConnection()).thenReturn(connection);

    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    // Act
    admin.createTable(namespace, table, metadata, new HashMap<>());

    // Assert
    for (int i = 0; i < expectedSqlStatements.length; i++) {
      verify(mockedStatements.get(i)).execute(expectedSqlStatements[i]);
    }
  }

  @Test
  public void createTable_WithClusteringOrderForMysql_shouldExecuteCreateTableStatement()
      throws ExecutionException, SQLException {
    createTable_WithClusteringOrderForX_shouldExecuteCreateTableStatement(
        RdbEngine.MYSQL,
        "CREATE TABLE `my_ns`.`foo_table`(`c3` BOOLEAN,`c1` VARCHAR(64),`c4` VARBINARY(64),`c2` BIGINT,`c5` INT,`c6` DOUBLE,`c7` DOUBLE, PRIMARY KEY (`c3` ASC,`c1` DESC,`c4` ASC))",
        "CREATE INDEX `index_my_ns_foo_table_c4` ON `my_ns`.`foo_table` (`c4`)",
        "CREATE INDEX `index_my_ns_foo_table_c1` ON `my_ns`.`foo_table` (`c1`)",
        "CREATE SCHEMA IF NOT EXISTS `" + tableMetadataSchemaName + "`",
        "CREATE TABLE IF NOT EXISTS `"
            + tableMetadataSchemaName
            + "`.`metadata`("
            + "`full_table_name` VARCHAR(128),"
            + "`column_name` VARCHAR(128),"
            + "`data_type` VARCHAR(20) NOT NULL,"
            + "`key_type` VARCHAR(20),"
            + "`clustering_order` VARCHAR(10),"
            + "`indexed` BOOLEAN NOT NULL,"
            + "`ordinal_position` INTEGER NOT NULL,"
            + "PRIMARY KEY (`full_table_name`, `column_name`))",
        "INSERT INTO `"
            + tableMetadataSchemaName
            + "`.`metadata` VALUES ('my_ns.foo_table','c3','BOOLEAN','PARTITION',NULL,false,1)",
        "INSERT INTO `"
            + tableMetadataSchemaName
            + "`.`metadata` VALUES ('my_ns.foo_table','c1','TEXT','CLUSTERING','DESC',true,2)",
        "INSERT INTO `"
            + tableMetadataSchemaName
            + "`.`metadata` VALUES ('my_ns.foo_table','c4','BLOB','CLUSTERING','ASC',true,3)",
        "INSERT INTO `"
            + tableMetadataSchemaName
            + "`.`metadata` VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,false,4)",
        "INSERT INTO `"
            + tableMetadataSchemaName
            + "`.`metadata` VALUES ('my_ns.foo_table','c5','INT',NULL,NULL,false,5)",
        "INSERT INTO `"
            + tableMetadataSchemaName
            + "`.`metadata` VALUES ('my_ns.foo_table','c6','DOUBLE',NULL,NULL,false,6)",
        "INSERT INTO `"
            + tableMetadataSchemaName
            + "`.`metadata` VALUES ('my_ns.foo_table','c7','FLOAT',NULL,NULL,false,7)");
  }

  @Test
  public void createTable_WithClusteringOrderForPostgresql_shouldExecuteCreateTableStatement()
      throws ExecutionException, SQLException {
    createTable_WithClusteringOrderForX_shouldExecuteCreateTableStatement(
        RdbEngine.POSTGRESQL,
        "CREATE TABLE \"my_ns\".\"foo_table\"(\"c3\" BOOLEAN,\"c1\" VARCHAR(10485760),\"c4\" BYTEA,\"c2\" BIGINT,\"c5\" INT,\"c6\" DOUBLE PRECISION,\"c7\" FLOAT, PRIMARY KEY (\"c3\",\"c1\",\"c4\"))",
        "CREATE UNIQUE INDEX \"my_ns.foo_table_clustering_order_idx\" ON \"my_ns\".\"foo_table\" (\"c3\" ASC,\"c1\" DESC,\"c4\" ASC)",
        "CREATE INDEX \"index_my_ns_foo_table_c4\" ON \"my_ns\".\"foo_table\" (\"c4\")",
        "CREATE INDEX \"index_my_ns_foo_table_c1\" ON \"my_ns\".\"foo_table\" (\"c1\")",
        "CREATE SCHEMA IF NOT EXISTS \"" + tableMetadataSchemaName + "\"",
        "CREATE TABLE IF NOT EXISTS \""
            + tableMetadataSchemaName
            + "\".\"metadata\"("
            + "\"full_table_name\" VARCHAR(128),"
            + "\"column_name\" VARCHAR(128),"
            + "\"data_type\" VARCHAR(20) NOT NULL,"
            + "\"key_type\" VARCHAR(20),"
            + "\"clustering_order\" VARCHAR(10),"
            + "\"indexed\" BOOLEAN NOT NULL,"
            + "\"ordinal_position\" INTEGER NOT NULL,"
            + "PRIMARY KEY (\"full_table_name\", \"column_name\"))",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c3','BOOLEAN','PARTITION',NULL,false,1)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c1','TEXT','CLUSTERING','DESC',true,2)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c4','BLOB','CLUSTERING','ASC',true,3)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,false,4)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c5','INT',NULL,NULL,false,5)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c6','DOUBLE',NULL,NULL,false,6)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c7','FLOAT',NULL,NULL,false,7)");
  }

  @Test
  public void createTable_WithClusteringOrderForSqlServer_shouldExecuteCreateTableStatement()
      throws ExecutionException, SQLException {
    createTable_WithClusteringOrderForX_shouldExecuteCreateTableStatement(
        RdbEngine.SQL_SERVER,
        "CREATE TABLE [my_ns].[foo_table]([c3] BIT,[c1] VARCHAR(8000),"
            + "[c4] VARBINARY(8000),[c2] BIGINT,[c5] INT,[c6] FLOAT,[c7] FLOAT(24), PRIMARY KEY ([c3] ASC,[c1] DESC,[c4] ASC))",
        "CREATE INDEX [index_my_ns_foo_table_c4] ON [my_ns].[foo_table] ([c4])",
        "CREATE INDEX [index_my_ns_foo_table_c1] ON [my_ns].[foo_table] ([c1])",
        "CREATE SCHEMA [" + tableMetadataSchemaName + "]",
        "CREATE TABLE ["
            + tableMetadataSchemaName
            + "].[metadata]("
            + "[full_table_name] VARCHAR(128),"
            + "[column_name] VARCHAR(128),"
            + "[data_type] VARCHAR(20) NOT NULL,"
            + "[key_type] VARCHAR(20),"
            + "[clustering_order] VARCHAR(10),"
            + "[indexed] BIT NOT NULL,"
            + "[ordinal_position] INTEGER NOT NULL,"
            + "PRIMARY KEY ([full_table_name], [column_name]))",
        "INSERT INTO ["
            + tableMetadataSchemaName
            + "].[metadata] VALUES ('my_ns.foo_table','c3','BOOLEAN','PARTITION',NULL,0,1)",
        "INSERT INTO ["
            + tableMetadataSchemaName
            + "].[metadata] VALUES ('my_ns.foo_table','c1','TEXT','CLUSTERING','DESC',1,2)",
        "INSERT INTO ["
            + tableMetadataSchemaName
            + "].[metadata] VALUES ('my_ns.foo_table','c4','BLOB','CLUSTERING','ASC',1,3)",
        "INSERT INTO ["
            + tableMetadataSchemaName
            + "].[metadata] VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,0,4)",
        "INSERT INTO ["
            + tableMetadataSchemaName
            + "].[metadata] VALUES ('my_ns.foo_table','c5','INT',NULL,NULL,0,5)",
        "INSERT INTO ["
            + tableMetadataSchemaName
            + "].[metadata] VALUES ('my_ns.foo_table','c6','DOUBLE',NULL,NULL,0,6)",
        "INSERT INTO ["
            + tableMetadataSchemaName
            + "].[metadata] VALUES ('my_ns.foo_table','c7','FLOAT',NULL,NULL,0,7)");
  }

  @Test
  public void createTable_WithClusteringOrderForOracle_shouldExecuteCreateTableStatement()
      throws ExecutionException, SQLException {
    createTable_WithClusteringOrderForX_shouldExecuteCreateTableStatement(
        RdbEngine.ORACLE,
        "CREATE TABLE \"my_ns\".\"foo_table\"(\"c3\" NUMBER(1),\"c1\" VARCHAR2(64),\"c4\" RAW(64),\"c2\" NUMBER(19),\"c5\" INT,\"c6\" BINARY_DOUBLE,\"c7\" BINARY_FLOAT, PRIMARY KEY (\"c3\",\"c1\",\"c4\")) ROWDEPENDENCIES",
        "ALTER TABLE \"my_ns\".\"foo_table\" INITRANS 3 MAXTRANS 255",
        "CREATE UNIQUE INDEX \"my_ns.foo_table_clustering_order_idx\" ON \"my_ns\".\"foo_table\" (\"c3\" ASC,\"c1\" DESC,\"c4\" ASC)",
        "CREATE INDEX \"index_my_ns_foo_table_c4\" ON \"my_ns\".\"foo_table\" (\"c4\")",
        "CREATE INDEX \"index_my_ns_foo_table_c1\" ON \"my_ns\".\"foo_table\" (\"c1\")",
        "CREATE USER \"" + tableMetadataSchemaName + "\" IDENTIFIED BY \"oracle\"",
        "ALTER USER \"" + tableMetadataSchemaName + "\" quota unlimited on USERS",
        "CREATE TABLE \""
            + tableMetadataSchemaName
            + "\".\"metadata\"(\"full_table_name\" VARCHAR2(128),\"column_name\" VARCHAR2(128),\"data_type\" VARCHAR2(20) NOT NULL,\"key_type\" VARCHAR2(20),\"clustering_order\" VARCHAR2(10),\"indexed\" NUMBER(1) NOT NULL,\"ordinal_position\" INTEGER NOT NULL,PRIMARY KEY (\"full_table_name\", \"column_name\"))",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c3','BOOLEAN','PARTITION',NULL,0,1)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c1','TEXT','CLUSTERING','DESC',1,2)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c4','BLOB','CLUSTERING','ASC',1,3)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,0,4)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c5','INT',NULL,NULL,0,5)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c6','DOUBLE',NULL,NULL,0,6)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c7','FLOAT',NULL,NULL,0,7)");
  }

  @Test
  public void createTable_WithClusteringOrderForSqlite_shouldExecuteCreateTableStatement()
      throws ExecutionException, SQLException {
    createTable_WithClusteringOrderForX_shouldExecuteCreateTableStatement(
        RdbEngine.SQLITE,
        "CREATE TABLE \"my_ns$foo_table\"(\"c3\" BOOLEAN,\"c1\" TEXT,\"c4\" BLOB,\"c2\" BIGINT,\"c5\" INT,\"c6\" DOUBLE,\"c7\" FLOAT, PRIMARY KEY (\"c3\",\"c1\",\"c4\"))",
        "CREATE INDEX \"index_my_ns_foo_table_c4\" ON \"my_ns$foo_table\" (\"c4\")",
        "CREATE INDEX \"index_my_ns_foo_table_c1\" ON \"my_ns$foo_table\" (\"c1\")",
        "CREATE TABLE IF NOT EXISTS \""
            + tableMetadataSchemaName
            + "$metadata\"("
            + "\"full_table_name\" TEXT,"
            + "\"column_name\" TEXT,"
            + "\"data_type\" TEXT NOT NULL,"
            + "\"key_type\" TEXT,"
            + "\"clustering_order\" TEXT,"
            + "\"indexed\" BOOLEAN NOT NULL,"
            + "\"ordinal_position\" INTEGER NOT NULL,"
            + "PRIMARY KEY (\"full_table_name\", \"column_name\"))",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "$metadata\" VALUES ('my_ns.foo_table','c3','BOOLEAN','PARTITION',NULL,FALSE,1)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "$metadata\" VALUES ('my_ns.foo_table','c1','TEXT','CLUSTERING','DESC',TRUE,2)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "$metadata\" VALUES ('my_ns.foo_table','c4','BLOB','CLUSTERING','ASC',TRUE,3)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "$metadata\" VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,FALSE,4)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "$metadata\" VALUES ('my_ns.foo_table','c5','INT',NULL,NULL,FALSE,5)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "$metadata\" VALUES ('my_ns.foo_table','c6','DOUBLE',NULL,NULL,FALSE,6)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "$metadata\" VALUES ('my_ns.foo_table','c7','FLOAT',NULL,NULL,FALSE,7)");
  }

  private void createTable_WithClusteringOrderForX_shouldExecuteCreateTableStatement(
      RdbEngine rdbEngine, String... expectedSqlStatements)
      throws SQLException, ExecutionException {
    // Arrange
    String namespace = "my_ns";
    String table = "foo_table";
    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addPartitionKey("c3")
            .addClusteringKey("c1", Order.DESC)
            .addClusteringKey("c4", Order.ASC)
            .addColumn("c1", DataType.TEXT)
            .addColumn("c2", DataType.BIGINT)
            .addColumn("c3", DataType.BOOLEAN)
            .addColumn("c4", DataType.BLOB)
            .addColumn("c5", DataType.INT)
            .addColumn("c6", DataType.DOUBLE)
            .addColumn("c7", DataType.FLOAT)
            .addSecondaryIndex("c1")
            .addSecondaryIndex("c4")
            .build();

    List<Statement> mockedStatements = new ArrayList<>();
    for (int i = 0; i < expectedSqlStatements.length; i++) {
      mockedStatements.add(mock(Statement.class));
    }
    when(connection.createStatement())
        .thenReturn(
            mockedStatements.get(0),
            mockedStatements.subList(1, mockedStatements.size()).toArray(new Statement[0]));
    when(dataSource.getConnection()).thenReturn(connection);

    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    // Act
    admin.createTable(namespace, table, metadata, new HashMap<>());

    // Assert
    for (int i = 0; i < expectedSqlStatements.length; i++) {
      verify(mockedStatements.get(i)).execute(expectedSqlStatements[i]);
    }
  }

  @Test
  public void
      createMetadataTableIfNotExists_WithInternalDbError_forMysql_shouldThrowInternalDbError()
          throws SQLException {
    createMetadataTableIfNotExists_WithInternalDbError_forX_shouldThrowInternalDbError(
        RdbEngine.MYSQL, new CommunicationsException("", null));
  }

  @Test
  public void
      createMetadataTableIfNotExists_WithInternalDbError_forPostgresql_shouldThrowInternalDbError()
          throws SQLException {
    createMetadataTableIfNotExists_WithInternalDbError_forX_shouldThrowInternalDbError(
        RdbEngine.POSTGRESQL, new PSQLException("", PSQLState.CONNECTION_FAILURE));
  }

  @Test
  public void
      createMetadataTableIfNotExists_WithInternalDbError_forSqlite_shouldThrowInternalDbError()
          throws SQLException {
    createMetadataTableIfNotExists_WithInternalDbError_forX_shouldThrowInternalDbError(
        RdbEngine.SQLITE, new SQLiteException("", SQLiteErrorCode.SQLITE_IOERR));
  }

  private void createMetadataTableIfNotExists_WithInternalDbError_forX_shouldThrowInternalDbError(
      RdbEngine rdbEngine, SQLException internalDbError) throws SQLException {
    // Arrange
    when(connection.createStatement()).thenThrow(internalDbError);
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    // Act
    // Assert
    assertThatThrownBy(() -> admin.createMetadataTableIfNotExists(connection))
        .isInstanceOf(internalDbError.getClass());
  }

  @Test
  public void truncateTable_forMysql_shouldExecuteTruncateTableStatement()
      throws SQLException, ExecutionException {
    truncateTable_forX_shouldExecuteTruncateTableStatement(
        RdbEngine.MYSQL, "TRUNCATE TABLE `my_ns`.`foo_table`");
  }

  @Test
  public void truncateTable_forPostgresql_shouldExecuteTruncateTableStatement()
      throws SQLException, ExecutionException {
    truncateTable_forX_shouldExecuteTruncateTableStatement(
        RdbEngine.POSTGRESQL, "TRUNCATE TABLE \"my_ns\".\"foo_table\"");
  }

  @Test
  public void truncateTable_forSqlServer_shouldExecuteTruncateTableStatement()
      throws SQLException, ExecutionException {
    truncateTable_forX_shouldExecuteTruncateTableStatement(
        RdbEngine.SQL_SERVER, "TRUNCATE TABLE [my_ns].[foo_table]");
  }

  @Test
  public void truncateTable_forOracle_shouldExecuteTruncateTableStatement()
      throws SQLException, ExecutionException {
    truncateTable_forX_shouldExecuteTruncateTableStatement(
        RdbEngine.ORACLE, "TRUNCATE TABLE \"my_ns\".\"foo_table\"");
  }

  @Test
  public void truncateTable_forSqlite_shouldExecuteTruncateTableStatement()
      throws SQLException, ExecutionException {
    truncateTable_forX_shouldExecuteTruncateTableStatement(
        RdbEngine.SQLITE, "DELETE FROM \"my_ns$foo_table\"");
  }

  private void truncateTable_forX_shouldExecuteTruncateTableStatement(
      RdbEngine rdbEngine, String expectedTruncateTableStatement)
      throws SQLException, ExecutionException {
    // Arrange
    String namespace = "my_ns";
    String table = "foo_table";
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    Statement truncateTableStatement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(truncateTableStatement);
    when(dataSource.getConnection()).thenReturn(connection);

    // Act
    admin.truncateTable(namespace, table);

    // Assert
    verify(truncateTableStatement).execute(expectedTruncateTableStatement);
  }

  @Test
  public void dropTable_forMysqlWithNoMoreMetadataAfterDeletion_shouldDropTableAndDeleteMetadata()
      throws Exception {
    dropTable_forXWithNoMoreMetadataAfterDeletion_shouldDropTableAndDeleteMetadata(
        RdbEngine.MYSQL,
        "DROP TABLE `my_ns`.`foo_table`",
        "DELETE FROM `"
            + tableMetadataSchemaName
            + "`.`metadata` WHERE `full_table_name` = 'my_ns.foo_table'",
        "SELECT DISTINCT `full_table_name` FROM `" + tableMetadataSchemaName + "`.`metadata`",
        "DROP TABLE `" + tableMetadataSchemaName + "`.`metadata`",
        "DROP SCHEMA `" + tableMetadataSchemaName + "`");
  }

  @Test
  public void
      dropTable_forPostgresqlWithNoMoreMetadataAfterDeletion_shouldDropTableAndDeleteMetadata()
          throws Exception {
    dropTable_forXWithNoMoreMetadataAfterDeletion_shouldDropTableAndDeleteMetadata(
        RdbEngine.POSTGRESQL,
        "DROP TABLE \"my_ns\".\"foo_table\"",
        "DELETE FROM \""
            + tableMetadataSchemaName
            + "\".\"metadata\" WHERE \"full_table_name\" = 'my_ns.foo_table'",
        "SELECT DISTINCT \"full_table_name\" FROM \"" + tableMetadataSchemaName + "\".\"metadata\"",
        "DROP TABLE \"" + tableMetadataSchemaName + "\".\"metadata\"",
        "DROP SCHEMA \"" + tableMetadataSchemaName + "\"");
  }

  @Test
  public void
      dropTable_forSqlServerWithNoMoreMetadataAfterDeletion_shouldDropTableAndDeleteMetadata()
          throws Exception {
    dropTable_forXWithNoMoreMetadataAfterDeletion_shouldDropTableAndDeleteMetadata(
        RdbEngine.SQL_SERVER,
        "DROP TABLE [my_ns].[foo_table]",
        "DELETE FROM ["
            + tableMetadataSchemaName
            + "].[metadata] WHERE [full_table_name] = 'my_ns.foo_table'",
        "SELECT DISTINCT [full_table_name] FROM [" + tableMetadataSchemaName + "].[metadata]",
        "DROP TABLE [" + tableMetadataSchemaName + "].[metadata]",
        "DROP SCHEMA [" + tableMetadataSchemaName + "]");
  }

  @Test
  public void dropTable_forOracleWithNoMoreMetadataAfterDeletion_shouldDropTableAndDeleteMetadata()
      throws Exception {
    dropTable_forXWithNoMoreMetadataAfterDeletion_shouldDropTableAndDeleteMetadata(
        RdbEngine.ORACLE,
        "DROP TABLE \"my_ns\".\"foo_table\"",
        "DELETE FROM \""
            + tableMetadataSchemaName
            + "\".\"metadata\" WHERE \"full_table_name\" = 'my_ns.foo_table'",
        "SELECT DISTINCT \"full_table_name\" FROM \"" + tableMetadataSchemaName + "\".\"metadata\"",
        "DROP TABLE \"" + tableMetadataSchemaName + "\".\"metadata\"",
        "DROP USER \"" + tableMetadataSchemaName + "\"");
  }

  @Test
  public void dropTable_forSqliteWithNoMoreMetadataAfterDeletion_shouldDropTableAndDeleteMetadata()
      throws Exception {
    dropTable_forXWithNoMoreMetadataAfterDeletion_shouldDropTableAndDeleteMetadata(
        RdbEngine.SQLITE,
        "DROP TABLE \"my_ns$foo_table\"",
        "DELETE FROM \""
            + tableMetadataSchemaName
            + "$metadata\" WHERE \"full_table_name\" = 'my_ns.foo_table'",
        "SELECT DISTINCT \"full_table_name\" FROM \"" + tableMetadataSchemaName + "$metadata\"",
        "DROP TABLE \"" + tableMetadataSchemaName + "$metadata\"");
  }

  private void dropTable_forXWithNoMoreMetadataAfterDeletion_shouldDropTableAndDeleteMetadata(
      RdbEngine rdbEngine, String... expectedSqlStatements) throws Exception {
    // Arrange
    String namespace = "my_ns";
    String table = "foo_table";

    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(false);

    List<Statement> mockedStatements = new ArrayList<>();
    for (String expectedSqlStatement : expectedSqlStatements) {
      Statement mock = mock(Statement.class);
      mockedStatements.add(mock);
      if (expectedSqlStatement.startsWith("SELECT ")) {
        when(mock.executeQuery(any())).thenReturn(resultSet);
      }
    }

    when(connection.createStatement())
        .thenReturn(
            mockedStatements.get(0),
            mockedStatements.subList(1, mockedStatements.size()).toArray(new Statement[0]));
    when(dataSource.getConnection()).thenReturn(connection);

    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    // Act
    admin.dropTable(namespace, table);

    // Assert
    for (int i = 0; i < expectedSqlStatements.length; i++) {
      if (expectedSqlStatements[i].startsWith("SELECT ")) {
        verify(mockedStatements.get(i)).executeQuery(expectedSqlStatements[i]);
      } else {
        verify(mockedStatements.get(i)).execute(expectedSqlStatements[i]);
      }
    }
  }

  @Test
  public void
      dropTable_forMysqlWithOtherMetadataAfterDeletion_ShouldDropTableAndDeleteMetadataButNotMetadataTable()
          throws Exception {
    dropTable_forXWithOtherMetadataAfterDeletion_ShouldDropTableAndDeleteMetadataButNotMetadataTable(
        RdbEngine.MYSQL,
        "DROP TABLE `my_ns`.`foo_table`",
        "DELETE FROM `"
            + tableMetadataSchemaName
            + "`.`metadata` WHERE `full_table_name` = 'my_ns.foo_table'",
        "SELECT DISTINCT `full_table_name` FROM `" + tableMetadataSchemaName + "`.`metadata`");
  }

  @Test
  public void
      dropTable_forPostgresqlWithOtherMetadataAfterDeletion_ShouldDropTableAndDeleteMetadataButNotMetadataTable()
          throws Exception {
    dropTable_forXWithOtherMetadataAfterDeletion_ShouldDropTableAndDeleteMetadataButNotMetadataTable(
        RdbEngine.POSTGRESQL,
        "DROP TABLE \"my_ns\".\"foo_table\"",
        "DELETE FROM \""
            + tableMetadataSchemaName
            + "\".\"metadata\" WHERE \"full_table_name\" = 'my_ns.foo_table'",
        "SELECT DISTINCT \"full_table_name\" FROM \""
            + tableMetadataSchemaName
            + "\".\"metadata\"");
  }

  @Test
  public void
      dropTable_forSqlServerWithOtherMetadataAfterDeletion_ShouldDropTableAndDeleteMetadataButNotMetadataTable()
          throws Exception {
    dropTable_forXWithOtherMetadataAfterDeletion_ShouldDropTableAndDeleteMetadataButNotMetadataTable(
        RdbEngine.SQL_SERVER,
        "DROP TABLE [my_ns].[foo_table]",
        "DELETE FROM ["
            + tableMetadataSchemaName
            + "].[metadata] WHERE [full_table_name] = 'my_ns.foo_table'",
        "SELECT DISTINCT [full_table_name] FROM [" + tableMetadataSchemaName + "].[metadata]");
  }

  @Test
  public void
      dropTable_forOracleWithOtherMetadataAfterDeletion_ShouldDropTableAndDeleteMetadataButNotMetadataTable()
          throws Exception {
    dropTable_forXWithOtherMetadataAfterDeletion_ShouldDropTableAndDeleteMetadataButNotMetadataTable(
        RdbEngine.ORACLE,
        "DROP TABLE \"my_ns\".\"foo_table\"",
        "DELETE FROM \""
            + tableMetadataSchemaName
            + "\".\"metadata\" WHERE \"full_table_name\" = 'my_ns.foo_table'",
        "SELECT DISTINCT \"full_table_name\" FROM \""
            + tableMetadataSchemaName
            + "\".\"metadata\"");
  }

  @Test
  public void
      dropTable_forSqliteWithOtherMetadataAfterDeletion_ShouldDropTableAndDeleteMetadataButNotMetadataTable()
          throws Exception {
    dropTable_forXWithOtherMetadataAfterDeletion_ShouldDropTableAndDeleteMetadataButNotMetadataTable(
        RdbEngine.SQLITE,
        "DROP TABLE \"my_ns$foo_table\"",
        "DELETE FROM \""
            + tableMetadataSchemaName
            + "$metadata\" WHERE \"full_table_name\" = 'my_ns.foo_table'",
        "SELECT DISTINCT \"full_table_name\" FROM \"" + tableMetadataSchemaName + "$metadata\"");
  }

  private void
      dropTable_forXWithOtherMetadataAfterDeletion_ShouldDropTableAndDeleteMetadataButNotMetadataTable(
          RdbEngine rdbEngine, String... expectedSqlStatements) throws Exception {
    // Arrange
    String namespace = "my_ns";
    String table = "foo_table";

    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(true);

    List<Statement> mockedStatements = new ArrayList<>();
    for (String expectedSqlStatement : expectedSqlStatements) {
      Statement mock = mock(Statement.class);
      mockedStatements.add(mock);
      if (expectedSqlStatement.startsWith("SELECT ")) {
        when(mock.executeQuery(any())).thenReturn(resultSet);
      }
    }

    when(connection.createStatement())
        .thenReturn(
            mockedStatements.get(0),
            mockedStatements.subList(1, mockedStatements.size()).toArray(new Statement[0]));
    when(dataSource.getConnection()).thenReturn(connection);

    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    // Act
    admin.dropTable(namespace, table);

    // Assert
    for (int i = 0; i < expectedSqlStatements.length; i++) {
      if (expectedSqlStatements[i].startsWith("SELECT ")) {
        verify(mockedStatements.get(i)).executeQuery(expectedSqlStatements[i]);
      } else {
        verify(mockedStatements.get(i)).execute(expectedSqlStatements[i]);
      }
    }
  }

  @Test
  public void dropNamespace_forMysql_shouldDropNamespace() throws Exception {
    dropSchema_forX_shouldDropSchema(RdbEngine.MYSQL, "DROP SCHEMA `my_ns`");
  }

  @Test
  public void dropNamespace_forPostgresql_shouldDropNamespace() throws Exception {
    dropSchema_forX_shouldDropSchema(RdbEngine.POSTGRESQL, "DROP SCHEMA \"my_ns\"");
  }

  @Test
  public void dropNamespace_forSqlServer_shouldDropNamespace() throws Exception {
    dropSchema_forX_shouldDropSchema(RdbEngine.SQL_SERVER, "DROP SCHEMA [my_ns]");
  }

  @Test
  public void dropNamespace_forOracle_shouldDropNamespace() throws Exception {
    dropSchema_forX_shouldDropSchema(RdbEngine.ORACLE, "DROP USER \"my_ns\"");
  }

  @Test
  public void dropNamespace_forSqlite_shouldDropNamespace() {
    // no SQL is executed
  }

  private void dropSchema_forX_shouldDropSchema(
      RdbEngine rdbEngine, String expectedDropSchemaStatement) throws Exception {
    // Arrange
    String namespace = "my_ns";
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    Connection connection = mock(Connection.class);
    Statement dropSchemaStatement = mock(Statement.class);

    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.createStatement()).thenReturn(dropSchemaStatement);

    // Act
    admin.dropNamespace(namespace);

    // Assert
    verify(dropSchemaStatement).execute(expectedDropSchemaStatement);
  }

  @Test
  public void getNamespaceTables_forMysql_ShouldReturnTableNames() throws Exception {
    getNamespaceTables_forX_ShouldReturnTableNames(
        RdbEngine.MYSQL,
        "SELECT DISTINCT `full_table_name` FROM `"
            + tableMetadataSchemaName
            + "`.`metadata` WHERE `full_table_name` LIKE ?");
  }

  @Test
  public void getNamespaceTables_forPostgresql_ShouldReturnTableNames() throws Exception {
    getNamespaceTables_forX_ShouldReturnTableNames(
        RdbEngine.POSTGRESQL,
        "SELECT DISTINCT \"full_table_name\" FROM \""
            + tableMetadataSchemaName
            + "\".\"metadata\" WHERE \"full_table_name\" LIKE ?");
  }

  @Test
  public void getNamespaceTables_forSqlServer_ShouldReturnTableNames() throws Exception {
    getNamespaceTables_forX_ShouldReturnTableNames(
        RdbEngine.SQL_SERVER,
        "SELECT DISTINCT [full_table_name] FROM ["
            + tableMetadataSchemaName
            + "].[metadata] WHERE [full_table_name] LIKE ?");
  }

  @Test
  public void getNamespaceTables_forOracle_ShouldReturnTableNames() throws Exception {
    getNamespaceTables_forX_ShouldReturnTableNames(
        RdbEngine.ORACLE,
        "SELECT DISTINCT \"full_table_name\" FROM \""
            + tableMetadataSchemaName
            + "\".\"metadata\" WHERE \"full_table_name\" LIKE ?");
  }

  @Test
  public void getNamespaceTables_forSqlite_ShouldReturnTableNames() throws Exception {
    getNamespaceTables_forX_ShouldReturnTableNames(
        RdbEngine.SQLITE,
        "SELECT DISTINCT \"full_table_name\" FROM \""
            + tableMetadataSchemaName
            + "$metadata\" WHERE \"full_table_name\" LIKE ?");
  }

  private void getNamespaceTables_forX_ShouldReturnTableNames(
      RdbEngine rdbEngine, String expectedSelectStatement) throws Exception {
    // Arrange
    String namespace = "ns1";
    String table1 = "t1";
    String table2 = "t2";
    ResultSet resultSet = mock(ResultSet.class);

    // Everytime the ResultSet.next() method will be called, the ResultSet.getXXX methods call be
    // mocked to return the current row data
    doAnswer(
            new GetTablesNamesResultSetMocker(
                Arrays.asList(
                    new GetTablesNamesResultSetMocker.Row(namespace + ".t1"),
                    new GetTablesNamesResultSetMocker.Row(namespace + ".t2"))))
        .when(resultSet)
        .next();
    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    when(preparedStatement.executeQuery()).thenReturn(resultSet);
    when(connection.prepareStatement(any())).thenReturn(preparedStatement);
    when(dataSource.getConnection()).thenReturn(connection);

    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    // Act
    Set<String> actualTableNames = admin.getNamespaceTableNames(namespace);

    // Assert
    verify(connection).prepareStatement(expectedSelectStatement);
    assertThat(actualTableNames).containsExactly(table1, table2);
    verify(preparedStatement).setString(1, namespace + ".%");
  }

  @Test
  public void namespaceExists_forMysqlWithExistingNamespace_shouldReturnTrue() throws Exception {
    namespaceExists_forXWithExistingNamespace_ShouldReturnTrue(
        RdbEngine.MYSQL,
        "SELECT 1 FROM `information_schema`.`schemata` WHERE `schema_name` = ?",
        "");
  }

  @Test
  public void namespaceExists_forPostgresqlWithExistingNamespace_shouldReturnTrue()
      throws Exception {
    namespaceExists_forXWithExistingNamespace_ShouldReturnTrue(
        RdbEngine.POSTGRESQL,
        "SELECT 1 FROM \"information_schema\".\"schemata\" WHERE \"schema_name\" = ?",
        "");
  }

  @Test
  public void namespaceExists_forSqlServerWithExistingNamespace_shouldReturnTrue()
      throws Exception {
    namespaceExists_forXWithExistingNamespace_ShouldReturnTrue(
        RdbEngine.SQL_SERVER, "SELECT 1 FROM [sys].[schemas] WHERE [name] = ?", "");
  }

  @Test
  public void namespaceExists_forOracleWithExistingNamespace_shouldReturnTrue() throws Exception {
    namespaceExists_forXWithExistingNamespace_ShouldReturnTrue(
        RdbEngine.ORACLE, "SELECT 1 FROM \"ALL_USERS\" WHERE \"USERNAME\" = ?", "");
  }

  @Test
  public void namespaceExists_forSqliteWithExistingNamespace_shouldReturnTrue() throws Exception {
    namespaceExists_forXWithExistingNamespace_ShouldReturnTrue(
        RdbEngine.SQLITE,
        "SELECT 1 FROM sqlite_master WHERE \"type\" = \"table\" AND \"tbl_name\" LIKE ?",
        "$%");
  }

  private void namespaceExists_forXWithExistingNamespace_ShouldReturnTrue(
      RdbEngine rdbEngine, String expectedSelectStatement, String namespacePlaceholderSuffix)
      throws SQLException, ExecutionException {
    // Arrange
    String namespace = "my_ns";
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    Connection connection = mock(Connection.class);
    PreparedStatement selectStatement = mock(PreparedStatement.class);
    ResultSet results = mock(ResultSet.class);

    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.prepareStatement(any())).thenReturn(selectStatement);
    when(results.next()).thenReturn(true);
    when(selectStatement.executeQuery()).thenReturn(results);

    // Act
    // Assert
    assertThat(admin.namespaceExists(namespace)).isTrue();

    verify(selectStatement).executeQuery();
    verify(connection).prepareStatement(expectedSelectStatement);
    verify(selectStatement).setString(1, namespace + namespacePlaceholderSuffix);
  }

  @Test
  public void createIndex_ForColumnTypeWithoutRequiredAlterationForMysql_ShouldCreateIndexProperly()
      throws Exception {
    createIndex_ForColumnTypeWithoutRequiredAlterationForX_ShouldCreateIndexProperly(
        RdbEngine.MYSQL,
        "SELECT `column_name`,`data_type`,`key_type`,`clustering_order`,`indexed` FROM `"
            + tableMetadataSchemaName
            + "`.`metadata` WHERE `full_table_name`=? ORDER BY `ordinal_position` ASC",
        "CREATE INDEX `index_my_ns_my_tbl_my_column` ON `my_ns`.`my_tbl` (`my_column`)",
        "UPDATE `"
            + tableMetadataSchemaName
            + "`.`metadata` SET `indexed`=true WHERE `full_table_name`='my_ns.my_tbl' AND `column_name`='my_column'");
  }

  @Test
  public void
      createIndex_ForColumnTypeWithoutRequiredAlterationForPostgresql_ShouldCreateIndexProperly()
          throws Exception {
    createIndex_ForColumnTypeWithoutRequiredAlterationForX_ShouldCreateIndexProperly(
        RdbEngine.POSTGRESQL,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + tableMetadataSchemaName
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "CREATE INDEX \"index_my_ns_my_tbl_my_column\" ON \"my_ns\".\"my_tbl\" (\"my_column\")",
        "UPDATE \""
            + tableMetadataSchemaName
            + "\".\"metadata\" SET \"indexed\"=true WHERE \"full_table_name\"='my_ns.my_tbl' AND \"column_name\"='my_column'");
  }

  @Test
  public void
      createIndex_ForColumnTypeWithoutRequiredAlterationForSqlServer_ShouldCreateIndexProperly()
          throws Exception {
    createIndex_ForColumnTypeWithoutRequiredAlterationForX_ShouldCreateIndexProperly(
        RdbEngine.SQL_SERVER,
        "SELECT [column_name],[data_type],[key_type],[clustering_order],[indexed] FROM ["
            + tableMetadataSchemaName
            + "].[metadata] WHERE [full_table_name]=? ORDER BY [ordinal_position] ASC",
        "CREATE INDEX [index_my_ns_my_tbl_my_column] ON [my_ns].[my_tbl] ([my_column])",
        "UPDATE ["
            + tableMetadataSchemaName
            + "].[metadata] SET [indexed]=1 WHERE [full_table_name]='my_ns.my_tbl' AND [column_name]='my_column'");
  }

  @Test
  public void
      createIndex_ForColumnTypeWithoutRequiredAlterationForOracle_ShouldCreateIndexProperly()
          throws Exception {
    createIndex_ForColumnTypeWithoutRequiredAlterationForX_ShouldCreateIndexProperly(
        RdbEngine.ORACLE,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + tableMetadataSchemaName
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "CREATE INDEX \"index_my_ns_my_tbl_my_column\" ON \"my_ns\".\"my_tbl\" (\"my_column\")",
        "UPDATE \""
            + tableMetadataSchemaName
            + "\".\"metadata\" SET \"indexed\"=1 WHERE \"full_table_name\"='my_ns.my_tbl' AND \"column_name\"='my_column'");
  }

  @Test
  public void
      createIndex_ForColumnTypeWithoutRequiredAlterationForSqlite_ShouldCreateIndexProperly()
          throws Exception {
    createIndex_ForColumnTypeWithoutRequiredAlterationForX_ShouldCreateIndexProperly(
        RdbEngine.SQLITE,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + tableMetadataSchemaName
            + "$metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "CREATE INDEX \"index_my_ns_my_tbl_my_column\" ON \"my_ns$my_tbl\" (\"my_column\")",
        "UPDATE \""
            + tableMetadataSchemaName
            + "$metadata\" SET \"indexed\"=TRUE WHERE \"full_table_name\"='my_ns.my_tbl' AND \"column_name\"='my_column'");
  }

  private void createIndex_ForColumnTypeWithoutRequiredAlterationForX_ShouldCreateIndexProperly(
      RdbEngine rdbEngine,
      String expectedGetTableMetadataStatement,
      String expectedCreateIndexStatement,
      String expectedUpdateTableMetadataStatement)
      throws SQLException, ExecutionException {
    // Arrange
    String namespace = "my_ns";
    String table = "my_tbl";
    String indexColumn = "my_column";
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    PreparedStatement checkStatement = prepareStatementForNamespaceCheck();
    PreparedStatement selectStatement = mock(PreparedStatement.class);
    ResultSet resultSet =
        mockResultSet(
            Arrays.asList(
                new GetColumnsResultSetMocker.Row(
                    "c1", DataType.BOOLEAN.toString(), "PARTITION", null, false),
                new GetColumnsResultSetMocker.Row(
                    indexColumn, DataType.BOOLEAN.toString(), null, null, false)));
    when(selectStatement.executeQuery()).thenReturn(resultSet);
    when(connection.prepareStatement(any())).thenReturn(checkStatement).thenReturn(selectStatement);
    Statement statement = mock(Statement.class);

    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.createStatement()).thenReturn(statement);

    // Act
    admin.createIndex(namespace, table, indexColumn, Collections.emptyMap());

    // Assert
    verify(connection).prepareStatement(expectedGetTableMetadataStatement);
    verify(selectStatement).setString(1, getFullTableName(namespace, table));

    verify(connection, times(2)).createStatement();
    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(statement, times(2)).execute(captor.capture());
    assertThat(captor.getAllValues().get(0)).isEqualTo(expectedCreateIndexStatement);
    assertThat(captor.getAllValues().get(1)).isEqualTo(expectedUpdateTableMetadataStatement);
  }

  @Test
  public void
      createIndex_forColumnTypeWithRequiredAlterationForMysql_ShouldAlterColumnAndCreateIndexProperly()
          throws Exception {
    createIndex_forColumnTypeWithRequiredAlterationForX_ShouldAlterColumnAndCreateIndexProperly(
        RdbEngine.MYSQL,
        "SELECT `column_name`,`data_type`,`key_type`,`clustering_order`,`indexed` FROM `"
            + tableMetadataSchemaName
            + "`.`metadata` WHERE `full_table_name`=? ORDER BY `ordinal_position` ASC",
        "ALTER TABLE `my_ns`.`my_tbl` MODIFY`my_column` VARCHAR(64)",
        "CREATE INDEX `index_my_ns_my_tbl_my_column` ON `my_ns`.`my_tbl` (`my_column`)",
        "UPDATE `"
            + tableMetadataSchemaName
            + "`.`metadata` SET `indexed`=true WHERE `full_table_name`='my_ns.my_tbl' AND `column_name`='my_column'");
  }

  @Test
  public void
      createIndex_forColumnTypeWithRequiredAlterationForPostgresql_ShouldAlterColumnAndCreateIndexProperly()
          throws Exception {
    createIndex_forColumnTypeWithRequiredAlterationForX_ShouldAlterColumnAndCreateIndexProperly(
        RdbEngine.POSTGRESQL,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + tableMetadataSchemaName
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "ALTER TABLE \"my_ns\".\"my_tbl\" ALTER COLUMN\"my_column\" TYPE VARCHAR(10485760)",
        "CREATE INDEX \"index_my_ns_my_tbl_my_column\" ON \"my_ns\".\"my_tbl\" (\"my_column\")",
        "UPDATE \""
            + tableMetadataSchemaName
            + "\".\"metadata\" SET \"indexed\"=true WHERE \"full_table_name\"='my_ns.my_tbl' AND \"column_name\"='my_column'");
  }

  @Test
  public void
      createIndex_forColumnTypeWithRequiredAlterationForOracle_ShouldAlterColumnAndCreateIndexProperly()
          throws Exception {
    createIndex_forColumnTypeWithRequiredAlterationForX_ShouldAlterColumnAndCreateIndexProperly(
        RdbEngine.ORACLE,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + tableMetadataSchemaName
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "ALTER TABLE \"my_ns\".\"my_tbl\" MODIFY ( \"my_column\" VARCHAR2(64) )",
        "CREATE INDEX \"index_my_ns_my_tbl_my_column\" ON \"my_ns\".\"my_tbl\" (\"my_column\")",
        "UPDATE \""
            + tableMetadataSchemaName
            + "\".\"metadata\" SET \"indexed\"=1 WHERE \"full_table_name\"='my_ns.my_tbl' AND \"column_name\"='my_column'");
  }

  @Test
  public void
      createIndex_forColumnTypeWithRequiredAlterationForSqlite_ShouldAlterColumnAndCreateIndexProperly() {
    // SQLite does not require column type change on CREATE INDEX.
  }

  private void
      createIndex_forColumnTypeWithRequiredAlterationForX_ShouldAlterColumnAndCreateIndexProperly(
          RdbEngine rdbEngine,
          String expectedGetTableMetadataStatement,
          String expectedAlterColumnTypeStatement,
          String expectedCreateIndexStatement,
          String expectedUpdateTableMetadataStatement)
          throws SQLException, ExecutionException {
    // Arrange
    String namespace = "my_ns";
    String table = "my_tbl";
    String indexColumn = "my_column";
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    PreparedStatement checkStatement = prepareStatementForNamespaceCheck();
    PreparedStatement selectStatement = mock(PreparedStatement.class);
    ResultSet resultSet =
        mockResultSet(
            Arrays.asList(
                new GetColumnsResultSetMocker.Row(
                    "c1", DataType.BOOLEAN.toString(), "PARTITION", null, false),
                new GetColumnsResultSetMocker.Row(
                    indexColumn, DataType.TEXT.toString(), null, null, false)));
    when(selectStatement.executeQuery()).thenReturn(resultSet);
    when(connection.prepareStatement(any())).thenReturn(checkStatement).thenReturn(selectStatement);

    Statement statement = mock(Statement.class);

    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.createStatement()).thenReturn(statement);

    // Act
    admin.createIndex(namespace, table, indexColumn, Collections.emptyMap());

    // Assert
    verify(connection).prepareStatement(expectedGetTableMetadataStatement);
    verify(selectStatement).setString(1, getFullTableName(namespace, table));

    verify(connection, times(3)).createStatement();
    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(statement, times(3)).execute(captor.capture());
    assertThat(captor.getAllValues().get(0)).isEqualTo(expectedAlterColumnTypeStatement);
    assertThat(captor.getAllValues().get(1)).isEqualTo(expectedCreateIndexStatement);
    assertThat(captor.getAllValues().get(2)).isEqualTo(expectedUpdateTableMetadataStatement);
  }

  @Test
  public void dropIndex_forColumnTypeWithoutRequiredAlterationForMysql_ShouldDropIndexProperly()
      throws Exception {
    dropIndex_forColumnTypeWithoutRequiredAlterationForX_ShouldDropIndexProperly(
        RdbEngine.MYSQL,
        "SELECT `column_name`,`data_type`,`key_type`,`clustering_order`,`indexed` FROM `"
            + tableMetadataSchemaName
            + "`.`metadata` WHERE `full_table_name`=? ORDER BY `ordinal_position` ASC",
        "DROP INDEX `index_my_ns_my_tbl_my_column` ON `my_ns`.`my_tbl`",
        "UPDATE `"
            + tableMetadataSchemaName
            + "`.`metadata` SET `indexed`=false WHERE `full_table_name`='my_ns.my_tbl' AND `column_name`='my_column'");
  }

  @Test
  public void
      dropIndex_forColumnTypeWithoutRequiredAlterationForPostgresql_ShouldDropIndexProperly()
          throws Exception {
    dropIndex_forColumnTypeWithoutRequiredAlterationForX_ShouldDropIndexProperly(
        RdbEngine.POSTGRESQL,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + tableMetadataSchemaName
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "DROP INDEX \"my_ns\".\"index_my_ns_my_tbl_my_column\"",
        "UPDATE \""
            + tableMetadataSchemaName
            + "\".\"metadata\" SET \"indexed\"=false WHERE \"full_table_name\"='my_ns.my_tbl' AND \"column_name\"='my_column'");
  }

  @Test
  public void dropIndex_forColumnTypeWithoutRequiredAlterationForServer_ShouldDropIndexProperly()
      throws Exception {
    dropIndex_forColumnTypeWithoutRequiredAlterationForX_ShouldDropIndexProperly(
        RdbEngine.SQL_SERVER,
        "SELECT [column_name],[data_type],[key_type],[clustering_order],[indexed] FROM ["
            + tableMetadataSchemaName
            + "].[metadata] WHERE [full_table_name]=? ORDER BY [ordinal_position] ASC",
        "DROP INDEX [index_my_ns_my_tbl_my_column] ON [my_ns].[my_tbl]",
        "UPDATE ["
            + tableMetadataSchemaName
            + "].[metadata] SET [indexed]=0 WHERE [full_table_name]='my_ns.my_tbl' AND [column_name]='my_column'");
  }

  @Test
  public void dropIndex_forColumnTypeWithoutRequiredAlterationForOracle_ShouldDropIndexProperly()
      throws Exception {
    dropIndex_forColumnTypeWithoutRequiredAlterationForX_ShouldDropIndexProperly(
        RdbEngine.ORACLE,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + tableMetadataSchemaName
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "DROP INDEX \"index_my_ns_my_tbl_my_column\"",
        "UPDATE \""
            + tableMetadataSchemaName
            + "\".\"metadata\" SET \"indexed\"=0 WHERE \"full_table_name\"='my_ns.my_tbl' AND \"column_name\"='my_column'");
  }

  @Test
  public void dropIndex_forColumnTypeWithoutRequiredAlterationForSqlite_ShouldDropIndexProperly()
      throws Exception {
    dropIndex_forColumnTypeWithoutRequiredAlterationForX_ShouldDropIndexProperly(
        RdbEngine.SQLITE,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + tableMetadataSchemaName
            + "$metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "DROP INDEX \"index_my_ns_my_tbl_my_column\"",
        "UPDATE \""
            + tableMetadataSchemaName
            + "$metadata\" SET \"indexed\"=FALSE WHERE \"full_table_name\"='my_ns.my_tbl' AND \"column_name\"='my_column'");
  }

  private void dropIndex_forColumnTypeWithoutRequiredAlterationForX_ShouldDropIndexProperly(
      RdbEngine rdbEngine,
      String expectedGetTableMetadataStatement,
      String expectedDropIndexStatement,
      String expectedUpdateTableMetadataStatement)
      throws SQLException, ExecutionException {
    // Arrange
    String namespace = "my_ns";
    String table = "my_tbl";
    String indexColumn = "my_column";
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);
    PreparedStatement checkStatement = prepareStatementForNamespaceCheck();
    PreparedStatement selectStatement = mock(PreparedStatement.class);
    ResultSet resultSet =
        mockResultSet(
            Arrays.asList(
                new GetColumnsResultSetMocker.Row(
                    "c1", DataType.BOOLEAN.toString(), "PARTITION", null, false),
                new GetColumnsResultSetMocker.Row(
                    indexColumn, DataType.BOOLEAN.toString(), null, null, false)));
    when(selectStatement.executeQuery()).thenReturn(resultSet);
    when(connection.prepareStatement(any())).thenReturn(checkStatement).thenReturn(selectStatement);

    Statement statement = mock(Statement.class);

    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.createStatement()).thenReturn(statement);

    // Act
    admin.dropIndex(namespace, table, indexColumn);

    // Assert
    verify(connection).prepareStatement(expectedGetTableMetadataStatement);
    verify(selectStatement).setString(1, getFullTableName(namespace, table));

    verify(connection, times(2)).createStatement();
    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(statement, times(2)).execute(captor.capture());
    assertThat(captor.getAllValues().get(0)).isEqualTo(expectedDropIndexStatement);
    assertThat(captor.getAllValues().get(1)).isEqualTo(expectedUpdateTableMetadataStatement);
  }

  @Test
  public void dropIndex_forColumnTypeWithRequiredAlterationForMysql_ShouldDropIndexProperly()
      throws Exception {
    dropIndex_forColumnTypeWithRequiredAlterationForX_ShouldDropIndexProperly(
        RdbEngine.MYSQL,
        "SELECT `column_name`,`data_type`,`key_type`,`clustering_order`,`indexed` FROM `"
            + tableMetadataSchemaName
            + "`.`metadata` WHERE `full_table_name`=? ORDER BY `ordinal_position` ASC",
        "DROP INDEX `index_my_ns_my_tbl_my_column` ON `my_ns`.`my_tbl`",
        "ALTER TABLE `my_ns`.`my_tbl` MODIFY`my_column` LONGTEXT",
        "UPDATE `"
            + tableMetadataSchemaName
            + "`.`metadata` SET `indexed`=false WHERE `full_table_name`='my_ns.my_tbl' AND `column_name`='my_column'");
  }

  @Test
  public void dropIndex_forColumnTypeWithRequiredAlterationForPostgresql_ShouldDropIndexProperly()
      throws Exception {
    dropIndex_forColumnTypeWithRequiredAlterationForX_ShouldDropIndexProperly(
        RdbEngine.POSTGRESQL,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + tableMetadataSchemaName
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "DROP INDEX \"my_ns\".\"index_my_ns_my_tbl_my_column\"",
        "ALTER TABLE \"my_ns\".\"my_tbl\" ALTER COLUMN\"my_column\" TYPE TEXT",
        "UPDATE \""
            + tableMetadataSchemaName
            + "\".\"metadata\" SET \"indexed\"=false WHERE \"full_table_name\"='my_ns.my_tbl' AND \"column_name\"='my_column'");
  }

  @Test
  public void dropIndex_forColumnTypeWithRequiredAlterationForOracle_ShouldDropIndexProperly()
      throws Exception {
    dropIndex_forColumnTypeWithRequiredAlterationForX_ShouldDropIndexProperly(
        RdbEngine.ORACLE,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + tableMetadataSchemaName
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "DROP INDEX \"index_my_ns_my_tbl_my_column\"",
        "ALTER TABLE \"my_ns\".\"my_tbl\" MODIFY ( \"my_column\" VARCHAR2(4000) )",
        "UPDATE \""
            + tableMetadataSchemaName
            + "\".\"metadata\" SET \"indexed\"=0 WHERE \"full_table_name\"='my_ns.my_tbl' AND \"column_name\"='my_column'");
  }

  @Test
  public void dropIndex_forColumnTypeWithRequiredAlterationForSqlite_ShouldDropIndexProperly() {
    // SQLite does not require column type change on CREATE INDEX.
  }

  private void dropIndex_forColumnTypeWithRequiredAlterationForX_ShouldDropIndexProperly(
      RdbEngine rdbEngine,
      String expectedGetTableMetadataStatement,
      String expectedDropIndexStatement,
      String expectedAlterColumnStatement,
      String expectedUpdateTableMetadataStatement)
      throws SQLException, ExecutionException {
    // Arrange
    String namespace = "my_ns";
    String table = "my_tbl";
    String indexColumn = "my_column";
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);
    PreparedStatement checkStatement = prepareStatementForNamespaceCheck();
    PreparedStatement selectStatement = mock(PreparedStatement.class);
    ResultSet resultSet =
        mockResultSet(
            Arrays.asList(
                new GetColumnsResultSetMocker.Row(
                    "c1", DataType.BOOLEAN.toString(), "PARTITION", null, false),
                new GetColumnsResultSetMocker.Row(
                    indexColumn, DataType.TEXT.toString(), null, null, false)));
    when(selectStatement.executeQuery()).thenReturn(resultSet);
    when(connection.prepareStatement(any())).thenReturn(checkStatement).thenReturn(selectStatement);

    Statement statement = mock(Statement.class);

    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.createStatement()).thenReturn(statement);

    // Act
    admin.dropIndex(namespace, table, indexColumn);

    // Assert
    verify(connection).prepareStatement(expectedGetTableMetadataStatement);
    verify(selectStatement).setString(1, getFullTableName(namespace, table));

    verify(connection, times(3)).createStatement();
    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(statement, times(3)).execute(captor.capture());
    assertThat(captor.getAllValues().get(0)).isEqualTo(expectedDropIndexStatement);
    assertThat(captor.getAllValues().get(1)).isEqualTo(expectedAlterColumnStatement);
    assertThat(captor.getAllValues().get(2)).isEqualTo(expectedUpdateTableMetadataStatement);
  }

  @Test
  public void
      repairTable_WithMissingMetadataTableForMysql_shouldCreateMetadataTableAndAddMetadataForTable()
          throws SQLException, ExecutionException {
    repairTable_WithMissingMetadataTableForX_shouldCreateMetadataTableAndAddMetadataForTable(
        RdbEngine.MYSQL,
        "SELECT 1 FROM `my_ns`.`foo_table` LIMIT 1",
        "SELECT 1 FROM `" + tableMetadataSchemaName + "`.`metadata` LIMIT 1",
        "CREATE SCHEMA IF NOT EXISTS `" + tableMetadataSchemaName + "`",
        "CREATE TABLE IF NOT EXISTS `"
            + tableMetadataSchemaName
            + "`.`metadata`(`full_table_name` VARCHAR(128),`column_name` VARCHAR(128),`data_type` VARCHAR(20) NOT NULL,`key_type` VARCHAR(20),`clustering_order` VARCHAR(10),`indexed` BOOLEAN NOT NULL,`ordinal_position` INTEGER NOT NULL,PRIMARY KEY (`full_table_name`, `column_name`))",
        "INSERT INTO `"
            + tableMetadataSchemaName
            + "`.`metadata` VALUES ('my_ns.foo_table','c1','TEXT','PARTITION',NULL,false,1)");
  }

  @Test
  public void
      repairTable_WithMissingMetadataTableForOracle_shouldCreateMetadataTableAndAddMetadataForTable()
          throws SQLException, ExecutionException {
    repairTable_WithMissingMetadataTableForX_shouldCreateMetadataTableAndAddMetadataForTable(
        RdbEngine.ORACLE,
        "SELECT 1 FROM \"my_ns\".\"foo_table\" FETCH FIRST 1 ROWS ONLY",
        "SELECT 1 FROM \"" + tableMetadataSchemaName + "\".\"metadata\" FETCH FIRST 1 ROWS ONLY",
        "CREATE USER \"" + tableMetadataSchemaName + "\" IDENTIFIED BY \"oracle\"",
        "ALTER USER \"" + tableMetadataSchemaName + "\" quota unlimited on USERS",
        "CREATE TABLE \""
            + tableMetadataSchemaName
            + "\".\"metadata\"(\"full_table_name\" VARCHAR2(128),\"column_name\" VARCHAR2(128),\"data_type\" VARCHAR2(20) NOT NULL,\"key_type\" VARCHAR2(20),\"clustering_order\" VARCHAR2(10),\"indexed\" NUMBER(1) NOT NULL,\"ordinal_position\" INTEGER NOT NULL,PRIMARY KEY (\"full_table_name\", \"column_name\"))",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c1','TEXT','PARTITION',NULL,0,1)");
  }

  @Test
  public void
      repairTable_WithMissingMetadataTableForPostgresql_shouldCreateMetadataTableAndAddMetadataForTable()
          throws SQLException, ExecutionException {
    repairTable_WithMissingMetadataTableForX_shouldCreateMetadataTableAndAddMetadataForTable(
        RdbEngine.POSTGRESQL,
        "SELECT 1 FROM \"my_ns\".\"foo_table\" LIMIT 1",
        "SELECT 1 FROM \"" + tableMetadataSchemaName + "\".\"metadata\" LIMIT 1",
        "CREATE SCHEMA IF NOT EXISTS \"" + tableMetadataSchemaName + "\"",
        "CREATE TABLE IF NOT EXISTS \""
            + tableMetadataSchemaName
            + "\".\"metadata\"(\"full_table_name\" VARCHAR(128),\"column_name\" VARCHAR(128),\"data_type\" VARCHAR(20) NOT NULL,\"key_type\" VARCHAR(20),\"clustering_order\" VARCHAR(10),\"indexed\" BOOLEAN NOT NULL,\"ordinal_position\" INTEGER NOT NULL,PRIMARY KEY (\"full_table_name\", \"column_name\"))",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c1','TEXT','PARTITION',NULL,false,1)");
  }

  @Test
  public void
      repairTable_WithMissingMetadataTableForSqlServer_shouldCreateMetadataTableAndAddMetadataForTable()
          throws SQLException, ExecutionException {
    repairTable_WithMissingMetadataTableForX_shouldCreateMetadataTableAndAddMetadataForTable(
        RdbEngine.SQL_SERVER,
        "SELECT TOP 1 1 FROM [my_ns].[foo_table]",
        "SELECT TOP 1 1 FROM [" + tableMetadataSchemaName + "].[metadata]",
        "CREATE SCHEMA [" + tableMetadataSchemaName + "]",
        "CREATE TABLE ["
            + tableMetadataSchemaName
            + "].[metadata]([full_table_name] VARCHAR(128),[column_name] VARCHAR(128),[data_type] VARCHAR(20) NOT NULL,[key_type] VARCHAR(20),[clustering_order] VARCHAR(10),[indexed] BIT NOT NULL,[ordinal_position] INTEGER NOT NULL,PRIMARY KEY ([full_table_name], [column_name]))",
        "INSERT INTO ["
            + tableMetadataSchemaName
            + "].[metadata] VALUES ('my_ns.foo_table','c1','TEXT','PARTITION',NULL,0,1)");
  }

  @Test
  public void
      repairTable_WithMissingMetadataTableForSqlite_shouldCreateMetadataTableAndAddMetadataForTable()
          throws SQLException, ExecutionException {
    repairTable_WithMissingMetadataTableForX_shouldCreateMetadataTableAndAddMetadataForTable(
        RdbEngine.SQLITE,
        "SELECT 1 FROM \"my_ns$foo_table\" LIMIT 1",
        "SELECT 1 FROM \"" + tableMetadataSchemaName + "$metadata\" LIMIT 1",
        "CREATE TABLE IF NOT EXISTS \""
            + tableMetadataSchemaName
            + "$metadata\"(\"full_table_name\" TEXT,\"column_name\" TEXT,\"data_type\" TEXT NOT NULL,\"key_type\" TEXT,\"clustering_order\" TEXT,\"indexed\" BOOLEAN NOT NULL,\"ordinal_position\" INTEGER NOT NULL,PRIMARY KEY (\"full_table_name\", \"column_name\"))",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "$metadata\" VALUES ('my_ns.foo_table','c1','TEXT','PARTITION',NULL,FALSE,1)");
  }

  private void
      repairTable_WithMissingMetadataTableForX_shouldCreateMetadataTableAndAddMetadataForTable(
          RdbEngine rdbEngine, String... expectedSqlStatements)
          throws SQLException, ExecutionException {
    // Arrange
    String namespace = "my_ns";
    String table = "foo_table";
    TableMetadata metadata =
        TableMetadata.newBuilder().addPartitionKey("c1").addColumn("c1", DataType.TEXT).build();

    List<Statement> mockedStatements = new ArrayList<>();
    for (int i = 0; i < expectedSqlStatements.length; i++) {
      mockedStatements.add(mock(Statement.class));
    }

    when(connection.createStatement())
        .thenReturn(
            mockedStatements.get(0),
            mockedStatements.subList(1, mockedStatements.size()).toArray(new Statement[0]));
    when(dataSource.getConnection()).thenReturn(connection);

    // Mock that the metadata table does not exist
    SQLException sqlException = mock(SQLException.class);
    mockUndefinedTableError(rdbEngine, sqlException);
    when(mockedStatements.get(1).execute(anyString())).thenThrow(sqlException);

    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    // Act
    admin.repairTable(namespace, table, metadata, new HashMap<>());

    // Assert
    for (int i = 0; i < expectedSqlStatements.length; i++) {
      verify(mockedStatements.get(i)).execute(expectedSqlStatements[i]);
    }
  }

  @Test
  public void repairTable_ExistingMetadataTableForMysql_shouldDeleteThenAddMetadataForTable()
      throws SQLException, ExecutionException {
    repairTable_ExistingMetadataTableForX_shouldDeleteThenAddMetadataForTable(
        RdbEngine.MYSQL,
        "SELECT 1 FROM `my_ns`.`foo_table` LIMIT 1",
        "SELECT 1 FROM `" + tableMetadataSchemaName + "`.`metadata` LIMIT 1",
        "DELETE FROM `"
            + tableMetadataSchemaName
            + "`.`metadata` WHERE `full_table_name` = 'my_ns.foo_table'",
        "INSERT INTO `"
            + tableMetadataSchemaName
            + "`.`metadata` VALUES ('my_ns.foo_table','c1','TEXT','PARTITION',NULL,false,1)");
  }

  @Test
  public void repairTable_ExistingMetadataTableForOracle_shouldDeleteThenAddMetadataForTable()
      throws SQLException, ExecutionException {
    repairTable_ExistingMetadataTableForX_shouldDeleteThenAddMetadataForTable(
        RdbEngine.ORACLE,
        "SELECT 1 FROM \"my_ns\".\"foo_table\" FETCH FIRST 1 ROWS ONLY",
        "SELECT 1 FROM \"" + tableMetadataSchemaName + "\".\"metadata\" FETCH FIRST 1 ROWS ONLY",
        "DELETE FROM \""
            + tableMetadataSchemaName
            + "\".\"metadata\" WHERE \"full_table_name\" = 'my_ns.foo_table'",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c1','TEXT','PARTITION',NULL,0,1)");
  }

  @Test
  public void repairTable_ExistingMetadataTableForPosgresql_shouldDeleteThenAddMetadataForTable()
      throws SQLException, ExecutionException {
    repairTable_ExistingMetadataTableForX_shouldDeleteThenAddMetadataForTable(
        RdbEngine.POSTGRESQL,
        "SELECT 1 FROM \"my_ns\".\"foo_table\" LIMIT 1",
        "SELECT 1 FROM \"" + tableMetadataSchemaName + "\".\"metadata\" LIMIT 1",
        "DELETE FROM \""
            + tableMetadataSchemaName
            + "\".\"metadata\" WHERE \"full_table_name\" = 'my_ns.foo_table'",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c1','TEXT','PARTITION',NULL,false,1)");
  }

  @Test
  public void repairTable_ExistingMetadataTableForSqlServer_shouldDeleteThenAddMetadataForTable()
      throws SQLException, ExecutionException {
    repairTable_ExistingMetadataTableForX_shouldDeleteThenAddMetadataForTable(
        RdbEngine.SQL_SERVER,
        "SELECT TOP 1 1 FROM [my_ns].[foo_table]",
        "SELECT TOP 1 1 FROM [" + tableMetadataSchemaName + "].[metadata]",
        "DELETE FROM ["
            + tableMetadataSchemaName
            + "].[metadata] WHERE [full_table_name] = 'my_ns.foo_table'",
        "INSERT INTO ["
            + tableMetadataSchemaName
            + "].[metadata] VALUES ('my_ns.foo_table','c1','TEXT','PARTITION',NULL,0,1)");
  }

  @Test
  public void repairTable_ExistingMetadataTableForSqlite_shouldDeleteThenAddMetadataForTable()
      throws SQLException, ExecutionException {
    repairTable_ExistingMetadataTableForX_shouldDeleteThenAddMetadataForTable(
        RdbEngine.SQLITE,
        "SELECT 1 FROM \"my_ns$foo_table\" LIMIT 1",
        "SELECT 1 FROM \"" + tableMetadataSchemaName + "$metadata\" LIMIT 1",
        "DELETE FROM \""
            + tableMetadataSchemaName
            + "$metadata\" WHERE \"full_table_name\" = 'my_ns.foo_table'",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "$metadata\" VALUES ('my_ns.foo_table','c1','TEXT','PARTITION',NULL,FALSE,1)");
  }

  private void repairTable_ExistingMetadataTableForX_shouldDeleteThenAddMetadataForTable(
      RdbEngine rdbEngine, String... expectedSqlStatements)
      throws SQLException, ExecutionException {
    // Arrange
    String namespace = "my_ns";
    String table = "foo_table";
    TableMetadata metadata =
        TableMetadata.newBuilder().addPartitionKey("c1").addColumn("c1", DataType.TEXT).build();

    List<Statement> mockedStatements = new ArrayList<>();
    for (int i = 0; i < expectedSqlStatements.length; i++) {
      mockedStatements.add(mock(Statement.class));
    }

    when(connection.createStatement())
        .thenReturn(
            mockedStatements.get(0),
            mockedStatements.subList(1, mockedStatements.size()).toArray(new Statement[0]));
    when(dataSource.getConnection()).thenReturn(connection);

    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    // Act
    admin.repairTable(namespace, table, metadata, new HashMap<>());

    // Assert
    for (int i = 0; i < expectedSqlStatements.length; i++) {
      verify(mockedStatements.get(i)).execute(expectedSqlStatements[i]);
    }
  }

  @Test
  public void repairTable_WithNonExistingTableToRepairForMysql_shouldThrowIllegalArgumentException()
      throws SQLException {
    repairTable_WithNonExistingTableToRepairForX_shouldThrowIllegalArgumentException(
        RdbEngine.MYSQL, "SELECT 1 FROM `my_ns`.`foo_table` LIMIT 1");
  }

  @Test
  public void
      repairTable_WithNonExistingTableToRepairForOracle_shouldThrowIllegalArgumentException()
          throws SQLException {
    repairTable_WithNonExistingTableToRepairForX_shouldThrowIllegalArgumentException(
        RdbEngine.ORACLE, "SELECT 1 FROM \"my_ns\".\"foo_table\" FETCH FIRST 1 ROWS ONLY");
  }

  @Test
  public void
      repairTable_WithNonExistingTableToRepairForPostgresql_shouldThrowIllegalArgumentException()
          throws SQLException {
    repairTable_WithNonExistingTableToRepairForX_shouldThrowIllegalArgumentException(
        RdbEngine.POSTGRESQL, "SELECT 1 FROM \"my_ns\".\"foo_table\" LIMIT 1");
  }

  @Test
  public void
      repairTable_WithNonExistingTableToRepairForSqlServer_shouldThrowIllegalArgumentException()
          throws SQLException {
    repairTable_WithNonExistingTableToRepairForX_shouldThrowIllegalArgumentException(
        RdbEngine.SQL_SERVER, "SELECT TOP 1 1 FROM [my_ns].[foo_table]");
  }

  @Test
  public void
      repairTable_WithNonExistingTableToRepairForSqlite_shouldThrowIllegalArgumentException()
          throws SQLException {
    repairTable_WithNonExistingTableToRepairForX_shouldThrowIllegalArgumentException(
        RdbEngine.SQLITE, "SELECT 1 FROM \"my_ns$foo_table\" LIMIT 1");
  }

  private void repairTable_WithNonExistingTableToRepairForX_shouldThrowIllegalArgumentException(
      RdbEngine rdbEngine, String expectedCheckTableExistStatement) throws SQLException {
    // Arrange
    String namespace = "my_ns";
    String table = "foo_table";
    TableMetadata metadata =
        TableMetadata.newBuilder().addPartitionKey("c1").addColumn("c1", DataType.TEXT).build();

    Statement checkTableExistStatement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(checkTableExistStatement);
    when(dataSource.getConnection()).thenReturn(connection);

    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);
    SQLException sqlException = mock(SQLException.class);
    mockUndefinedTableError(rdbEngine, sqlException);
    when(checkTableExistStatement.execute(any())).thenThrow(sqlException);

    // Act
    assertThatThrownBy(() -> admin.repairTable(namespace, table, metadata, new HashMap<>()))
        .isInstanceOf(IllegalArgumentException.class);

    // Assert
    verify(checkTableExistStatement).execute(expectedCheckTableExistStatement);
  }

  @Test
  public void addNewColumnToTable_ForMysql_ShouldWorkProperly()
      throws SQLException, ExecutionException {
    addNewColumnToTable_ForX_ShouldWorkProperly(
        RdbEngine.MYSQL,
        "SELECT `column_name`,`data_type`,`key_type`,`clustering_order`,`indexed` FROM `"
            + tableMetadataSchemaName
            + "`.`metadata` WHERE `full_table_name`=? ORDER BY `ordinal_position` ASC",
        "ALTER TABLE `ns`.`table` ADD `c2` INT",
        "DELETE FROM `"
            + tableMetadataSchemaName
            + "`.`metadata` WHERE `full_table_name` = 'ns.table'",
        "INSERT INTO `"
            + tableMetadataSchemaName
            + "`.`metadata` VALUES ('ns.table','c1','TEXT','PARTITION',NULL,false,1)",
        "INSERT INTO `"
            + tableMetadataSchemaName
            + "`.`metadata` VALUES ('ns.table','c2','INT',NULL,NULL,false,2)");
  }

  @Test
  public void addNewColumnToTable_ForOracle_ShouldWorkProperly()
      throws SQLException, ExecutionException {
    addNewColumnToTable_ForX_ShouldWorkProperly(
        RdbEngine.ORACLE,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + tableMetadataSchemaName
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "ALTER TABLE \"ns\".\"table\" ADD \"c2\" INT",
        "DELETE FROM \""
            + tableMetadataSchemaName
            + "\".\"metadata\" WHERE \"full_table_name\" = 'ns.table'",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "\".\"metadata\" VALUES ('ns.table','c1','TEXT','PARTITION',NULL,0,1)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "\".\"metadata\" VALUES ('ns.table','c2','INT',NULL,NULL,0,2)");
  }

  @Test
  public void addNewColumnToTable_ForPostgrsql_ShouldWorkProperly()
      throws SQLException, ExecutionException {
    addNewColumnToTable_ForX_ShouldWorkProperly(
        RdbEngine.POSTGRESQL,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + tableMetadataSchemaName
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "ALTER TABLE \"ns\".\"table\" ADD \"c2\" INT",
        "DELETE FROM \""
            + tableMetadataSchemaName
            + "\".\"metadata\" WHERE \"full_table_name\" = 'ns.table'",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "\".\"metadata\" VALUES ('ns.table','c1','TEXT','PARTITION',NULL,false,1)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "\".\"metadata\" VALUES ('ns.table','c2','INT',NULL,NULL,false,2)");
  }

  @Test
  public void addNewColumnToTable_ForSqlServer_ShouldWorkProperly()
      throws SQLException, ExecutionException {
    addNewColumnToTable_ForX_ShouldWorkProperly(
        RdbEngine.SQL_SERVER,
        "SELECT [column_name],[data_type],[key_type],[clustering_order],[indexed] FROM ["
            + tableMetadataSchemaName
            + "].[metadata] WHERE [full_table_name]=? ORDER BY [ordinal_position] ASC",
        "ALTER TABLE [ns].[table] ADD [c2] INT",
        "DELETE FROM ["
            + tableMetadataSchemaName
            + "].[metadata] WHERE [full_table_name] = 'ns.table'",
        "INSERT INTO ["
            + tableMetadataSchemaName
            + "].[metadata] VALUES ('ns.table','c1','TEXT','PARTITION',NULL,0,1)",
        "INSERT INTO ["
            + tableMetadataSchemaName
            + "].[metadata] VALUES ('ns.table','c2','INT',NULL,NULL,0,2)");
  }

  @Test
  public void addNewColumnToTable_ForSqlite_ShouldWorkProperly()
      throws SQLException, ExecutionException {
    addNewColumnToTable_ForX_ShouldWorkProperly(
        RdbEngine.SQLITE,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + tableMetadataSchemaName
            + "$metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "ALTER TABLE \"ns$table\" ADD \"c2\" INT",
        "DELETE FROM \""
            + tableMetadataSchemaName
            + "$metadata\" WHERE \"full_table_name\" = 'ns.table'",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "$metadata\" VALUES ('ns.table','c1','TEXT','PARTITION',NULL,FALSE,1)",
        "INSERT INTO \""
            + tableMetadataSchemaName
            + "$metadata\" VALUES ('ns.table','c2','INT',NULL,NULL,FALSE,2)");
  }

  private void addNewColumnToTable_ForX_ShouldWorkProperly(
      RdbEngine rdbEngine, String expectedGetMetadataStatement, String... expectedSqlStatements)
      throws SQLException, ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "table";
    String currentColumn = "c1";
    String newColumn = "c2";

    PreparedStatement checkStatement = prepareStatementForNamespaceCheck();
    PreparedStatement selectStatement = mock(PreparedStatement.class);
    ResultSet resultSet =
        mockResultSet(
            Collections.singletonList(
                new Row(currentColumn, DataType.TEXT.toString(), "PARTITION", null, false)));
    when(selectStatement.executeQuery()).thenReturn(resultSet);

    when(connection.prepareStatement(any())).thenReturn(checkStatement).thenReturn(selectStatement);
    List<Statement> expectedStatements = new ArrayList<>();
    for (int i = 0; i < expectedSqlStatements.length; i++) {
      Statement expectedStatement = mock(Statement.class);
      expectedStatements.add(expectedStatement);
    }
    when(connection.createStatement())
        .thenReturn(
            expectedStatements.get(0),
            expectedStatements.subList(1, expectedStatements.size()).toArray(new Statement[0]));

    when(dataSource.getConnection()).thenReturn(connection);
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    // Act
    admin.addNewColumnToTable(namespace, table, newColumn, DataType.INT);

    // Assert
    verify(selectStatement).setString(1, getFullTableName(namespace, table));
    verify(connection).prepareStatement(expectedGetMetadataStatement);
    for (int i = 0; i < expectedSqlStatements.length; i++) {
      verify(expectedStatements.get(i)).execute(expectedSqlStatements[i]);
    }
  }

  @Test
  public void getImportTableMetadata_ForX_ShouldWorkProperly()
      throws SQLException, ExecutionException {
    for (RdbEngine rdbEngine : RDB_ENGINES.keySet()) {
      if (rdbEngine.equals(RdbEngine.SQLITE)) {
        getImportTableMetadata_ForSQLite_ShouldThrowUnsupportedOperationException(rdbEngine);
      } else {
        getImportTableMetadata_ForOtherThanSQLite_ShouldWorkProperly(
            rdbEngine, prepareSqlForTableCheck(rdbEngine, NAMESPACE, TABLE));
      }
    }
  }

  private void getImportTableMetadata_ForOtherThanSQLite_ShouldWorkProperly(
      RdbEngine rdbEngine, String expectedCheckTableExistStatement)
      throws SQLException, ExecutionException {
    // Arrange
    Statement checkTableExistStatement = mock(Statement.class);
    DatabaseMetaData metadata = mock(DatabaseMetaData.class);
    ResultSet primaryKeyResults = mock(ResultSet.class);
    ResultSet columnResults = mock(ResultSet.class);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.createStatement()).thenReturn(checkTableExistStatement);
    when(connection.getMetaData()).thenReturn(metadata);
    when(primaryKeyResults.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    when(primaryKeyResults.getString(JDBC_COL_COLUMN_NAME)).thenReturn("pk1").thenReturn("pk2");
    when(columnResults.next())
        .thenAnswer(
            new Answer<Boolean>() {
              private int i = 0;

              @Override
              public Boolean answer(InvocationOnMock invocation) {
                // two primary key columns + one regular column
                return i++ < 3;
              }
            });
    when(columnResults.getString(JDBC_COL_COLUMN_NAME))
        .thenReturn("pk1")
        .thenReturn("pk2")
        .thenReturn("col");
    when(columnResults.getInt(JDBC_COL_DATA_TYPE))
        .thenReturn(Types.VARCHAR)
        .thenReturn(Types.VARCHAR)
        .thenReturn(Types.REAL);
    when(columnResults.getString(JDBC_COL_TYPE_NAME))
        .thenReturn("VARCHAR")
        .thenReturn("VARCHAR")
        .thenReturn("");
    when(columnResults.getInt(JDBC_COL_COLUMN_SIZE)).thenReturn(0).thenReturn(0).thenReturn(0);
    when(columnResults.getInt(JDBC_COL_DECIMAL_DIGITS)).thenReturn(0).thenReturn(0).thenReturn(0);
    when(metadata.getPrimaryKeys(null, NAMESPACE, TABLE)).thenReturn(primaryKeyResults);
    when(metadata.getColumns(null, NAMESPACE, TABLE, "%")).thenReturn(columnResults);

    Map<String, DataType> expectedColumns = new LinkedHashMap<>();
    expectedColumns.put("pk1", DataType.TEXT);
    expectedColumns.put("pk2", DataType.TEXT);
    expectedColumns.put("col", DataType.FLOAT);

    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);
    String description = "database engine specific test failed: " + rdbEngine;

    // Act
    TableMetadata actual = admin.getImportTableMetadata(NAMESPACE, TABLE);

    // Assert
    verify(checkTableExistStatement, description(description))
        .execute(expectedCheckTableExistStatement);
    assertThat(actual.getPartitionKeyNames()).hasSameElementsAs(ImmutableSet.of("pk1", "pk2"));
    assertThat(actual.getColumnDataTypes()).containsExactlyEntriesOf(expectedColumns);
  }

  private void getImportTableMetadata_ForSQLite_ShouldThrowUnsupportedOperationException(
      RdbEngine rdbEngine) {
    // Arrange
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    // Act Assert
    assertThatThrownBy(() -> admin.getImportTableMetadata(NAMESPACE, TABLE))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void getImportTableMetadata_PrimaryKeyNotExistsForX_ShouldThrowExecutionException()
      throws SQLException {
    for (RdbEngine rdbEngine : RDB_ENGINES.keySet()) {
      if (!rdbEngine.equals(RdbEngine.SQLITE)) {
        getImportTableMetadata_PrimaryKeyNotExistsForX_ShouldThrowIllegalStateException(
            rdbEngine, prepareSqlForTableCheck(rdbEngine, NAMESPACE, TABLE));
      }
    }
  }

  @Test
  public void getImportTableMetadata_WithNonExistingTableForX_ShouldThrowIllegalArgumentException()
      throws SQLException {
    for (RdbEngine rdbEngine : RDB_ENGINES.keySet()) {
      if (!rdbEngine.equals(RdbEngine.SQLITE)) {
        getImportTableMetadata_WithNonExistingTableForX_ShouldThrowIllegalArgumentException(
            rdbEngine, prepareSqlForTableCheck(rdbEngine, NAMESPACE, TABLE));
      }
    }
  }

  private void getImportTableMetadata_WithNonExistingTableForX_ShouldThrowIllegalArgumentException(
      RdbEngine rdbEngine, String expectedCheckTableExistStatement) throws SQLException {
    // Arrange
    Statement checkTableExistStatement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(checkTableExistStatement);
    when(dataSource.getConnection()).thenReturn(connection);

    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);
    SQLException sqlException = mock(SQLException.class);
    mockUndefinedTableError(rdbEngine, sqlException);
    when(checkTableExistStatement.execute(any())).thenThrow(sqlException);

    // Act Assert
    assertThatThrownBy(() -> admin.getImportTableMetadata(NAMESPACE, TABLE))
        .isInstanceOf(IllegalArgumentException.class);
    verify(
            checkTableExistStatement,
            description("database engine specific test failed: " + rdbEngine))
        .execute(expectedCheckTableExistStatement);
  }

  private void getImportTableMetadata_PrimaryKeyNotExistsForX_ShouldThrowIllegalStateException(
      RdbEngine rdbEngine, String expectedCheckTableExistStatement) throws SQLException {
    // Arrange
    Statement checkTableExistStatement = mock(Statement.class);
    DatabaseMetaData metadata = mock(DatabaseMetaData.class);
    ResultSet primaryKeyResults = mock(ResultSet.class);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.createStatement()).thenReturn(checkTableExistStatement);
    when(connection.getMetaData()).thenReturn(metadata);
    when(primaryKeyResults.next()).thenReturn(false);
    when(metadata.getPrimaryKeys(null, NAMESPACE, TABLE)).thenReturn(primaryKeyResults);
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);
    String description = "database engine specific test failed: " + rdbEngine;

    // Act
    Throwable thrown = catchThrowable(() -> admin.getImportTableMetadata(NAMESPACE, TABLE));

    // Assert
    verify(checkTableExistStatement, description(description))
        .execute(expectedCheckTableExistStatement);
    assertThat(thrown).as(description).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void getImportTableMetadata_UnsupportedDataTypeGivenForX_ShouldThrowExecutionException()
      throws SQLException {
    for (RdbEngine rdbEngine : RDB_ENGINES.keySet()) {
      if (!rdbEngine.equals(RdbEngine.SQLITE)) {
        getImportTableMetadata_UnsupportedDataTypeGivenForX_ShouldThrowExecutionException(
            rdbEngine, prepareSqlForTableCheck(rdbEngine, NAMESPACE, TABLE));
      }
    }
  }

  private void getImportTableMetadata_UnsupportedDataTypeGivenForX_ShouldThrowExecutionException(
      RdbEngine rdbEngine, String expectedCheckTableExistStatement) throws SQLException {
    // Arrange
    Statement checkTableExistStatement = mock(Statement.class);
    DatabaseMetaData metadata = mock(DatabaseMetaData.class);
    ResultSet primaryKeyResults = mock(ResultSet.class);
    ResultSet columnResults = mock(ResultSet.class);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.createStatement()).thenReturn(checkTableExistStatement);
    when(connection.getMetaData()).thenReturn(metadata);
    when(primaryKeyResults.next()).thenReturn(true).thenReturn(false);
    when(primaryKeyResults.getString(JDBC_COL_COLUMN_NAME)).thenReturn("pk1");
    when(columnResults.next()).thenReturn(true).thenReturn(false);
    when(columnResults.getString(JDBC_COL_COLUMN_NAME)).thenReturn("pk1");
    when(columnResults.getInt(JDBC_COL_DATA_TYPE)).thenReturn(Types.TIMESTAMP);
    when(columnResults.getString(JDBC_COL_TYPE_NAME)).thenReturn("timestamp");
    when(columnResults.getInt(JDBC_COL_COLUMN_SIZE)).thenReturn(0);
    when(columnResults.getInt(JDBC_COL_DECIMAL_DIGITS)).thenReturn(0);
    when(metadata.getPrimaryKeys(null, NAMESPACE, TABLE)).thenReturn(primaryKeyResults);
    when(metadata.getColumns(null, NAMESPACE, TABLE, "%")).thenReturn(columnResults);
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);
    String description = "database engine specific test failed: " + rdbEngine;

    // Act
    Throwable thrown = catchThrowable(() -> admin.getImportTableMetadata(NAMESPACE, TABLE));

    // Assert
    verify(checkTableExistStatement, description(description))
        .execute(expectedCheckTableExistStatement);
    assertThat(thrown).as(description).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void addRawColumnToTable_ForX_ShouldWorkProperly()
      throws SQLException, ExecutionException {
    for (RdbEngine rdbEngine : RDB_ENGINES.keySet()) {
      addRawColumnToTable_ForX_ShouldWorkProperly(
          rdbEngine,
          prepareSqlForTableCheck(rdbEngine, NAMESPACE, TABLE),
          prepareSqlForAlterTableAddColumn(rdbEngine, COLUMN_1));
    }
  }

  private void addRawColumnToTable_ForX_ShouldWorkProperly(
      RdbEngine rdbEngine, String... expectedSqlStatements)
      throws SQLException, ExecutionException {
    // Arrange
    List<Statement> expectedStatements = new ArrayList<>();
    for (int i = 0; i < expectedSqlStatements.length; i++) {
      Statement expectedStatement = mock(Statement.class);
      expectedStatements.add(expectedStatement);
    }
    when(connection.createStatement())
        .thenReturn(
            expectedStatements.get(0),
            expectedStatements.subList(1, expectedStatements.size()).toArray(new Statement[0]));

    when(dataSource.getConnection()).thenReturn(connection);
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    // Act
    admin.addRawColumnToTable(NAMESPACE, TABLE, COLUMN_1, DataType.INT);

    // Assert
    for (int i = 0; i < expectedSqlStatements.length; i++) {
      verify(
              expectedStatements.get(i),
              description("database engine specific test failed: " + rdbEngine))
          .execute(expectedSqlStatements[i]);
    }
  }

  @Test
  public void addRawColumnToTable_WithNonExistingTableForX_ShouldThrowIllegalArgumentException()
      throws SQLException {
    for (RdbEngine rdbEngine : RDB_ENGINES.keySet()) {
      addRawColumnToTable_WithNonExistingTableForX_ShouldThrowIllegalArgumentException(
          rdbEngine, prepareSqlForTableCheck(rdbEngine, NAMESPACE, TABLE));
    }
  }

  private void addRawColumnToTable_WithNonExistingTableForX_ShouldThrowIllegalArgumentException(
      RdbEngine rdbEngine, String expectedCheckTableExistStatement) throws SQLException {
    // Arrange
    Statement checkTableExistStatement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(checkTableExistStatement);
    when(dataSource.getConnection()).thenReturn(connection);

    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);
    SQLException sqlException = mock(SQLException.class);
    mockUndefinedTableError(rdbEngine, sqlException);
    when(checkTableExistStatement.execute(any())).thenThrow(sqlException);

    // Act Assert
    assertThatThrownBy(() -> admin.addRawColumnToTable(NAMESPACE, TABLE, COLUMN_1, DataType.INT))
        .isInstanceOf(IllegalArgumentException.class);
    verify(
            checkTableExistStatement,
            description("database engine specific test failed: " + rdbEngine))
        .execute(expectedCheckTableExistStatement);
  }

  @Test
  public void importTable_ForX_ShouldWorkProperly() throws SQLException, ExecutionException {
    for (RdbEngine rdbEngine : RDB_ENGINES.keySet()) {
      if (!rdbEngine.equals(RdbEngine.SQLITE)) {
        List<String> statements = new ArrayList<>();
        statements.add(prepareSqlForTableCheck(rdbEngine, NAMESPACE, TABLE));
        statements.add(prepareSqlForTableCheck(rdbEngine, NAMESPACE, TABLE));
        statements.add(prepareSqlForMetadataTableCheck(rdbEngine));
        statements.addAll(prepareSqlForCreateSchemaStatements(rdbEngine));
        statements.add(prepareSqlForCreateMetadataTable(rdbEngine));
        statements.add(
            prepareSqlForInsertMetadata(
                rdbEngine, COLUMN_1, "TEXT", "PARTITION", "NULL", false, 1));
        importTable_ForX_ShouldWorkProperly(rdbEngine, statements);
      }
    }
  }

  private void importTable_ForX_ShouldWorkProperly(
      RdbEngine rdbEngine, List<String> expectedSqlStatements)
      throws SQLException, ExecutionException {
    // Arrange
    DatabaseMetaData metadata = mock(DatabaseMetaData.class);
    ResultSet primaryKeyResults = mock(ResultSet.class);
    ResultSet columnResults = mock(ResultSet.class);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.getMetaData()).thenReturn(metadata);
    when(primaryKeyResults.next()).thenReturn(true).thenReturn(false);
    when(primaryKeyResults.getString(JDBC_COL_COLUMN_NAME)).thenReturn(COLUMN_1);
    when(columnResults.next()).thenReturn(true).thenReturn(false);
    when(columnResults.getString(JDBC_COL_COLUMN_NAME)).thenReturn(COLUMN_1);
    when(columnResults.getInt(JDBC_COL_DATA_TYPE)).thenReturn(Types.VARCHAR);
    when(columnResults.getString(JDBC_COL_TYPE_NAME)).thenReturn("VARCHAR");
    when(columnResults.getInt(JDBC_COL_COLUMN_SIZE)).thenReturn(0);
    when(columnResults.getInt(JDBC_COL_DECIMAL_DIGITS)).thenReturn(0);
    when(metadata.getPrimaryKeys(null, NAMESPACE, TABLE)).thenReturn(primaryKeyResults);
    when(metadata.getColumns(null, NAMESPACE, TABLE, "%")).thenReturn(columnResults);
    List<Statement> expectedStatements = new ArrayList<>();
    for (int i = 0; i < expectedSqlStatements.size(); i++) {
      Statement expectedStatement = mock(Statement.class);
      expectedStatements.add(expectedStatement);
    }
    when(connection.createStatement())
        .thenReturn(
            expectedStatements.get(0),
            expectedStatements.subList(1, expectedStatements.size()).toArray(new Statement[0]));

    // prepare the situation where metadata table does not exist
    SQLException sqlException = mock(SQLException.class);
    mockUndefinedTableError(rdbEngine, sqlException);
    when(expectedStatements.get(2).execute(any())).thenThrow(sqlException);

    when(dataSource.getConnection()).thenReturn(connection);
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    // Act
    admin.importTable(NAMESPACE, TABLE, Collections.emptyMap());

    // Assert
    for (int i = 0; i < expectedSqlStatements.size(); i++) {
      verify(
              expectedStatements.get(i),
              description("database engine specific test failed: " + rdbEngine))
          .execute(expectedSqlStatements.get(i));
    }
  }

  @Test
  public void importTable_ForSQLite_ShouldThrowUnsupportedOperationException() {
    // Arrange
    JdbcAdmin admin = createJdbcAdminFor(RdbEngine.SQLITE);

    // Act
    Throwable thrown =
        catchThrowable(() -> admin.importTable(NAMESPACE, TABLE, Collections.emptyMap()));

    // Assert
    assertThat(thrown).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void hasDifferentClusteringOrders_GivenOnlyAscOrders_ShouldReturnFalse() {
    // Arrange
    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk")
            .addClusteringKey("ck1", Order.ASC)
            .addClusteringKey("ck2", Order.ASC)
            .addColumn("pk", DataType.INT)
            .addColumn("ck1", DataType.INT)
            .addColumn("ck2", DataType.INT)
            .addColumn("value", DataType.TEXT)
            .build();

    // Act
    // Assert
    assertThat(hasDifferentClusteringOrders(metadata)).isFalse();
  }

  @Test
  void hasDifferentClusteringOrders_GivenOnlyDescOrders_ShouldReturnFalse() {
    // Arrange
    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk")
            .addClusteringKey("ck1", Order.DESC)
            .addClusteringKey("ck2", Order.DESC)
            .addColumn("pk", DataType.INT)
            .addColumn("ck1", DataType.INT)
            .addColumn("ck2", DataType.INT)
            .addColumn("value", DataType.TEXT)
            .build();

    // Act
    // Assert
    assertThat(hasDifferentClusteringOrders(metadata)).isFalse();
  }

  @Test
  void hasDifferentClusteringOrders_GivenBothAscAndDescOrders_ShouldReturnTrue() {
    // Arrange
    TableMetadata metadata1 =
        TableMetadata.newBuilder()
            .addPartitionKey("pk")
            .addClusteringKey("ck1", Order.ASC)
            .addClusteringKey("ck2", Order.DESC)
            .addColumn("pk", DataType.INT)
            .addColumn("ck1", DataType.INT)
            .addColumn("ck2", DataType.INT)
            .addColumn("value", DataType.TEXT)
            .build();
    TableMetadata metadata2 =
        TableMetadata.newBuilder()
            .addPartitionKey("pk")
            .addClusteringKey("ck1", Order.DESC)
            .addClusteringKey("ck2", Order.ASC)
            .addColumn("pk", DataType.INT)
            .addColumn("ck1", DataType.INT)
            .addColumn("ck2", DataType.INT)
            .addColumn("value", DataType.TEXT)
            .build();

    // Act
    // Assert
    assertThat(hasDifferentClusteringOrders(metadata1)).isTrue();
    assertThat(hasDifferentClusteringOrders(metadata2)).isTrue();
  }

  private PreparedStatement prepareStatementForNamespaceCheck() throws SQLException {
    return prepareStatementForNamespaceCheck(true);
  }

  private PreparedStatement prepareStatementForNamespaceCheck(boolean exists) throws SQLException {
    PreparedStatement statement = mock(PreparedStatement.class);
    ResultSet results = mock(ResultSet.class);
    doNothing().when(statement).setString(anyInt(), anyString());
    when(statement.executeQuery()).thenReturn(results);
    when(results.next()).thenReturn(exists);
    return statement;
  }

  private String prepareSqlForMetadataTableCheck(RdbEngine rdbEngine) {
    return prepareSqlForTableCheck(rdbEngine, tableMetadataSchemaName, "metadata");
  }

  private String prepareSqlForTableCheck(RdbEngine rdbEngine, String namespace, String table) {
    RdbEngineStrategy rdbEngineStrategy = getRdbEngineStrategy(rdbEngine);
    StringBuilder sql =
        new StringBuilder("SELECT ")
            .append(rdbEngine.equals(RdbEngine.SQL_SERVER) ? "TOP 1 1" : "1")
            .append(" FROM ")
            .append(rdbEngineStrategy.encloseFullTableName(namespace, table));

    switch (rdbEngine) {
      case ORACLE:
        sql.append(" FETCH FIRST 1 ROWS ONLY");
        break;
      case SQL_SERVER:
        break;
      default:
        sql.append(" LIMIT 1");
    }

    return sql.toString();
  }

  private String prepareSqlForAlterTableAddColumn(RdbEngine rdbEngine, String column) {
    RdbEngineStrategy rdbEngineStrategy = getRdbEngineStrategy(rdbEngine);
    return "ALTER TABLE "
        + rdbEngineStrategy.encloseFullTableName(NAMESPACE, TABLE)
        + " ADD "
        + rdbEngineStrategy.enclose(column)
        + " INT";
  }

  private List<String> prepareSqlForCreateSchemaStatements(RdbEngine rdbEngine) {
    RdbEngineStrategy rdbEngineStrategy = getRdbEngineStrategy(rdbEngine);
    List<String> statements = new ArrayList<>();

    switch (rdbEngine) {
      case MYSQL:
      case POSTGRESQL:
      case YUGABYTE:
      case SQL_SERVER:
        statements.add(
            "CREATE SCHEMA "
                + (rdbEngine.equals(RdbEngine.SQL_SERVER) ? "" : "IF NOT EXISTS ")
                + rdbEngineStrategy.enclose(tableMetadataSchemaName));
        break;
      case ORACLE:
        statements.add(
            "CREATE USER "
                + rdbEngineStrategy.enclose(tableMetadataSchemaName)
                + " IDENTIFIED BY "
                + rdbEngineStrategy.enclose("oracle"));
        statements.add(
            "ALTER USER "
                + rdbEngineStrategy.enclose(tableMetadataSchemaName)
                + " quota unlimited on USERS");
        break;
      default:
        break;
    }

    return statements;
  }

  private String prepareSqlForCreateMetadataTable(RdbEngine rdbEngine) {
    RdbEngineStrategy rdbEngineStrategy = getRdbEngineStrategy(rdbEngine);
    StringBuilder sql = new StringBuilder("CREATE TABLE ");
    if (!rdbEngine.equals(RdbEngine.ORACLE) && !rdbEngine.equals(RdbEngine.SQL_SERVER)) {
      sql.append("IF NOT EXISTS ");
    }

    sql.append(rdbEngineStrategy.encloseFullTableName(tableMetadataSchemaName, "metadata"))
        .append("(")
        .append(rdbEngineStrategy.enclose("full_table_name"))
        .append(" ")
        .append(getVarcharString(rdbEngine, 128))
        .append(",")
        .append(rdbEngineStrategy.enclose("column_name"))
        .append(" ")
        .append(getVarcharString(rdbEngine, 128))
        .append(",")
        .append(rdbEngineStrategy.enclose("data_type"))
        .append(" ")
        .append(getVarcharString(rdbEngine, 20))
        .append(" NOT NULL,")
        .append(rdbEngineStrategy.enclose("key_type"))
        .append(" ")
        .append(getVarcharString(rdbEngine, 20))
        .append(",")
        .append(rdbEngineStrategy.enclose("clustering_order"))
        .append(" ")
        .append(getVarcharString(rdbEngine, 10))
        .append(",")
        .append(rdbEngineStrategy.enclose("indexed"));

    switch (rdbEngine) {
      case ORACLE:
        sql.append(" NUMBER(1) NOT NULL,");
        break;
      case SQL_SERVER:
        sql.append(" BIT NOT NULL,");
        break;
      default:
        sql.append(" BOOLEAN NOT NULL,");
        break;
    }

    sql.append(rdbEngineStrategy.enclose("ordinal_position"))
        .append(" INTEGER NOT NULL,PRIMARY KEY (")
        .append(rdbEngineStrategy.enclose("full_table_name"))
        .append(", ")
        .append(rdbEngineStrategy.enclose("column_name"))
        .append("))");

    return sql.toString();
  }

  private String getVarcharString(RdbEngine rdbEngine, int size) {
    switch (rdbEngine) {
      case ORACLE:
        return "VARCHAR2(" + size + ")";
      case SQLITE:
        return "TEXT";
      default:
        return "VARCHAR(" + size + ")";
    }
  }

  private String prepareSqlForInsertMetadata(
      RdbEngine rdbEngine,
      String column,
      String dataType,
      String keyType,
      String clusteringOrder,
      boolean indexed,
      int ordinal) {
    RdbEngineStrategy rdbEngineStrategy = getRdbEngineStrategy(rdbEngine);
    List<String> values =
        ImmutableList.of(
            "'" + NAMESPACE + "." + TABLE + "'",
            "'" + column + "'",
            "'" + dataType + "'",
            "'" + keyType + "'",
            clusteringOrder,
            getBooleanString(rdbEngine, indexed),
            Integer.toString(ordinal));

    return "INSERT INTO "
        + rdbEngineStrategy.encloseFullTableName(tableMetadataSchemaName, "metadata")
        + " VALUES ("
        + String.join(",", values)
        + ")";
  }

  private String getBooleanString(RdbEngine rdbEngine, boolean value) {
    switch (rdbEngine) {
      case ORACLE:
      case SQL_SERVER:
        return value ? "1" : "0";
      case SQLITE:
        return value ? "TRUE" : "FALSE";
      default:
        return value ? "true" : "false";
    }
  }

  private RdbEngineStrategy getRdbEngineStrategy(RdbEngine rdbEngine) {
    return RDB_ENGINES.getOrDefault(rdbEngine, RdbEngineFactory.create("jdbc:mysql:"));
  }

  // Utility class used to mock ResultSet for getTableMetadata test
  static class GetColumnsResultSetMocker implements org.mockito.stubbing.Answer<Object> {

    final List<GetColumnsResultSetMocker.Row> rows;
    int row = -1;

    public GetColumnsResultSetMocker(List<GetColumnsResultSetMocker.Row> rows) {
      this.rows = rows;
    }

    @Override
    public Object answer(InvocationOnMock invocation) throws Throwable {
      row++;
      if (row >= rows.size()) {
        return false;
      }
      GetColumnsResultSetMocker.Row currentRow = rows.get(row);
      ResultSet mock = (ResultSet) invocation.getMock();
      when(mock.getString(JdbcAdmin.METADATA_COL_COLUMN_NAME)).thenReturn(currentRow.columnName);
      when(mock.getString(JdbcAdmin.METADATA_COL_DATA_TYPE)).thenReturn(currentRow.dataType);
      when(mock.getString(JdbcAdmin.METADATA_COL_KEY_TYPE)).thenReturn(currentRow.keyType);
      when(mock.getString(JdbcAdmin.METADATA_COL_CLUSTERING_ORDER))
          .thenReturn(currentRow.clusteringOrder);
      when(mock.getBoolean(JdbcAdmin.METADATA_COL_INDEXED)).thenReturn(currentRow.indexed);
      return true;
    }

    static class Row {

      final String columnName;
      final String dataType;
      final String keyType;
      final String clusteringOrder;
      final boolean indexed;

      public Row(
          String columnName,
          String dataType,
          String keyType,
          String clusteringOrder,
          boolean indexed) {
        this.columnName = columnName;
        this.dataType = dataType;
        this.keyType = keyType;
        this.clusteringOrder = clusteringOrder;
        this.indexed = indexed;
      }
    }
  }

  // Utility class used to mock ResultSet for getTablesNames test
  static class GetTablesNamesResultSetMocker implements org.mockito.stubbing.Answer<Object> {

    final List<GetTablesNamesResultSetMocker.Row> rows;
    int row = -1;

    public GetTablesNamesResultSetMocker(List<GetTablesNamesResultSetMocker.Row> rows) {
      this.rows = rows;
    }

    @Override
    public Object answer(InvocationOnMock invocation) throws Throwable {
      row++;
      if (row >= rows.size()) {
        return false;
      }
      GetTablesNamesResultSetMocker.Row currentRow = rows.get(row);
      ResultSet mock = (ResultSet) invocation.getMock();
      when(mock.getString(JdbcAdmin.METADATA_COL_FULL_TABLE_NAME))
          .thenReturn(currentRow.fullTableName);
      return true;
    }

    static class Row {

      final String fullTableName;

      public Row(String fullTableName) {
        this.fullTableName = fullTableName;
      }
    }
  }
}
