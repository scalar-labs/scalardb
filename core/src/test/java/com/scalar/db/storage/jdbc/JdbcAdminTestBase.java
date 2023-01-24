package com.scalar.db.storage.jdbc;

import static com.scalar.db.util.ScalarDbUtils.getFullTableName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.JdbcAdminTestBase.GetColumnsResultSetMocker.Row;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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

/**
 * Abstraction that defines unit tests for the {@link JdbcAdmin}. The class purpose is to be able to
 * run the {@link JdbcAdmin} unit tests with different values for the {@link JdbcConfig}, notably
 * {@link JdbcConfig#TABLE_METADATA_SCHEMA}.
 */
public abstract class JdbcAdminTestBase {

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
    RdbEngineStrategy st = RdbEngineFactory.create(rdbEngine);
    try (MockedStatic<RdbEngineFactory> mocked = mockStatic(RdbEngineFactory.class)) {
      mocked.when(() -> RdbEngineFactory.create(any(JdbcConfig.class))).thenReturn(st);
      return new JdbcAdmin(dataSource, config);
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

  private void getTableMetadata_forX_ShouldReturnTableMetadata(
      RdbEngine rdbEngine, String expectedSelectStatements)
      throws ExecutionException, SQLException {
    // Arrange
    String namespace = "ns";
    String table = "table";

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
    when(connection.prepareStatement(any())).thenReturn(selectStatement);
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
  public void createNamespace_forMysql_shouldExecuteCreateNamespaceStatement()
      throws ExecutionException, SQLException {
    createNamespace_forX_shouldExecuteCreateNamespaceStatement(
        RdbEngine.MYSQL, "CREATE SCHEMA `my_ns` character set utf8 COLLATE utf8_bin");
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
        "CREATE TABLE [my_ns].[foo_table]([c3] BIT,[c1] VARCHAR(8000) COLLATE Latin1_General_BIN,"
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
        "CREATE TABLE [my_ns].[foo_table]([c3] BIT,[c1] VARCHAR(8000) COLLATE Latin1_General_BIN,"
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
        RdbEngine.MYSQL, "SELECT 1 FROM `information_schema`.`schemata` WHERE `schema_name` = ?");
  }

  @Test
  public void namespaceExists_forPostgresqlWithExistingNamespace_shouldReturnTrue()
      throws Exception {
    namespaceExists_forXWithExistingNamespace_ShouldReturnTrue(
        RdbEngine.POSTGRESQL,
        "SELECT 1 FROM \"information_schema\".\"schemata\" WHERE \"schema_name\" = ?");
  }

  @Test
  public void namespaceExists_forSqlServerWithExistingNamespace_shouldReturnTrue()
      throws Exception {
    namespaceExists_forXWithExistingNamespace_ShouldReturnTrue(
        RdbEngine.SQL_SERVER, "SELECT 1 FROM [sys].[schemas] WHERE [name] = ?");
  }

  @Test
  public void namespaceExists_forOracleWithExistingNamespace_shouldReturnTrue() throws Exception {
    namespaceExists_forXWithExistingNamespace_ShouldReturnTrue(
        RdbEngine.ORACLE, "SELECT 1 FROM \"ALL_USERS\" WHERE \"USERNAME\" = ?");
  }

  private void namespaceExists_forXWithExistingNamespace_ShouldReturnTrue(
      RdbEngine rdbEngine, String expectedSelectStatement) throws SQLException, ExecutionException {
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
    verify(selectStatement).setString(1, namespace);
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

    PreparedStatement selectStatement = mock(PreparedStatement.class);
    ResultSet resultSet =
        mockResultSet(
            Arrays.asList(
                new GetColumnsResultSetMocker.Row(
                    "c1", DataType.BOOLEAN.toString(), "PARTITION", null, false),
                new GetColumnsResultSetMocker.Row(
                    indexColumn, DataType.BOOLEAN.toString(), null, null, false)));
    when(selectStatement.executeQuery()).thenReturn(resultSet);
    when(connection.prepareStatement(any())).thenReturn(selectStatement);
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

    PreparedStatement selectStatement = mock(PreparedStatement.class);
    ResultSet resultSet =
        mockResultSet(
            Arrays.asList(
                new GetColumnsResultSetMocker.Row(
                    "c1", DataType.BOOLEAN.toString(), "PARTITION", null, false),
                new GetColumnsResultSetMocker.Row(
                    indexColumn, DataType.TEXT.toString(), null, null, false)));
    when(selectStatement.executeQuery()).thenReturn(resultSet);
    when(connection.prepareStatement(any())).thenReturn(selectStatement);

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
    PreparedStatement selectStatement = mock(PreparedStatement.class);
    ResultSet resultSet =
        mockResultSet(
            Arrays.asList(
                new GetColumnsResultSetMocker.Row(
                    "c1", DataType.BOOLEAN.toString(), "PARTITION", null, false),
                new GetColumnsResultSetMocker.Row(
                    indexColumn, DataType.BOOLEAN.toString(), null, null, false)));
    when(selectStatement.executeQuery()).thenReturn(resultSet);
    when(connection.prepareStatement(any())).thenReturn(selectStatement);

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
    PreparedStatement selectStatement = mock(PreparedStatement.class);
    ResultSet resultSet =
        mockResultSet(
            Arrays.asList(
                new GetColumnsResultSetMocker.Row(
                    "c1", DataType.BOOLEAN.toString(), "PARTITION", null, false),
                new GetColumnsResultSetMocker.Row(
                    indexColumn, DataType.TEXT.toString(), null, null, false)));
    when(selectStatement.executeQuery()).thenReturn(resultSet);
    when(connection.prepareStatement(any())).thenReturn(selectStatement);

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
    if (rdbEngine == RdbEngine.MYSQL) {
      when(sqlException.getErrorCode()).thenReturn(1049);
    } else if (rdbEngine == RdbEngine.POSTGRESQL) {
      when(sqlException.getSQLState()).thenReturn("42P01");
    } else if (rdbEngine == RdbEngine.ORACLE) {
      when(sqlException.getErrorCode()).thenReturn(942);
    } else {
      when(sqlException.getErrorCode()).thenReturn(208);
    }
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
    if (rdbEngine == RdbEngine.MYSQL) {
      when(sqlException.getErrorCode()).thenReturn(1049);
    } else if (rdbEngine == RdbEngine.POSTGRESQL) {
      when(sqlException.getSQLState()).thenReturn("42P01");
    } else if (rdbEngine == RdbEngine.ORACLE) {
      when(sqlException.getErrorCode()).thenReturn(942);
    } else {
      when(sqlException.getErrorCode()).thenReturn(208);
    }
    when(checkTableExistStatement.execute(any())).thenThrow(sqlException);

    // Act
    assertThatThrownBy(() -> admin.repairTable(namespace, table, metadata, new HashMap<>()))
        .isInstanceOf(IllegalArgumentException.class);

    // Assert
    verify(checkTableExistStatement).execute(expectedCheckTableExistStatement);
  }

  @Test
  public void
      addNewColumnToTable_WithAlreadyExistingColumnForMysql_ShouldThrowIllegalArgumentException()
          throws SQLException {
    addNewColumnToTable_WithAlreadyExistingColumnForX_ShouldThrowIllegalArgumentException(
        RdbEngine.MYSQL,
        "SELECT `column_name`,`data_type`,`key_type`,`clustering_order`,`indexed` FROM `"
            + tableMetadataSchemaName
            + "`.`metadata` WHERE `full_table_name`=? ORDER BY `ordinal_position` ASC");
  }

  @Test
  public void
      addNewColumnToTable_WithAlreadyExistingColumnForOracle_ShouldThrowIllegalArgumentException()
          throws SQLException {
    addNewColumnToTable_WithAlreadyExistingColumnForX_ShouldThrowIllegalArgumentException(
        RdbEngine.ORACLE,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + tableMetadataSchemaName
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC");
  }

  @Test
  public void
      addNewColumnToTable_WithAlreadyExistingColumnForPostgresql_ShouldThrowIllegalArgumentException()
          throws SQLException {
    addNewColumnToTable_WithAlreadyExistingColumnForX_ShouldThrowIllegalArgumentException(
        RdbEngine.POSTGRESQL,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + tableMetadataSchemaName
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC");
  }

  @Test
  public void
      addNewColumnToTable_WithAlreadyExistingColumnForSqlServer_ShouldThrowIllegalArgumentException()
          throws SQLException {
    addNewColumnToTable_WithAlreadyExistingColumnForX_ShouldThrowIllegalArgumentException(
        RdbEngine.SQL_SERVER,
        "SELECT [column_name],[data_type],[key_type],[clustering_order],[indexed] FROM ["
            + tableMetadataSchemaName
            + "].[metadata] WHERE [full_table_name]=? ORDER BY [ordinal_position] ASC");
  }

  private void
      addNewColumnToTable_WithAlreadyExistingColumnForX_ShouldThrowIllegalArgumentException(
          RdbEngine rdbEngine, String expectedGetMetadataStatement) throws SQLException {
    // Arrange
    String namespace = "ns";
    String table = "table";
    String currentColumn = "c1";

    PreparedStatement selectStatement = mock(PreparedStatement.class);
    ResultSet resultSet =
        mockResultSet(
            Collections.singletonList(
                new Row(currentColumn, DataType.TEXT.toString(), "PARTITION", null, false)));
    when(selectStatement.executeQuery()).thenReturn(resultSet);
    when(connection.prepareStatement(any())).thenReturn(selectStatement);
    when(dataSource.getConnection()).thenReturn(connection);
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    // Act
    // Act Assert
    assertThatThrownBy(
            () -> admin.addNewColumnToTable(namespace, table, currentColumn, DataType.INT))
        .isInstanceOf(IllegalArgumentException.class);

    // Assert
    verify(connection).prepareStatement(expectedGetMetadataStatement);
    verify(selectStatement).setString(1, getFullTableName(namespace, table));
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

  private void addNewColumnToTable_ForX_ShouldWorkProperly(
      RdbEngine rdbEngine, String expectedGetMetadataStatement, String... expectedSqlStatements)
      throws SQLException, ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "table";
    String currentColumn = "c1";
    String newColumn = "c2";

    PreparedStatement selectStatement = mock(PreparedStatement.class);
    ResultSet resultSet =
        mockResultSet(
            Collections.singletonList(
                new Row(currentColumn, DataType.TEXT.toString(), "PARTITION", null, false)));
    when(selectStatement.executeQuery()).thenReturn(resultSet);

    when(connection.prepareStatement(any())).thenReturn(selectStatement);
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
