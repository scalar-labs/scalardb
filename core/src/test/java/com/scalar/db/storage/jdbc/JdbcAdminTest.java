package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;

@SuppressFBWarnings({"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"})
public class JdbcAdminTest {

  @Mock private BasicDataSource dataSource;
  @Mock private Connection connection;
  @Mock private JdbcConfig config;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  @Test
  public void getTableMetadata_forMysql_ShouldReturnTableMetadata()
      throws SQLException, ExecutionException {
    getTableMetadata_forX_ShouldReturnTableMetadata(
        RdbEngine.MYSQL,
        Optional.empty(),
        "SELECT `column_name`,`data_type`,`key_type`,`clustering_order`,`indexed` FROM `scalardb`.`metadata` WHERE `full_table_name`=? ORDER BY `ordinal_position` ASC");
  }

  @Test
  public void getTableMetadata_forPostgresql_ShouldReturnTableMetadata()
      throws SQLException, ExecutionException {
    getTableMetadata_forX_ShouldReturnTableMetadata(
        RdbEngine.POSTGRESQL,
        Optional.empty(),
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \"scalardb\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC");
  }

  @Test
  public void getTableMetadata_forSqlServer_ShouldReturnTableMetadata()
      throws SQLException, ExecutionException {
    getTableMetadata_forX_ShouldReturnTableMetadata(
        RdbEngine.SQL_SERVER,
        Optional.empty(),
        "SELECT [column_name],[data_type],[key_type],[clustering_order],[indexed] FROM [scalardb].[metadata] WHERE [full_table_name]=? ORDER BY [ordinal_position] ASC");
  }

  @Test
  public void getTableMetadata_forOracle_ShouldReturnTableMetadata()
      throws SQLException, ExecutionException {
    getTableMetadata_forX_ShouldReturnTableMetadata(
        RdbEngine.ORACLE,
        Optional.empty(),
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \"scalardb\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC");
  }

  @Test
  public void getTableMetadata_forMysqlWithTableMetadataSchemaChanged_ShouldReturnTableMetadata()
      throws SQLException, ExecutionException {
    getTableMetadata_forX_ShouldReturnTableMetadata(
        RdbEngine.MYSQL,
        Optional.of("changed"),
        "SELECT `column_name`,`data_type`,`key_type`,`clustering_order`,`indexed` FROM `changed`.`metadata` WHERE `full_table_name`=? ORDER BY `ordinal_position` ASC");
  }

  @Test
  public void
      getTableMetadata_forPostgresqlWithTableMetadataSchemaChanged_ShouldReturnTableMetadata()
          throws SQLException, ExecutionException {
    getTableMetadata_forX_ShouldReturnTableMetadata(
        RdbEngine.POSTGRESQL,
        Optional.of("changed"),
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \"changed\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC");
  }

  @Test
  public void
      getTableMetadata_forSqlServerWithTableMetadataSchemaChanged_ShouldReturnTableMetadata()
          throws SQLException, ExecutionException {
    getTableMetadata_forX_ShouldReturnTableMetadata(
        RdbEngine.SQL_SERVER,
        Optional.of("changed"),
        "SELECT [column_name],[data_type],[key_type],[clustering_order],[indexed] FROM [changed].[metadata] WHERE [full_table_name]=? ORDER BY [ordinal_position] ASC");
  }

  @Test
  public void getTableMetadata_forOracleWithTableMetadataSchemaChanged_ShouldReturnTableMetadata()
      throws SQLException, ExecutionException {
    getTableMetadata_forX_ShouldReturnTableMetadata(
        RdbEngine.ORACLE,
        Optional.of("changed"),
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \"changed\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC");
  }

  private void getTableMetadata_forX_ShouldReturnTableMetadata(
      RdbEngine rdbEngine, Optional<String> tableMetadataSchema, String expectedSelectStatements)
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

    if (tableMetadataSchema.isPresent()) {
      when(config.getTableMetadataSchema()).thenReturn(tableMetadataSchema);
    }
    JdbcAdmin admin = new JdbcAdmin(dataSource, rdbEngine, config);

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
    JdbcAdmin admin = new JdbcAdmin(dataSource, rdbEngine, config);
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
        "CREATE INDEX index_my_ns_foo_table_c4 ON `my_ns`.`foo_table` (`c4`)",
        "CREATE INDEX index_my_ns_foo_table_c1 ON `my_ns`.`foo_table` (`c1`)",
        "CREATE SCHEMA IF NOT EXISTS `scalardb`",
        "CREATE TABLE IF NOT EXISTS `scalardb`.`metadata`("
            + "`full_table_name` VARCHAR(128),"
            + "`column_name` VARCHAR(128),"
            + "`data_type` VARCHAR(20) NOT NULL,"
            + "`key_type` VARCHAR(20),"
            + "`clustering_order` VARCHAR(10),"
            + "`indexed` BOOLEAN NOT NULL,"
            + "`ordinal_position` INTEGER NOT NULL,"
            + "PRIMARY KEY (`full_table_name`, `column_name`))",
        "INSERT INTO `scalardb`.`metadata` VALUES ('my_ns.foo_table','c3','BOOLEAN','PARTITION',NULL,false,1)",
        "INSERT INTO `scalardb`.`metadata` VALUES ('my_ns.foo_table','c1','TEXT','CLUSTERING','ASC',true,2)",
        "INSERT INTO `scalardb`.`metadata` VALUES ('my_ns.foo_table','c4','BLOB','CLUSTERING','ASC',true,3)",
        "INSERT INTO `scalardb`.`metadata` VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,false,4)",
        "INSERT INTO `scalardb`.`metadata` VALUES ('my_ns.foo_table','c5','INT',NULL,NULL,false,5)",
        "INSERT INTO `scalardb`.`metadata` VALUES ('my_ns.foo_table','c6','DOUBLE',NULL,NULL,false,6)",
        "INSERT INTO `scalardb`.`metadata` VALUES ('my_ns.foo_table','c7','FLOAT',NULL,NULL,false,7)");
  }

  @Test
  public void createTable_forPostgresql_shouldExecuteCreateTableStatement()
      throws ExecutionException, SQLException {
    createTable_forX_shouldExecuteCreateTableStatement(
        RdbEngine.POSTGRESQL,
        "CREATE TABLE \"my_ns\".\"foo_table\"(\"c3\" BOOLEAN,\"c1\" VARCHAR(10485760),\"c4\" BYTEA,\"c2\" BIGINT,\"c5\" INT,\"c6\" DOUBLE PRECISION,\"c7\" FLOAT, PRIMARY KEY (\"c3\",\"c1\",\"c4\"))",
        "CREATE INDEX index_my_ns_foo_table_c4 ON \"my_ns\".\"foo_table\" (\"c4\")",
        "CREATE INDEX index_my_ns_foo_table_c1 ON \"my_ns\".\"foo_table\" (\"c1\")",
        "CREATE SCHEMA IF NOT EXISTS \"scalardb\"",
        "CREATE TABLE IF NOT EXISTS \"scalardb\".\"metadata\"("
            + "\"full_table_name\" VARCHAR(128),"
            + "\"column_name\" VARCHAR(128),"
            + "\"data_type\" VARCHAR(20) NOT NULL,"
            + "\"key_type\" VARCHAR(20),"
            + "\"clustering_order\" VARCHAR(10),"
            + "\"indexed\" BOOLEAN NOT NULL,"
            + "\"ordinal_position\" INTEGER NOT NULL,"
            + "PRIMARY KEY (\"full_table_name\", \"column_name\"))",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c3','BOOLEAN','PARTITION',NULL,false,1)",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c1','TEXT','CLUSTERING','ASC',true,2)",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c4','BLOB','CLUSTERING','ASC',true,3)",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,false,4)",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c5','INT',NULL,NULL,false,5)",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c6','DOUBLE',NULL,NULL,false,6)",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c7','FLOAT',NULL,NULL,false,7)");
  }

  @Test
  public void createTable_forSqlServer_shouldExecuteCreateTableStatement()
      throws ExecutionException, SQLException {
    createTable_forX_shouldExecuteCreateTableStatement(
        RdbEngine.SQL_SERVER,
        "CREATE TABLE [my_ns].[foo_table]([c3] BIT,[c1] VARCHAR(8000) COLLATE Latin1_General_BIN,"
            + "[c4] VARBINARY(8000),[c2] BIGINT,[c5] INT,[c6] FLOAT,[c7] FLOAT(24), PRIMARY KEY ([c3],[c1],[c4]))",
        "CREATE INDEX index_my_ns_foo_table_c4 ON [my_ns].[foo_table] ([c4])",
        "CREATE INDEX index_my_ns_foo_table_c1 ON [my_ns].[foo_table] ([c1])",
        "CREATE SCHEMA [scalardb]",
        "CREATE TABLE [scalardb].[metadata]("
            + "[full_table_name] VARCHAR(128),"
            + "[column_name] VARCHAR(128),"
            + "[data_type] VARCHAR(20) NOT NULL,"
            + "[key_type] VARCHAR(20),"
            + "[clustering_order] VARCHAR(10),"
            + "[indexed] BIT NOT NULL,"
            + "[ordinal_position] INTEGER NOT NULL,"
            + "PRIMARY KEY ([full_table_name], [column_name]))",
        "INSERT INTO [scalardb].[metadata] VALUES ('my_ns.foo_table','c3','BOOLEAN','PARTITION',NULL,0,1)",
        "INSERT INTO [scalardb].[metadata] VALUES ('my_ns.foo_table','c1','TEXT','CLUSTERING','ASC',1,2)",
        "INSERT INTO [scalardb].[metadata] VALUES ('my_ns.foo_table','c4','BLOB','CLUSTERING','ASC',1,3)",
        "INSERT INTO [scalardb].[metadata] VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,0,4)",
        "INSERT INTO [scalardb].[metadata] VALUES ('my_ns.foo_table','c5','INT',NULL,NULL,0,5)",
        "INSERT INTO [scalardb].[metadata] VALUES ('my_ns.foo_table','c6','DOUBLE',NULL,NULL,0,6)",
        "INSERT INTO [scalardb].[metadata] VALUES ('my_ns.foo_table','c7','FLOAT',NULL,NULL,0,7)");
  }

  @Test
  public void createTable_forOracle_shouldExecuteCreateTableStatement()
      throws ExecutionException, SQLException {
    createTable_forX_shouldExecuteCreateTableStatement(
        RdbEngine.ORACLE,
        "CREATE TABLE \"my_ns\".\"foo_table\"(\"c3\" NUMBER(1),\"c1\" VARCHAR2(64),\"c4\" RAW(64),\"c2\" NUMBER(19),\"c5\" INT,\"c6\" BINARY_DOUBLE,\"c7\" BINARY_FLOAT, PRIMARY KEY (\"c3\",\"c1\",\"c4\")) ROWDEPENDENCIES",
        "ALTER TABLE \"my_ns\".\"foo_table\" INITRANS 3 MAXTRANS 255",
        "CREATE INDEX index_my_ns_foo_table_c4 ON \"my_ns\".\"foo_table\" (\"c4\")",
        "CREATE INDEX index_my_ns_foo_table_c1 ON \"my_ns\".\"foo_table\" (\"c1\")",
        "CREATE USER \"scalardb\" IDENTIFIED BY \"oracle\"",
        "ALTER USER \"scalardb\" quota unlimited on USERS",
        "CREATE TABLE \"scalardb\".\"metadata\"(\"full_table_name\" VARCHAR2(128),\"column_name\" VARCHAR2(128),\"data_type\" VARCHAR2(20) NOT NULL,\"key_type\" VARCHAR2(20),\"clustering_order\" VARCHAR2(10),\"indexed\" NUMBER(1) NOT NULL,\"ordinal_position\" INTEGER NOT NULL,PRIMARY KEY (\"full_table_name\", \"column_name\"))",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c3','BOOLEAN','PARTITION',NULL,0,1)",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c1','TEXT','CLUSTERING','ASC',1,2)",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c4','BLOB','CLUSTERING','ASC',1,3)",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,0,4)",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c5','INT',NULL,NULL,0,5)",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c6','DOUBLE',NULL,NULL,0,6)",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c7','FLOAT',NULL,NULL,0,7)");
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

    JdbcAdmin admin = new JdbcAdmin(dataSource, rdbEngine, config);

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
        Optional.empty(),
        "CREATE TABLE `my_ns`.`foo_table`(`c3` BOOLEAN,`c1` VARCHAR(64),`c4` VARBINARY(64),`c2` BIGINT,`c5` INT,`c6` DOUBLE,`c7` DOUBLE, PRIMARY KEY (`c3` ASC,`c1` DESC,`c4` ASC))",
        "CREATE INDEX index_my_ns_foo_table_c4 ON `my_ns`.`foo_table` (`c4`)",
        "CREATE INDEX index_my_ns_foo_table_c1 ON `my_ns`.`foo_table` (`c1`)",
        "CREATE SCHEMA IF NOT EXISTS `scalardb`",
        "CREATE TABLE IF NOT EXISTS `scalardb`.`metadata`("
            + "`full_table_name` VARCHAR(128),"
            + "`column_name` VARCHAR(128),"
            + "`data_type` VARCHAR(20) NOT NULL,"
            + "`key_type` VARCHAR(20),"
            + "`clustering_order` VARCHAR(10),"
            + "`indexed` BOOLEAN NOT NULL,"
            + "`ordinal_position` INTEGER NOT NULL,"
            + "PRIMARY KEY (`full_table_name`, `column_name`))",
        "INSERT INTO `scalardb`.`metadata` VALUES ('my_ns.foo_table','c3','BOOLEAN','PARTITION',NULL,false,1)",
        "INSERT INTO `scalardb`.`metadata` VALUES ('my_ns.foo_table','c1','TEXT','CLUSTERING','DESC',true,2)",
        "INSERT INTO `scalardb`.`metadata` VALUES ('my_ns.foo_table','c4','BLOB','CLUSTERING','ASC',true,3)",
        "INSERT INTO `scalardb`.`metadata` VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,false,4)",
        "INSERT INTO `scalardb`.`metadata` VALUES ('my_ns.foo_table','c5','INT',NULL,NULL,false,5)",
        "INSERT INTO `scalardb`.`metadata` VALUES ('my_ns.foo_table','c6','DOUBLE',NULL,NULL,false,6)",
        "INSERT INTO `scalardb`.`metadata` VALUES ('my_ns.foo_table','c7','FLOAT',NULL,NULL,false,7)");
  }

  @Test
  public void createTable_WithClusteringOrderForPostgresql_shouldExecuteCreateTableStatement()
      throws ExecutionException, SQLException {
    createTable_WithClusteringOrderForX_shouldExecuteCreateTableStatement(
        RdbEngine.POSTGRESQL,
        Optional.empty(),
        "CREATE TABLE \"my_ns\".\"foo_table\"(\"c3\" BOOLEAN,\"c1\" VARCHAR(10485760),\"c4\" BYTEA,\"c2\" BIGINT,\"c5\" INT,\"c6\" DOUBLE PRECISION,\"c7\" FLOAT, PRIMARY KEY (\"c3\",\"c1\",\"c4\"))",
        "CREATE UNIQUE INDEX \"my_ns.foo_table_clustering_order_idx\" ON \"my_ns\".\"foo_table\" (\"c3\" ASC,\"c1\" DESC,\"c4\" ASC)",
        "CREATE INDEX index_my_ns_foo_table_c4 ON \"my_ns\".\"foo_table\" (\"c4\")",
        "CREATE INDEX index_my_ns_foo_table_c1 ON \"my_ns\".\"foo_table\" (\"c1\")",
        "CREATE SCHEMA IF NOT EXISTS \"scalardb\"",
        "CREATE TABLE IF NOT EXISTS \"scalardb\".\"metadata\"("
            + "\"full_table_name\" VARCHAR(128),"
            + "\"column_name\" VARCHAR(128),"
            + "\"data_type\" VARCHAR(20) NOT NULL,"
            + "\"key_type\" VARCHAR(20),"
            + "\"clustering_order\" VARCHAR(10),"
            + "\"indexed\" BOOLEAN NOT NULL,"
            + "\"ordinal_position\" INTEGER NOT NULL,"
            + "PRIMARY KEY (\"full_table_name\", \"column_name\"))",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c3','BOOLEAN','PARTITION',NULL,false,1)",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c1','TEXT','CLUSTERING','DESC',true,2)",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c4','BLOB','CLUSTERING','ASC',true,3)",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,false,4)",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c5','INT',NULL,NULL,false,5)",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c6','DOUBLE',NULL,NULL,false,6)",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c7','FLOAT',NULL,NULL,false,7)");
  }

  @Test
  public void createTable_WithClusteringOrderForSqlServer_shouldExecuteCreateTableStatement()
      throws ExecutionException, SQLException {
    createTable_WithClusteringOrderForX_shouldExecuteCreateTableStatement(
        RdbEngine.SQL_SERVER,
        Optional.empty(),
        "CREATE TABLE [my_ns].[foo_table]([c3] BIT,[c1] VARCHAR(8000) COLLATE Latin1_General_BIN,"
            + "[c4] VARBINARY(8000),[c2] BIGINT,[c5] INT,[c6] FLOAT,[c7] FLOAT(24), PRIMARY KEY ([c3] ASC,[c1] DESC,[c4] ASC))",
        "CREATE INDEX index_my_ns_foo_table_c4 ON [my_ns].[foo_table] ([c4])",
        "CREATE INDEX index_my_ns_foo_table_c1 ON [my_ns].[foo_table] ([c1])",
        "CREATE SCHEMA [scalardb]",
        "CREATE TABLE [scalardb].[metadata]("
            + "[full_table_name] VARCHAR(128),"
            + "[column_name] VARCHAR(128),"
            + "[data_type] VARCHAR(20) NOT NULL,"
            + "[key_type] VARCHAR(20),"
            + "[clustering_order] VARCHAR(10),"
            + "[indexed] BIT NOT NULL,"
            + "[ordinal_position] INTEGER NOT NULL,"
            + "PRIMARY KEY ([full_table_name], [column_name]))",
        "INSERT INTO [scalardb].[metadata] VALUES ('my_ns.foo_table','c3','BOOLEAN','PARTITION',NULL,0,1)",
        "INSERT INTO [scalardb].[metadata] VALUES ('my_ns.foo_table','c1','TEXT','CLUSTERING','DESC',1,2)",
        "INSERT INTO [scalardb].[metadata] VALUES ('my_ns.foo_table','c4','BLOB','CLUSTERING','ASC',1,3)",
        "INSERT INTO [scalardb].[metadata] VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,0,4)",
        "INSERT INTO [scalardb].[metadata] VALUES ('my_ns.foo_table','c5','INT',NULL,NULL,0,5)",
        "INSERT INTO [scalardb].[metadata] VALUES ('my_ns.foo_table','c6','DOUBLE',NULL,NULL,0,6)",
        "INSERT INTO [scalardb].[metadata] VALUES ('my_ns.foo_table','c7','FLOAT',NULL,NULL,0,7)");
  }

  @Test
  public void createTable_WithClusteringOrderForOracle_shouldExecuteCreateTableStatement()
      throws ExecutionException, SQLException {
    createTable_WithClusteringOrderForX_shouldExecuteCreateTableStatement(
        RdbEngine.ORACLE,
        Optional.empty(),
        "CREATE TABLE \"my_ns\".\"foo_table\"(\"c3\" NUMBER(1),\"c1\" VARCHAR2(64),\"c4\" RAW(64),\"c2\" NUMBER(19),\"c5\" INT,\"c6\" BINARY_DOUBLE,\"c7\" BINARY_FLOAT, PRIMARY KEY (\"c3\",\"c1\",\"c4\")) ROWDEPENDENCIES",
        "ALTER TABLE \"my_ns\".\"foo_table\" INITRANS 3 MAXTRANS 255",
        "CREATE UNIQUE INDEX \"my_ns.foo_table_clustering_order_idx\" ON \"my_ns\".\"foo_table\" (\"c3\" ASC,\"c1\" DESC,\"c4\" ASC)",
        "CREATE INDEX index_my_ns_foo_table_c4 ON \"my_ns\".\"foo_table\" (\"c4\")",
        "CREATE INDEX index_my_ns_foo_table_c1 ON \"my_ns\".\"foo_table\" (\"c1\")",
        "CREATE USER \"scalardb\" IDENTIFIED BY \"oracle\"",
        "ALTER USER \"scalardb\" quota unlimited on USERS",
        "CREATE TABLE \"scalardb\".\"metadata\"(\"full_table_name\" VARCHAR2(128),\"column_name\" VARCHAR2(128),\"data_type\" VARCHAR2(20) NOT NULL,\"key_type\" VARCHAR2(20),\"clustering_order\" VARCHAR2(10),\"indexed\" NUMBER(1) NOT NULL,\"ordinal_position\" INTEGER NOT NULL,PRIMARY KEY (\"full_table_name\", \"column_name\"))",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c3','BOOLEAN','PARTITION',NULL,0,1)",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c1','TEXT','CLUSTERING','DESC',1,2)",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c4','BLOB','CLUSTERING','ASC',1,3)",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,0,4)",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c5','INT',NULL,NULL,0,5)",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c6','DOUBLE',NULL,NULL,0,6)",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c7','FLOAT',NULL,NULL,0,7)");
  }

  @Test
  public void
      createTable_WithClusteringOrderForMysqlWithTableMetadataSchemaChanged_shouldExecuteCreateTableStatement()
          throws ExecutionException, SQLException {
    createTable_WithClusteringOrderForX_shouldExecuteCreateTableStatement(
        RdbEngine.MYSQL,
        Optional.of("changed"),
        "CREATE TABLE `my_ns`.`foo_table`(`c3` BOOLEAN,`c1` VARCHAR(64),`c4` VARBINARY(64),`c2` BIGINT,`c5` INT,`c6` DOUBLE,`c7` DOUBLE, PRIMARY KEY (`c3` ASC,`c1` DESC,`c4` ASC))",
        "CREATE INDEX index_my_ns_foo_table_c4 ON `my_ns`.`foo_table` (`c4`)",
        "CREATE INDEX index_my_ns_foo_table_c1 ON `my_ns`.`foo_table` (`c1`)",
        "CREATE SCHEMA IF NOT EXISTS `changed`",
        "CREATE TABLE IF NOT EXISTS `changed`.`metadata`("
            + "`full_table_name` VARCHAR(128),"
            + "`column_name` VARCHAR(128),"
            + "`data_type` VARCHAR(20) NOT NULL,"
            + "`key_type` VARCHAR(20),"
            + "`clustering_order` VARCHAR(10),"
            + "`indexed` BOOLEAN NOT NULL,"
            + "`ordinal_position` INTEGER NOT NULL,"
            + "PRIMARY KEY (`full_table_name`, `column_name`))",
        "INSERT INTO `changed`.`metadata` VALUES ('my_ns.foo_table','c3','BOOLEAN','PARTITION',NULL,false,1)",
        "INSERT INTO `changed`.`metadata` VALUES ('my_ns.foo_table','c1','TEXT','CLUSTERING','DESC',true,2)",
        "INSERT INTO `changed`.`metadata` VALUES ('my_ns.foo_table','c4','BLOB','CLUSTERING','ASC',true,3)",
        "INSERT INTO `changed`.`metadata` VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,false,4)",
        "INSERT INTO `changed`.`metadata` VALUES ('my_ns.foo_table','c5','INT',NULL,NULL,false,5)",
        "INSERT INTO `changed`.`metadata` VALUES ('my_ns.foo_table','c6','DOUBLE',NULL,NULL,false,6)",
        "INSERT INTO `changed`.`metadata` VALUES ('my_ns.foo_table','c7','FLOAT',NULL,NULL,false,7)");
  }

  @Test
  public void
      createTable_WithClusteringOrderForPostgresqlWithTableMetadataSchemaChanged_shouldExecuteCreateTableStatement()
          throws ExecutionException, SQLException {
    createTable_WithClusteringOrderForX_shouldExecuteCreateTableStatement(
        RdbEngine.POSTGRESQL,
        Optional.of("changed"),
        "CREATE TABLE \"my_ns\".\"foo_table\"(\"c3\" BOOLEAN,\"c1\" VARCHAR(10485760),\"c4\" BYTEA,\"c2\" BIGINT,\"c5\" INT,\"c6\" DOUBLE PRECISION,\"c7\" FLOAT, PRIMARY KEY (\"c3\",\"c1\",\"c4\"))",
        "CREATE UNIQUE INDEX \"my_ns.foo_table_clustering_order_idx\" ON \"my_ns\".\"foo_table\" (\"c3\" ASC,\"c1\" DESC,\"c4\" ASC)",
        "CREATE INDEX index_my_ns_foo_table_c4 ON \"my_ns\".\"foo_table\" (\"c4\")",
        "CREATE INDEX index_my_ns_foo_table_c1 ON \"my_ns\".\"foo_table\" (\"c1\")",
        "CREATE SCHEMA IF NOT EXISTS \"changed\"",
        "CREATE TABLE IF NOT EXISTS \"changed\".\"metadata\"("
            + "\"full_table_name\" VARCHAR(128),"
            + "\"column_name\" VARCHAR(128),"
            + "\"data_type\" VARCHAR(20) NOT NULL,"
            + "\"key_type\" VARCHAR(20),"
            + "\"clustering_order\" VARCHAR(10),"
            + "\"indexed\" BOOLEAN NOT NULL,"
            + "\"ordinal_position\" INTEGER NOT NULL,"
            + "PRIMARY KEY (\"full_table_name\", \"column_name\"))",
        "INSERT INTO \"changed\".\"metadata\" VALUES ('my_ns.foo_table','c3','BOOLEAN','PARTITION',NULL,false,1)",
        "INSERT INTO \"changed\".\"metadata\" VALUES ('my_ns.foo_table','c1','TEXT','CLUSTERING','DESC',true,2)",
        "INSERT INTO \"changed\".\"metadata\" VALUES ('my_ns.foo_table','c4','BLOB','CLUSTERING','ASC',true,3)",
        "INSERT INTO \"changed\".\"metadata\" VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,false,4)",
        "INSERT INTO \"changed\".\"metadata\" VALUES ('my_ns.foo_table','c5','INT',NULL,NULL,false,5)",
        "INSERT INTO \"changed\".\"metadata\" VALUES ('my_ns.foo_table','c6','DOUBLE',NULL,NULL,false,6)",
        "INSERT INTO \"changed\".\"metadata\" VALUES ('my_ns.foo_table','c7','FLOAT',NULL,NULL,false,7)");
  }

  @Test
  public void
      createTable_WithClusteringOrderForSqlServerWithTableMetadataSchemaChanged_shouldExecuteCreateTableStatement()
          throws ExecutionException, SQLException {
    createTable_WithClusteringOrderForX_shouldExecuteCreateTableStatement(
        RdbEngine.SQL_SERVER,
        Optional.of("changed"),
        "CREATE TABLE [my_ns].[foo_table]([c3] BIT,[c1] VARCHAR(8000) COLLATE Latin1_General_BIN,[c4] VARBINARY(8000),[c2] BIGINT,[c5] INT,[c6] FLOAT,[c7] FLOAT(24), PRIMARY KEY ([c3] ASC,[c1] DESC,[c4] ASC))",
        "CREATE INDEX index_my_ns_foo_table_c4 ON [my_ns].[foo_table] ([c4])",
        "CREATE INDEX index_my_ns_foo_table_c1 ON [my_ns].[foo_table] ([c1])",
        "CREATE SCHEMA [changed]",
        "CREATE TABLE [changed].[metadata]("
            + "[full_table_name] VARCHAR(128),"
            + "[column_name] VARCHAR(128),"
            + "[data_type] VARCHAR(20) NOT NULL,"
            + "[key_type] VARCHAR(20),"
            + "[clustering_order] VARCHAR(10),"
            + "[indexed] BIT NOT NULL,"
            + "[ordinal_position] INTEGER NOT NULL,"
            + "PRIMARY KEY ([full_table_name], [column_name]))",
        "INSERT INTO [changed].[metadata] VALUES ('my_ns.foo_table','c3','BOOLEAN','PARTITION',NULL,0,1)",
        "INSERT INTO [changed].[metadata] VALUES ('my_ns.foo_table','c1','TEXT','CLUSTERING','DESC',1,2)",
        "INSERT INTO [changed].[metadata] VALUES ('my_ns.foo_table','c4','BLOB','CLUSTERING','ASC',1,3)",
        "INSERT INTO [changed].[metadata] VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,0,4)",
        "INSERT INTO [changed].[metadata] VALUES ('my_ns.foo_table','c5','INT',NULL,NULL,0,5)",
        "INSERT INTO [changed].[metadata] VALUES ('my_ns.foo_table','c6','DOUBLE',NULL,NULL,0,6)",
        "INSERT INTO [changed].[metadata] VALUES ('my_ns.foo_table','c7','FLOAT',NULL,NULL,0,7)");
  }

  @Test
  public void
      createTable_WithClusteringOrderForOracleWithTableMetadataSchemaChanged_shouldExecuteCreateTableStatement()
          throws ExecutionException, SQLException {
    createTable_WithClusteringOrderForX_shouldExecuteCreateTableStatement(
        RdbEngine.ORACLE,
        Optional.of("changed"),
        "CREATE TABLE \"my_ns\".\"foo_table\"(\"c3\" NUMBER(1),\"c1\" VARCHAR2(64),\"c4\" RAW(64),\"c2\" NUMBER(19),\"c5\" INT,\"c6\" BINARY_DOUBLE,\"c7\" BINARY_FLOAT, PRIMARY KEY (\"c3\",\"c1\",\"c4\")) ROWDEPENDENCIES",
        "ALTER TABLE \"my_ns\".\"foo_table\" INITRANS 3 MAXTRANS 255",
        "CREATE UNIQUE INDEX \"my_ns.foo_table_clustering_order_idx\" ON \"my_ns\".\"foo_table\" (\"c3\" ASC,\"c1\" DESC,\"c4\" ASC)",
        "CREATE INDEX index_my_ns_foo_table_c4 ON \"my_ns\".\"foo_table\" (\"c4\")",
        "CREATE INDEX index_my_ns_foo_table_c1 ON \"my_ns\".\"foo_table\" (\"c1\")",
        "CREATE USER \"changed\" IDENTIFIED BY \"oracle\"",
        "ALTER USER \"changed\" quota unlimited on USERS",
        "CREATE TABLE \"changed\".\"metadata\"(\"full_table_name\" VARCHAR2(128),\"column_name\" VARCHAR2(128),\"data_type\" VARCHAR2(20) NOT NULL,\"key_type\" VARCHAR2(20),\"clustering_order\" VARCHAR2(10),\"indexed\" NUMBER(1) NOT NULL,\"ordinal_position\" INTEGER NOT NULL,PRIMARY KEY (\"full_table_name\", \"column_name\"))",
        "INSERT INTO \"changed\".\"metadata\" VALUES ('my_ns.foo_table','c3','BOOLEAN','PARTITION',NULL,0,1)",
        "INSERT INTO \"changed\".\"metadata\" VALUES ('my_ns.foo_table','c1','TEXT','CLUSTERING','DESC',1,2)",
        "INSERT INTO \"changed\".\"metadata\" VALUES ('my_ns.foo_table','c4','BLOB','CLUSTERING','ASC',1,3)",
        "INSERT INTO \"changed\".\"metadata\" VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,0,4)",
        "INSERT INTO \"changed\".\"metadata\" VALUES ('my_ns.foo_table','c5','INT',NULL,NULL,0,5)",
        "INSERT INTO \"changed\".\"metadata\" VALUES ('my_ns.foo_table','c6','DOUBLE',NULL,NULL,0,6)",
        "INSERT INTO \"changed\".\"metadata\" VALUES ('my_ns.foo_table','c7','FLOAT',NULL,NULL,0,7)");
  }

  private void createTable_WithClusteringOrderForX_shouldExecuteCreateTableStatement(
      RdbEngine rdbEngine, Optional<String> tableMetadataSchema, String... expectedSqlStatements)
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

    if (tableMetadataSchema.isPresent()) {
      when(config.getTableMetadataSchema()).thenReturn(tableMetadataSchema);
    }
    JdbcAdmin admin = new JdbcAdmin(dataSource, rdbEngine, config);

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
    JdbcAdmin admin = new JdbcAdmin(dataSource, rdbEngine, config);

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
        Optional.empty(),
        "DROP TABLE `my_ns`.`foo_table`",
        "DELETE FROM `scalardb`.`metadata` WHERE `full_table_name` = 'my_ns.foo_table'",
        "SELECT DISTINCT `full_table_name` FROM `scalardb`.`metadata`",
        "DROP TABLE `scalardb`.`metadata`",
        "DROP SCHEMA `scalardb`");
  }

  @Test
  public void
      dropTable_forPostgresqlWithNoMoreMetadataAfterDeletion_shouldDropTableAndDeleteMetadata()
          throws Exception {
    dropTable_forXWithNoMoreMetadataAfterDeletion_shouldDropTableAndDeleteMetadata(
        RdbEngine.POSTGRESQL,
        Optional.empty(),
        "DROP TABLE \"my_ns\".\"foo_table\"",
        "DELETE FROM \"scalardb\".\"metadata\" WHERE \"full_table_name\" = 'my_ns.foo_table'",
        "SELECT DISTINCT \"full_table_name\" FROM \"scalardb\".\"metadata\"",
        "DROP TABLE \"scalardb\".\"metadata\"",
        "DROP SCHEMA \"scalardb\"");
  }

  @Test
  public void
      dropTable_forSqlServerWithNoMoreMetadataAfterDeletion_shouldDropTableAndDeleteMetadata()
          throws Exception {
    dropTable_forXWithNoMoreMetadataAfterDeletion_shouldDropTableAndDeleteMetadata(
        RdbEngine.SQL_SERVER,
        Optional.empty(),
        "DROP TABLE [my_ns].[foo_table]",
        "DELETE FROM [scalardb].[metadata] WHERE [full_table_name] = 'my_ns.foo_table'",
        "SELECT DISTINCT [full_table_name] FROM [scalardb].[metadata]",
        "DROP TABLE [scalardb].[metadata]",
        "DROP SCHEMA [scalardb]");
  }

  @Test
  public void dropTable_forOracleWithNoMoreMetadataAfterDeletion_shouldDropTableAndDeleteMetadata()
      throws Exception {
    dropTable_forXWithNoMoreMetadataAfterDeletion_shouldDropTableAndDeleteMetadata(
        RdbEngine.ORACLE,
        Optional.empty(),
        "DROP TABLE \"my_ns\".\"foo_table\"",
        "DELETE FROM \"scalardb\".\"metadata\" WHERE \"full_table_name\" = 'my_ns.foo_table'",
        "SELECT DISTINCT \"full_table_name\" FROM \"scalardb\".\"metadata\"",
        "DROP TABLE \"scalardb\".\"metadata\"",
        "DROP USER \"scalardb\"");
  }

  @Test
  public void
      dropTable_forMysqlWithNoMoreMetadataAfterDeletionWithTableMetadataSchemaChanged_shouldDropTableAndDeleteMetadata()
          throws Exception {
    dropTable_forXWithNoMoreMetadataAfterDeletion_shouldDropTableAndDeleteMetadata(
        RdbEngine.MYSQL,
        Optional.of("changed"),
        "DROP TABLE `my_ns`.`foo_table`",
        "DELETE FROM `changed`.`metadata` WHERE `full_table_name` = 'my_ns.foo_table'",
        "SELECT DISTINCT `full_table_name` FROM `changed`.`metadata`",
        "DROP TABLE `changed`.`metadata`",
        "DROP SCHEMA `changed`");
  }

  @Test
  public void
      dropTable_forPostgresqlWithNoMoreMetadataAfterDeletionWithTableMetadataSchemaChanged_shouldDropTableAndDeleteMetadata()
          throws Exception {
    dropTable_forXWithNoMoreMetadataAfterDeletion_shouldDropTableAndDeleteMetadata(
        RdbEngine.POSTGRESQL,
        Optional.of("changed"),
        "DROP TABLE \"my_ns\".\"foo_table\"",
        "DELETE FROM \"changed\".\"metadata\" WHERE \"full_table_name\" = 'my_ns.foo_table'",
        "SELECT DISTINCT \"full_table_name\" FROM \"changed\".\"metadata\"",
        "DROP TABLE \"changed\".\"metadata\"",
        "DROP SCHEMA \"changed\"");
  }

  @Test
  public void
      dropTable_forSqlServerWithNoMoreMetadataAfterDeletionWithTableMetadataSchemaChanged_shouldDropTableAndDeleteMetadata()
          throws Exception {
    dropTable_forXWithNoMoreMetadataAfterDeletion_shouldDropTableAndDeleteMetadata(
        RdbEngine.SQL_SERVER,
        Optional.of("changed"),
        "DROP TABLE [my_ns].[foo_table]",
        "DELETE FROM [changed].[metadata] WHERE [full_table_name] = 'my_ns.foo_table'",
        "SELECT DISTINCT [full_table_name] FROM [changed].[metadata]",
        "DROP TABLE [changed].[metadata]",
        "DROP SCHEMA [changed]");
  }

  @Test
  public void
      dropTable_forOracleWithNoMoreMetadataAfterDeletionWithTableMetadataSchemaChanged_shouldDropTableAndDeleteMetadata()
          throws Exception {
    dropTable_forXWithNoMoreMetadataAfterDeletion_shouldDropTableAndDeleteMetadata(
        RdbEngine.ORACLE,
        Optional.of("changed"),
        "DROP TABLE \"my_ns\".\"foo_table\"",
        "DELETE FROM \"changed\".\"metadata\" WHERE \"full_table_name\" = 'my_ns.foo_table'",
        "SELECT DISTINCT \"full_table_name\" FROM \"changed\".\"metadata\"",
        "DROP TABLE \"changed\".\"metadata\"",
        "DROP USER \"changed\"");
  }

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED")
  private void dropTable_forXWithNoMoreMetadataAfterDeletion_shouldDropTableAndDeleteMetadata(
      RdbEngine rdbEngine, Optional<String> tableMetadataSchema, String... expectedSqlStatements)
      throws Exception {
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

    if (tableMetadataSchema.isPresent()) {
      when(config.getTableMetadataSchema()).thenReturn(tableMetadataSchema);
    }
    JdbcAdmin admin = new JdbcAdmin(dataSource, rdbEngine, config);

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
        Optional.empty(),
        "DROP TABLE `my_ns`.`foo_table`",
        "DELETE FROM `scalardb`.`metadata` WHERE `full_table_name` = 'my_ns.foo_table'",
        "SELECT DISTINCT `full_table_name` FROM `scalardb`.`metadata`");
  }

  @Test
  public void
      dropTable_forPostgresqlWithOtherMetadataAfterDeletion_ShouldDropTableAndDeleteMetadataButNotMetadataTable()
          throws Exception {
    dropTable_forXWithOtherMetadataAfterDeletion_ShouldDropTableAndDeleteMetadataButNotMetadataTable(
        RdbEngine.POSTGRESQL,
        Optional.empty(),
        "DROP TABLE \"my_ns\".\"foo_table\"",
        "DELETE FROM \"scalardb\".\"metadata\" WHERE \"full_table_name\" = 'my_ns.foo_table'",
        "SELECT DISTINCT \"full_table_name\" FROM \"scalardb\".\"metadata\"");
  }

  @Test
  public void
      dropTable_forSqlServerWithOtherMetadataAfterDeletion_ShouldDropTableAndDeleteMetadataButNotMetadataTable()
          throws Exception {
    dropTable_forXWithOtherMetadataAfterDeletion_ShouldDropTableAndDeleteMetadataButNotMetadataTable(
        RdbEngine.SQL_SERVER,
        Optional.empty(),
        "DROP TABLE [my_ns].[foo_table]",
        "DELETE FROM [scalardb].[metadata] WHERE [full_table_name] = 'my_ns.foo_table'",
        "SELECT DISTINCT [full_table_name] FROM [scalardb].[metadata]");
  }

  @Test
  public void
      dropTable_forOracleWithOtherMetadataAfterDeletion_ShouldDropTableAndDeleteMetadataButNotMetadataTable()
          throws Exception {
    dropTable_forXWithOtherMetadataAfterDeletion_ShouldDropTableAndDeleteMetadataButNotMetadataTable(
        RdbEngine.ORACLE,
        Optional.empty(),
        "DROP TABLE \"my_ns\".\"foo_table\"",
        "DELETE FROM \"scalardb\".\"metadata\" WHERE \"full_table_name\" = 'my_ns.foo_table'",
        "SELECT DISTINCT \"full_table_name\" FROM \"scalardb\".\"metadata\"");
  }

  @Test
  public void
      dropTable_forMysqlWithOtherMetadataAfterDeletionWithTableMetadataSchemaChanged_ShouldDropTableAndDeleteMetadataButNotMetadataTable()
          throws Exception {
    dropTable_forXWithOtherMetadataAfterDeletion_ShouldDropTableAndDeleteMetadataButNotMetadataTable(
        RdbEngine.MYSQL,
        Optional.of("changed"),
        "DROP TABLE `my_ns`.`foo_table`",
        "DELETE FROM `changed`.`metadata` WHERE `full_table_name` = 'my_ns.foo_table'",
        "SELECT DISTINCT `full_table_name` FROM `changed`.`metadata`");
  }

  @Test
  public void
      dropTable_forPostgresqlWithOtherMetadataAfterDeletionWithTableMetadataSchemaChanged_ShouldDropTableAndDeleteMetadataButNotMetadataTable()
          throws Exception {
    dropTable_forXWithOtherMetadataAfterDeletion_ShouldDropTableAndDeleteMetadataButNotMetadataTable(
        RdbEngine.POSTGRESQL,
        Optional.of("changed"),
        "DROP TABLE \"my_ns\".\"foo_table\"",
        "DELETE FROM \"changed\".\"metadata\" WHERE \"full_table_name\" = 'my_ns.foo_table'",
        "SELECT DISTINCT \"full_table_name\" FROM \"changed\".\"metadata\"");
  }

  @Test
  public void
      dropTable_forSqlServerWithOtherMetadataAfterDeletionWithTableMetadataSchemaChanged_ShouldDropTableAndDeleteMetadataButNotMetadataTable()
          throws Exception {
    dropTable_forXWithOtherMetadataAfterDeletion_ShouldDropTableAndDeleteMetadataButNotMetadataTable(
        RdbEngine.SQL_SERVER,
        Optional.of("changed"),
        "DROP TABLE [my_ns].[foo_table]",
        "DELETE FROM [changed].[metadata] WHERE [full_table_name] = 'my_ns.foo_table'",
        "SELECT DISTINCT [full_table_name] FROM [changed].[metadata]");
  }

  @Test
  public void
      dropTable_forOracleWithOtherMetadataAfterDeletionWithTableMetadataSchemaChanged_ShouldDropTableAndDeleteMetadataButNotMetadataTable()
          throws Exception {
    dropTable_forXWithOtherMetadataAfterDeletion_ShouldDropTableAndDeleteMetadataButNotMetadataTable(
        RdbEngine.ORACLE,
        Optional.of("changed"),
        "DROP TABLE \"my_ns\".\"foo_table\"",
        "DELETE FROM \"changed\".\"metadata\" WHERE \"full_table_name\" = 'my_ns.foo_table'",
        "SELECT DISTINCT \"full_table_name\" FROM \"changed\".\"metadata\"");
  }

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED")
  private void
      dropTable_forXWithOtherMetadataAfterDeletion_ShouldDropTableAndDeleteMetadataButNotMetadataTable(
          RdbEngine rdbEngine,
          Optional<String> tableMetadataSchema,
          String... expectedSqlStatements)
          throws Exception {
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

    if (tableMetadataSchema.isPresent()) {
      when(config.getTableMetadataSchema()).thenReturn(tableMetadataSchema);
    }
    JdbcAdmin admin = new JdbcAdmin(dataSource, rdbEngine, config);

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
    JdbcAdmin admin = new JdbcAdmin(dataSource, rdbEngine, config);

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
        Optional.empty(),
        "SELECT DISTINCT `full_table_name` FROM `scalardb`.`metadata` WHERE `full_table_name` LIKE ?");
  }

  @Test
  public void getNamespaceTables_forPostgresql_ShouldReturnTableNames() throws Exception {
    getNamespaceTables_forX_ShouldReturnTableNames(
        RdbEngine.POSTGRESQL,
        Optional.empty(),
        "SELECT DISTINCT \"full_table_name\" FROM \"scalardb\".\"metadata\" WHERE \"full_table_name\" LIKE ?");
  }

  @Test
  public void getNamespaceTables_forSqlServer_ShouldReturnTableNames() throws Exception {
    getNamespaceTables_forX_ShouldReturnTableNames(
        RdbEngine.SQL_SERVER,
        Optional.empty(),
        "SELECT DISTINCT [full_table_name] FROM [scalardb].[metadata] WHERE [full_table_name] LIKE ?");
  }

  @Test
  public void getNamespaceTables_forOracle_ShouldReturnTableNames() throws Exception {
    getNamespaceTables_forX_ShouldReturnTableNames(
        RdbEngine.ORACLE,
        Optional.empty(),
        "SELECT DISTINCT \"full_table_name\" FROM \"scalardb\".\"metadata\" WHERE \"full_table_name\" LIKE ?");
  }

  @Test
  public void getNamespaceTables_forMysqlWithTableMetadataSchemaChanged_ShouldReturnTableNames()
      throws Exception {
    getNamespaceTables_forX_ShouldReturnTableNames(
        RdbEngine.MYSQL,
        Optional.of("changed"),
        "SELECT DISTINCT `full_table_name` FROM `changed`.`metadata` WHERE `full_table_name` LIKE ?");
  }

  @Test
  public void
      getNamespaceTables_forPostgresqlWithTableMetadataSchemaChanged_ShouldReturnTableNames()
          throws Exception {
    getNamespaceTables_forX_ShouldReturnTableNames(
        RdbEngine.POSTGRESQL,
        Optional.of("changed"),
        "SELECT DISTINCT \"full_table_name\" FROM \"changed\".\"metadata\" WHERE \"full_table_name\" LIKE ?");
  }

  @Test
  public void getNamespaceTables_forSqlServerWithTableMetadataSchemaChanged_ShouldReturnTableNames()
      throws Exception {
    getNamespaceTables_forX_ShouldReturnTableNames(
        RdbEngine.SQL_SERVER,
        Optional.of("changed"),
        "SELECT DISTINCT [full_table_name] FROM [changed].[metadata] WHERE [full_table_name] LIKE ?");
  }

  @Test
  public void getNamespaceTables_forOracleWithTableMetadataSchemaChanged_ShouldReturnTableNames()
      throws Exception {
    getNamespaceTables_forX_ShouldReturnTableNames(
        RdbEngine.ORACLE,
        Optional.of("changed"),
        "SELECT DISTINCT \"full_table_name\" FROM \"changed\".\"metadata\" WHERE \"full_table_name\" LIKE ?");
  }

  private void getNamespaceTables_forX_ShouldReturnTableNames(
      RdbEngine rdbEngine, Optional<String> tableMetadataSchema, String expectedSelectStatement)
      throws Exception {
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

    if (tableMetadataSchema.isPresent()) {
      when(config.getTableMetadataSchema()).thenReturn(tableMetadataSchema);
    }
    JdbcAdmin admin = new JdbcAdmin(dataSource, rdbEngine, config);

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

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED")
  private void namespaceExists_forXWithExistingNamespace_ShouldReturnTrue(
      RdbEngine rdbEngine, String expectedSelectStatement) throws SQLException, ExecutionException {
    // Arrange
    String namespace = "my_ns";
    JdbcAdmin admin = new JdbcAdmin(dataSource, rdbEngine, config);

    Connection connection = mock(Connection.class);
    PreparedStatement selectStatement = mock(PreparedStatement.class);
    ResultSet results = mock(ResultSet.class);

    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.prepareStatement(any())).thenReturn(selectStatement);
    when(results.next()).thenReturn(true);
    when(selectStatement.executeQuery()).thenReturn(results);

    // Act
    // Assert
    assertTrue(admin.namespaceExists(namespace));

    verify(selectStatement).executeQuery();
    verify(connection).prepareStatement(expectedSelectStatement);
    verify(selectStatement).setString(1, namespace);
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
