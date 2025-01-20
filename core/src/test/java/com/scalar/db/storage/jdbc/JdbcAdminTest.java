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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.description;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
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

public class JdbcAdminTest {

  private static final String METADATA_SCHEMA = "scalardb";
  private static final String NAMESPACE = "namespace";
  private static final String TABLE = "table";
  private static final String COLUMN_1 = "c1";
  private static final ImmutableMap<RdbEngine, RdbEngineStrategy> RDB_ENGINES =
      ImmutableMap.of(
          RdbEngine.MYSQL,
          new RdbEngineMysql(),
          RdbEngine.ORACLE,
          new RdbEngineOracle(),
          RdbEngine.POSTGRESQL,
          new RdbEnginePostgresql(),
          RdbEngine.SQL_SERVER,
          new RdbEngineSqlServer(),
          RdbEngine.SQLITE,
          new RdbEngineSqlite(),
          RdbEngine.YUGABYTE,
          new RdbEngineYugabyte(),
          RdbEngine.MARIADB,
          new RdbEngineMariaDB());

  @Mock private BasicDataSource dataSource;
  @Mock private Connection connection;
  @Mock private JdbcConfig config;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    when(config.getMetadataSchema()).thenReturn(METADATA_SCHEMA);
  }

  private JdbcAdmin createJdbcAdminFor(RdbEngine rdbEngine) {
    // Arrange
    RdbEngineStrategy st = RdbEngine.createRdbEngineStrategy(rdbEngine);
    try (MockedStatic<RdbEngineFactory> mocked = mockStatic(RdbEngineFactory.class)) {
      mocked.when(() -> RdbEngineFactory.create(any(JdbcConfig.class))).thenReturn(st);
      return new JdbcAdmin(dataSource, config);
    }
  }

  private JdbcAdmin createJdbcAdminFor(RdbEngineStrategy rdbEngineStrategy) {
    // Arrange
    try (MockedStatic<RdbEngineFactory> mocked = mockStatic(RdbEngineFactory.class)) {
      mocked
          .when(() -> RdbEngineFactory.create(any(JdbcConfig.class)))
          .thenReturn(rdbEngineStrategy);
      return new JdbcAdmin(dataSource, config);
    }
  }

  private void mockUndefinedTableError(RdbEngine rdbEngine, SQLException sqlException) {
    switch (rdbEngine) {
      case MYSQL:
      case MARIADB:
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

  @Test
  public void getTableMetadata_forMysql_ShouldReturnTableMetadata()
      throws SQLException, ExecutionException {
    getTableMetadata_forX_ShouldReturnTableMetadata(
        RdbEngine.MYSQL,
        "SELECT `column_name`,`data_type`,`key_type`,`clustering_order`,`indexed` FROM `"
            + METADATA_SCHEMA
            + "`.`metadata` WHERE `full_table_name`=? ORDER BY `ordinal_position` ASC");
  }

  @Test
  public void getTableMetadata_forPostgresql_ShouldReturnTableMetadata()
      throws SQLException, ExecutionException {
    getTableMetadata_forX_ShouldReturnTableMetadata(
        RdbEngine.POSTGRESQL,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC");
  }

  @Test
  public void getTableMetadata_forSqlServer_ShouldReturnTableMetadata()
      throws SQLException, ExecutionException {
    getTableMetadata_forX_ShouldReturnTableMetadata(
        RdbEngine.SQL_SERVER,
        "SELECT [column_name],[data_type],[key_type],[clustering_order],[indexed] FROM ["
            + METADATA_SCHEMA
            + "].[metadata] WHERE [full_table_name]=? ORDER BY [ordinal_position] ASC");
  }

  @Test
  public void getTableMetadata_forOracle_ShouldReturnTableMetadata()
      throws SQLException, ExecutionException {
    getTableMetadata_forX_ShouldReturnTableMetadata(
        RdbEngine.ORACLE,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC");
  }

  @Test
  public void getTableMetadata_forSqlite_ShouldReturnTableMetadata()
      throws SQLException, ExecutionException {
    getTableMetadata_forX_ShouldReturnTableMetadata(
        RdbEngine.SQLITE,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + METADATA_SCHEMA
            + "$metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC");
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
            new SelectAllFromMetadataTableResultSetMocker.Row(
                "c3", DataType.BOOLEAN.toString(), "PARTITION", null, false),
            new SelectAllFromMetadataTableResultSetMocker.Row(
                "c1", DataType.TEXT.toString(), "CLUSTERING", Order.DESC.toString(), false),
            new SelectAllFromMetadataTableResultSetMocker.Row(
                "c4", DataType.BLOB.toString(), "CLUSTERING", Order.ASC.toString(), true),
            new SelectAllFromMetadataTableResultSetMocker.Row(
                "c2", DataType.BIGINT.toString(), null, null, false),
            new SelectAllFromMetadataTableResultSetMocker.Row(
                "c5", DataType.INT.toString(), null, null, false),
            new SelectAllFromMetadataTableResultSetMocker.Row(
                "c6", DataType.DOUBLE.toString(), null, null, false),
            new SelectAllFromMetadataTableResultSetMocker.Row(
                "c7", DataType.FLOAT.toString(), null, null, false));
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

  public ResultSet mockResultSet(SelectAllFromMetadataTableResultSetMocker.Row... rows)
      throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);
    // Everytime the ResultSet.next() method will be called, the ResultSet.getXXX methods call be
    // mocked to return the current row data
    doAnswer(new SelectAllFromMetadataTableResultSetMocker(Arrays.asList(rows)))
        .when(resultSet)
        .next();
    return resultSet;
  }

  public ResultSet mockResultSet(SelectFullTableNameFromMetadataTableResultSetMocker.Row... rows)
      throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);
    // Everytime the ResultSet.next() method will be called, the ResultSet.getXXX methods call be
    // mocked to return the current row data
    doAnswer(new SelectFullTableNameFromMetadataTableResultSetMocker(Arrays.asList(rows)))
        .when(resultSet)
        .next();
    return resultSet;
  }

  public ResultSet mockResultSet(SelectNamespaceNameFromNamespaceTableResultSetMocker.Row... rows)
      throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);
    // Everytime the ResultSet.next() method will be called, the ResultSet.getXXX methods call be
    // mocked to return the current row data
    doAnswer(new SelectNamespaceNameFromNamespaceTableResultSetMocker(Arrays.asList(rows)))
        .when(resultSet)
        .next();
    return resultSet;
  }

  @Test
  public void getTableMetadata_MetadataSchemaNotExistsForX_ShouldReturnNull()
      throws SQLException, ExecutionException {
    for (RdbEngine rdbEngine : RDB_ENGINES.keySet()) {
      getTableMetadata_MetadataTableNotExistsForX_ShouldReturnNull(rdbEngine);
    }
  }

  private void getTableMetadata_MetadataTableNotExistsForX_ShouldReturnNull(RdbEngine rdbEngine)
      throws SQLException, ExecutionException {
    // Arrange
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    Connection connection = mock(Connection.class);
    PreparedStatement selectStatement = mock(PreparedStatement.class);

    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.prepareStatement(any())).thenReturn(selectStatement);
    SQLException sqlException = mock(SQLException.class);
    mockUndefinedTableError(rdbEngine, sqlException);
    when(selectStatement.executeQuery()).thenThrow(sqlException);

    // Act
    TableMetadata actual = admin.getTableMetadata("my_ns", "my_tbl");

    // Assert
    assertThat(actual).isNull();
  }

  @Test
  public void createNamespace_forMysql_shouldExecuteCreateNamespaceStatement()
      throws ExecutionException, SQLException {
    createNamespace_forX_shouldExecuteCreateNamespaceStatement(
        RdbEngine.MYSQL,
        Collections.singletonList("CREATE SCHEMA `my_ns`"),
        "SELECT 1 FROM `" + METADATA_SCHEMA + "`.`namespaces` LIMIT 1",
        Collections.singletonList("CREATE SCHEMA IF NOT EXISTS `" + METADATA_SCHEMA + "`"),
        "CREATE TABLE IF NOT EXISTS `"
            + METADATA_SCHEMA
            + "`.`namespaces`(`namespace_name` VARCHAR(128), PRIMARY KEY (`namespace_name`))",
        "INSERT INTO `" + METADATA_SCHEMA + "`.`namespaces` VALUES (?)");
  }

  @Test
  public void createNamespace_forPostgresql_shouldExecuteCreateNamespaceStatement()
      throws ExecutionException, SQLException {
    createNamespace_forX_shouldExecuteCreateNamespaceStatement(
        RdbEngine.POSTGRESQL,
        Collections.singletonList("CREATE SCHEMA \"my_ns\""),
        "SELECT 1 FROM \"" + METADATA_SCHEMA + "\".\"namespaces\" LIMIT 1",
        Collections.singletonList("CREATE SCHEMA IF NOT EXISTS \"" + METADATA_SCHEMA + "\""),
        "CREATE TABLE IF NOT EXISTS \""
            + METADATA_SCHEMA
            + "\".\"namespaces\"(\"namespace_name\" VARCHAR(128), PRIMARY KEY (\"namespace_name\"))",
        "INSERT INTO \"" + METADATA_SCHEMA + "\".\"namespaces\" VALUES (?)");
  }

  @Test
  public void createNamespace_forSqlServer_shouldExecuteCreateNamespaceStatement()
      throws ExecutionException, SQLException {
    createNamespace_forX_shouldExecuteCreateNamespaceStatement(
        RdbEngine.SQL_SERVER,
        Collections.singletonList("CREATE SCHEMA [my_ns]"),
        "SELECT TOP 1 1 FROM [" + METADATA_SCHEMA + "].[namespaces]",
        Collections.singletonList("CREATE SCHEMA [" + METADATA_SCHEMA + "]"),
        "CREATE TABLE ["
            + METADATA_SCHEMA
            + "].[namespaces]([namespace_name] VARCHAR(128), PRIMARY KEY ([namespace_name]))",
        "INSERT INTO [" + METADATA_SCHEMA + "].[namespaces] VALUES (?)");
  }

  @Test
  public void createNamespace_forOracle_shouldExecuteCreateNamespaceStatement()
      throws ExecutionException, SQLException {
    createNamespace_forX_shouldExecuteCreateNamespaceStatement(
        RdbEngine.ORACLE,
        Arrays.asList(
            "CREATE USER \"my_ns\" IDENTIFIED BY \"Oracle1234!@#$\"",
            "ALTER USER \"my_ns\" quota unlimited on USERS"),
        "SELECT 1 FROM \"" + METADATA_SCHEMA + "\".\"namespaces\" FETCH FIRST 1 ROWS ONLY",
        Arrays.asList(
            "CREATE USER \"" + METADATA_SCHEMA + "\" IDENTIFIED BY \"Oracle1234!@#$\"",
            "ALTER USER \"" + METADATA_SCHEMA + "\" quota unlimited on USERS"),
        "CREATE TABLE \""
            + METADATA_SCHEMA
            + "\".\"namespaces\"(\"namespace_name\" VARCHAR2(128), PRIMARY KEY (\"namespace_name\"))",
        "INSERT INTO \"" + METADATA_SCHEMA + "\".\"namespaces\" VALUES (?)");
  }

  @Test
  public void createNamespace_forSqlite_shouldExecuteCreateNamespaceStatement()
      throws SQLException, ExecutionException {
    createNamespace_forX_shouldExecuteCreateNamespaceStatement(
        RdbEngine.SQLITE,
        Collections.emptyList(),
        "SELECT 1 FROM \"" + METADATA_SCHEMA + "$namespaces\" LIMIT 1",
        Collections.emptyList(),
        "CREATE TABLE IF NOT EXISTS \""
            + METADATA_SCHEMA
            + "$namespaces\"(\"namespace_name\" TEXT, PRIMARY KEY (\"namespace_name\"))",
        "INSERT INTO \"" + METADATA_SCHEMA + "$namespaces\" VALUES (?)");
  }

  private void createNamespace_forX_shouldExecuteCreateNamespaceStatement(
      RdbEngine rdbEngine,
      List<String> createSchemaSqls,
      String namespacesTableExistsSql,
      List<String> createMetadataSchemaSqls,
      String createNamespacesTableSql,
      String insertNamespaceSql)
      throws SQLException, ExecutionException {
    // Arrange
    String namespace = "my_ns";
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    List<Statement> mockedCreateSchemaStatements = new ArrayList<>();
    for (int i = 0; i < createSchemaSqls.size(); i++) {
      mockedCreateSchemaStatements.add(mock(Statement.class));
    }

    Statement mockedNamespacesTableExistsStatement = mock(Statement.class);
    SQLException sqlException = mock(SQLException.class);
    mockUndefinedTableError(rdbEngine, sqlException);
    when(mockedNamespacesTableExistsStatement.execute(namespacesTableExistsSql))
        .thenThrow(sqlException);

    List<Statement> mockedCreateMetadataSchemaStatements = new ArrayList<>();
    for (int i = 0; i < createMetadataSchemaSqls.size(); i++) {
      mockedCreateMetadataSchemaStatements.add(mock(Statement.class));
    }

    Statement mockedCreateNamespacesTableStatement = mock(Statement.class);

    List<Statement> statementsMock =
        ImmutableList.<Statement>builder()
            .addAll(mockedCreateSchemaStatements)
            .add(mockedNamespacesTableExistsStatement)
            .addAll(mockedCreateMetadataSchemaStatements)
            .add(mockedCreateNamespacesTableStatement)
            .build();
    when(connection.createStatement())
        .thenReturn(
            statementsMock.get(0),
            statementsMock.subList(1, statementsMock.size()).toArray(new Statement[0]));

    PreparedStatement mockedInsertNamespaceStatement1 = mock(PreparedStatement.class);
    PreparedStatement mockedInsertNamespaceStatement2 = mock(PreparedStatement.class);
    when(connection.prepareStatement(insertNamespaceSql))
        .thenReturn(mockedInsertNamespaceStatement1, mockedInsertNamespaceStatement2);

    when(dataSource.getConnection()).thenReturn(connection);

    // Act
    admin.createNamespace(namespace);

    // Assert
    for (int i = 0; i < createSchemaSqls.size(); i++) {
      verify(mockedCreateSchemaStatements.get(i)).execute(createSchemaSqls.get(i));
    }
    verify(mockedNamespacesTableExistsStatement).execute(namespacesTableExistsSql);
    for (int i = 0; i < createMetadataSchemaSqls.size(); i++) {
      verify(mockedCreateMetadataSchemaStatements.get(i)).execute(createMetadataSchemaSqls.get(i));
    }
    verify(mockedCreateNamespacesTableStatement).execute(createNamespacesTableSql);
    verify(mockedInsertNamespaceStatement1).setString(1, METADATA_SCHEMA);
    verify(mockedInsertNamespaceStatement2).setString(1, namespace);
    verify(mockedInsertNamespaceStatement1).execute();
    verify(mockedInsertNamespaceStatement2).execute();
  }

  @Test
  public void createTableInternal_ForSqlite_withInvalidTableName_ShouldThrowExecutionException() {
    // Arrange
    String namespace = "my_ns";
    String table = "foo$table"; // contains namespace separator
    TableMetadata metadata =
        TableMetadata.newBuilder().addPartitionKey("c1").addColumn("c1", DataType.TEXT).build();

    JdbcAdmin admin = createJdbcAdminFor(RdbEngine.SQLITE);

    // Act
    // Assert
    assertThatThrownBy(
            () ->
                admin.createTableInternal(
                    mock(Connection.class), namespace, table, metadata, false))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void createTableInternal_ForMysql_ShouldCreateTableAndIndexes() throws SQLException {
    createTableInternal_ForX_CreateTableAndIndexes(
        RdbEngine.MYSQL,
        "CREATE TABLE `my_ns`.`foo_table`(`c3` BOOLEAN,`c1` VARCHAR(128),`c4` VARBINARY(128),`c2` BIGINT,`c5` INT,`c6` DOUBLE,`c7` REAL, PRIMARY KEY (`c3` ASC,`c1` DESC,`c4` ASC))",
        "CREATE INDEX `index_my_ns_foo_table_c4` ON `my_ns`.`foo_table` (`c4`)",
        "CREATE INDEX `index_my_ns_foo_table_c1` ON `my_ns`.`foo_table` (`c1`)");
  }

  @Test
  public void
      createTableInternal_ForMysqlWithModifiedKeyColumnSize_ShouldCreateTableAndIndexesWithModifiedKeyColumnSize()
          throws SQLException {
    when(config.getMysqlVariableKeyColumnSize()).thenReturn(64);
    createTableInternal_ForX_CreateTableAndIndexes(
        new RdbEngineMysql(config),
        "CREATE TABLE `my_ns`.`foo_table`(`c3` BOOLEAN,`c1` VARCHAR(64),`c4` VARBINARY(64),`c2` BIGINT,`c5` INT,`c6` DOUBLE,`c7` REAL, PRIMARY KEY (`c3` ASC,`c1` DESC,`c4` ASC))",
        "CREATE INDEX `index_my_ns_foo_table_c4` ON `my_ns`.`foo_table` (`c4`)",
        "CREATE INDEX `index_my_ns_foo_table_c1` ON `my_ns`.`foo_table` (`c1`)");
  }

  @Test
  public void createTableInternal_ForPostgresql_ShouldCreateTableAndIndexes() throws SQLException {
    createTableInternal_ForX_CreateTableAndIndexes(
        RdbEngine.POSTGRESQL,
        "CREATE TABLE \"my_ns\".\"foo_table\"(\"c3\" BOOLEAN,\"c1\" VARCHAR(10485760),\"c4\" BYTEA,\"c2\" BIGINT,\"c5\" INT,\"c6\" DOUBLE PRECISION,\"c7\" REAL, PRIMARY KEY (\"c3\",\"c1\",\"c4\"))",
        "CREATE UNIQUE INDEX \"my_ns.foo_table_clustering_order_idx\" ON \"my_ns\".\"foo_table\" (\"c3\" ASC,\"c1\" DESC,\"c4\" ASC)",
        "CREATE INDEX \"index_my_ns_foo_table_c4\" ON \"my_ns\".\"foo_table\" (\"c4\")",
        "CREATE INDEX \"index_my_ns_foo_table_c1\" ON \"my_ns\".\"foo_table\" (\"c1\")");
  }

  @Test
  public void createTableInternal_ForSqlServer_ShouldCreateTableAndIndexes() throws SQLException {
    createTableInternal_ForX_CreateTableAndIndexes(
        RdbEngine.SQL_SERVER,
        "CREATE TABLE [my_ns].[foo_table]([c3] BIT,[c1] VARCHAR(8000),"
            + "[c4] VARBINARY(8000),[c2] BIGINT,[c5] INT,[c6] FLOAT,[c7] FLOAT(24), PRIMARY KEY ([c3] ASC,[c1] DESC,[c4] ASC))",
        "CREATE INDEX [index_my_ns_foo_table_c4] ON [my_ns].[foo_table] ([c4])",
        "CREATE INDEX [index_my_ns_foo_table_c1] ON [my_ns].[foo_table] ([c1])");
  }

  @Test
  public void createTableInternal_ForOracle_ShouldCreateTableAndIndexes() throws SQLException {
    createTableInternal_ForX_CreateTableAndIndexes(
        RdbEngine.ORACLE,
        "CREATE TABLE \"my_ns\".\"foo_table\"(\"c3\" NUMBER(1),\"c1\" VARCHAR2(128),\"c4\" RAW(128),\"c2\" NUMBER(19),\"c5\" NUMBER(10),\"c6\" BINARY_DOUBLE,\"c7\" BINARY_FLOAT, PRIMARY KEY (\"c3\",\"c1\",\"c4\")) ROWDEPENDENCIES",
        "ALTER TABLE \"my_ns\".\"foo_table\" INITRANS 3 MAXTRANS 255",
        "CREATE UNIQUE INDEX \"my_ns.foo_table_clustering_order_idx\" ON \"my_ns\".\"foo_table\" (\"c3\" ASC,\"c1\" DESC,\"c4\" ASC)",
        "CREATE INDEX \"index_my_ns_foo_table_c4\" ON \"my_ns\".\"foo_table\" (\"c4\")",
        "CREATE INDEX \"index_my_ns_foo_table_c1\" ON \"my_ns\".\"foo_table\" (\"c1\")");
  }

  @Test
  public void
      createTableInternal_ForOracleWithModifiedKeyColumnSize_ShouldCreateTableAndIndexesWithModifiedKeyColumnSize()
          throws SQLException {
    when(config.getOracleVariableKeyColumnSize()).thenReturn(64);
    createTableInternal_ForX_CreateTableAndIndexes(
        new RdbEngineOracle(config),
        "CREATE TABLE \"my_ns\".\"foo_table\"(\"c3\" NUMBER(1),\"c1\" VARCHAR2(64),\"c4\" RAW(64),\"c2\" NUMBER(19),\"c5\" NUMBER(10),\"c6\" BINARY_DOUBLE,\"c7\" BINARY_FLOAT, PRIMARY KEY (\"c3\",\"c1\",\"c4\")) ROWDEPENDENCIES",
        "ALTER TABLE \"my_ns\".\"foo_table\" INITRANS 3 MAXTRANS 255",
        "CREATE UNIQUE INDEX \"my_ns.foo_table_clustering_order_idx\" ON \"my_ns\".\"foo_table\" (\"c3\" ASC,\"c1\" DESC,\"c4\" ASC)",
        "CREATE INDEX \"index_my_ns_foo_table_c4\" ON \"my_ns\".\"foo_table\" (\"c4\")",
        "CREATE INDEX \"index_my_ns_foo_table_c1\" ON \"my_ns\".\"foo_table\" (\"c1\")");
  }

  @Test
  public void createTableInternal_ForSqlite_ShouldCreateTableAndIndexes() throws SQLException {
    createTableInternal_ForX_CreateTableAndIndexes(
        RdbEngine.SQLITE,
        "CREATE TABLE \"my_ns$foo_table\"(\"c3\" BOOLEAN,\"c1\" TEXT,\"c4\" BLOB,\"c2\" BIGINT,\"c5\" INT,\"c6\" DOUBLE,\"c7\" FLOAT, PRIMARY KEY (\"c3\",\"c1\",\"c4\"))",
        "CREATE INDEX \"index_my_ns_foo_table_c4\" ON \"my_ns$foo_table\" (\"c4\")",
        "CREATE INDEX \"index_my_ns_foo_table_c1\" ON \"my_ns$foo_table\" (\"c1\")");
  }

  private void createTableInternal_ForX_CreateTableAndIndexes(
      RdbEngine rdbEngine, String... expectedSqlStatements) throws SQLException {
    RdbEngineStrategy rdbEngineStrategy = RdbEngine.createRdbEngineStrategy(rdbEngine);
    createTableInternal_ForX_CreateTableAndIndexes(rdbEngineStrategy, expectedSqlStatements);
  }

  private void createTableInternal_ForX_CreateTableAndIndexes(
      RdbEngineStrategy rdbEngineStrategy, String... expectedSqlStatements) throws SQLException {
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

    JdbcAdmin admin = createJdbcAdminFor(rdbEngineStrategy);

    // Act
    admin.createTableInternal(connection, namespace, table, metadata, false);

    // Assert
    for (int i = 0; i < expectedSqlStatements.length; i++) {
      verify(mockedStatements.get(i)).execute(expectedSqlStatements[i]);
    }
  }

  @Test
  public void createTableInternal_IfNotExistsForMysql_ShouldCreateTableAndIndexesIfNotExists()
      throws SQLException {
    createTableInternal_IfNotExistsForX_createTableAndIndexesIfNotExists(
        RdbEngine.MYSQL,
        "CREATE TABLE IF NOT EXISTS `my_ns`.`foo_table`(`c3` BOOLEAN,`c1` VARCHAR(128),`c4` VARBINARY(128),`c2` BIGINT,`c5` INT,`c6` DOUBLE,`c7` REAL, PRIMARY KEY (`c3` ASC,`c1` DESC,`c4` ASC))",
        "CREATE INDEX `index_my_ns_foo_table_c4` ON `my_ns`.`foo_table` (`c4`)",
        "CREATE INDEX `index_my_ns_foo_table_c1` ON `my_ns`.`foo_table` (`c1`)");
  }

  @Test
  public void createTableInternal_IfNotExistsForPostgresql_ShouldCreateTableAndIndexesIfNotExists()
      throws SQLException {
    createTableInternal_IfNotExistsForX_createTableAndIndexesIfNotExists(
        RdbEngine.POSTGRESQL,
        "CREATE TABLE IF NOT EXISTS \"my_ns\".\"foo_table\"(\"c3\" BOOLEAN,\"c1\" VARCHAR(10485760),\"c4\" BYTEA,\"c2\" BIGINT,\"c5\" INT,\"c6\" DOUBLE PRECISION,\"c7\" REAL, PRIMARY KEY (\"c3\",\"c1\",\"c4\"))",
        "CREATE UNIQUE INDEX IF NOT EXISTS \"my_ns.foo_table_clustering_order_idx\" ON \"my_ns\".\"foo_table\" (\"c3\" ASC,\"c1\" DESC,\"c4\" ASC)",
        "CREATE INDEX IF NOT EXISTS \"index_my_ns_foo_table_c4\" ON \"my_ns\".\"foo_table\" (\"c4\")",
        "CREATE INDEX IF NOT EXISTS \"index_my_ns_foo_table_c1\" ON \"my_ns\".\"foo_table\" (\"c1\")");
  }

  @Test
  public void createTableInternal_IfNotExistsForSqlServer_ShouldCreateTableAndIndexesIfNotExists()
      throws SQLException {
    createTableInternal_IfNotExistsForX_createTableAndIndexesIfNotExists(
        RdbEngine.SQL_SERVER,
        "CREATE TABLE [my_ns].[foo_table]([c3] BIT,[c1] VARCHAR(8000),"
            + "[c4] VARBINARY(8000),[c2] BIGINT,[c5] INT,[c6] FLOAT,[c7] FLOAT(24), PRIMARY KEY ([c3] ASC,[c1] DESC,[c4] ASC))",
        "CREATE INDEX [index_my_ns_foo_table_c4] ON [my_ns].[foo_table] ([c4])",
        "CREATE INDEX [index_my_ns_foo_table_c1] ON [my_ns].[foo_table] ([c1])");
  }

  @Test
  public void createTableInternal_IfNotExistsForOracle_ShouldCreateTableAndIndexesIfNotExists()
      throws SQLException {
    createTableInternal_IfNotExistsForX_createTableAndIndexesIfNotExists(
        RdbEngine.ORACLE,
        "CREATE TABLE \"my_ns\".\"foo_table\"(\"c3\" NUMBER(1),\"c1\" VARCHAR2(128),\"c4\" RAW(128),\"c2\" NUMBER(19),\"c5\" NUMBER(10),\"c6\" BINARY_DOUBLE,\"c7\" BINARY_FLOAT, PRIMARY KEY (\"c3\",\"c1\",\"c4\")) ROWDEPENDENCIES",
        "ALTER TABLE \"my_ns\".\"foo_table\" INITRANS 3 MAXTRANS 255",
        "CREATE UNIQUE INDEX \"my_ns.foo_table_clustering_order_idx\" ON \"my_ns\".\"foo_table\" (\"c3\" ASC,\"c1\" DESC,\"c4\" ASC)",
        "CREATE INDEX \"index_my_ns_foo_table_c4\" ON \"my_ns\".\"foo_table\" (\"c4\")",
        "CREATE INDEX \"index_my_ns_foo_table_c1\" ON \"my_ns\".\"foo_table\" (\"c1\")");
  }

  @Test
  public void createTableInternal_IfNotExistsForSqlite_ShouldCreateTableAndIndexesIfNotExists()
      throws SQLException {
    createTableInternal_IfNotExistsForX_createTableAndIndexesIfNotExists(
        RdbEngine.SQLITE,
        "CREATE TABLE IF NOT EXISTS \"my_ns$foo_table\"(\"c3\" BOOLEAN,\"c1\" TEXT,\"c4\" BLOB,\"c2\" BIGINT,\"c5\" INT,\"c6\" DOUBLE,\"c7\" FLOAT, PRIMARY KEY (\"c3\",\"c1\",\"c4\"))",
        "CREATE INDEX IF NOT EXISTS \"index_my_ns_foo_table_c4\" ON \"my_ns$foo_table\" (\"c4\")",
        "CREATE INDEX IF NOT EXISTS \"index_my_ns_foo_table_c1\" ON \"my_ns$foo_table\" (\"c1\")");
  }

  private void createTableInternal_IfNotExistsForX_createTableAndIndexesIfNotExists(
      RdbEngine rdbEngine, String... expectedSqlStatements) throws SQLException {
    RdbEngineStrategy strategy = RdbEngine.createRdbEngineStrategy(rdbEngine);
    createTableInternal_IfNotExistsForX_createTableAndIndexesIfNotExists(
        strategy, expectedSqlStatements);
  }

  private void createTableInternal_IfNotExistsForX_createTableAndIndexesIfNotExists(
      RdbEngineStrategy rdbEngineStrategy, String... expectedSqlStatements) throws SQLException {
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

    JdbcAdmin admin = createJdbcAdminFor(rdbEngineStrategy);

    // Act
    admin.createTableInternal(connection, namespace, table, metadata, true);

    // Assert
    for (int i = 0; i < expectedSqlStatements.length; i++) {
      verify(mockedStatements.get(i)).execute(expectedSqlStatements[i]);
    }
  }

  @Test
  public void addTableMetadata_OverwriteMetadataForMysql_ShouldWorkProperly() throws Exception {
    addTableMetadata_overwriteMetadataForX_ShouldWorkProperly(
        RdbEngine.MYSQL,
        "DELETE FROM `"
            + METADATA_SCHEMA
            + "`.`metadata` WHERE `full_table_name` = 'my_ns.foo_table'",
        "INSERT INTO `"
            + METADATA_SCHEMA
            + "`.`metadata` VALUES ('my_ns.foo_table','c1','TEXT','PARTITION',NULL,false,1)",
        "INSERT INTO `"
            + METADATA_SCHEMA
            + "`.`metadata` VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,false,2)");
  }

  @Test
  public void addTableMetadata_OverwriteMetadataForPostgresql_ShouldWorkProperly()
      throws Exception {
    addTableMetadata_overwriteMetadataForX_ShouldWorkProperly(
        RdbEngine.POSTGRESQL,
        "DELETE FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\" = 'my_ns.foo_table'",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c1','TEXT','PARTITION',NULL,false,1)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,false,2)");
  }

  @Test
  public void addTableMetadata_OverwriteMetadataForSqlServer_ShouldWorkProperly() throws Exception {
    addTableMetadata_overwriteMetadataForX_ShouldWorkProperly(
        RdbEngine.SQL_SERVER,
        "DELETE FROM ["
            + METADATA_SCHEMA
            + "].[metadata] WHERE [full_table_name] = 'my_ns.foo_table'",
        "INSERT INTO ["
            + METADATA_SCHEMA
            + "].[metadata] VALUES ('my_ns.foo_table','c1','TEXT','PARTITION',NULL,0,1)",
        "INSERT INTO ["
            + METADATA_SCHEMA
            + "].[metadata] VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,0,2)");
  }

  @Test
  public void addTableMetadata_OverwriteMetadataForOracle_ShouldWorkProperly() throws Exception {
    addTableMetadata_overwriteMetadataForX_ShouldWorkProperly(
        RdbEngine.ORACLE,
        "DELETE FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\" = 'my_ns.foo_table'",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c1','TEXT','PARTITION',NULL,0,1)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,0,2)");
  }

  @Test
  public void addTableMetadata_OverwriteMetadataForSqlite_ShouldWorkProperly() throws Exception {
    addTableMetadata_overwriteMetadataForX_ShouldWorkProperly(
        RdbEngine.SQLITE,
        "DELETE FROM \""
            + METADATA_SCHEMA
            + "$metadata\" WHERE \"full_table_name\" = 'my_ns.foo_table'",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "$metadata\" VALUES ('my_ns.foo_table','c1','TEXT','PARTITION',NULL,FALSE,1)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "$metadata\" VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,FALSE,2)");
  }

  private void addTableMetadata_overwriteMetadataForX_ShouldWorkProperly(
      RdbEngine rdbEngine, String... expectedSqlStatements) throws Exception {
    // Arrange
    String namespace = "my_ns";
    String table = "foo_table";
    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addPartitionKey("c1")
            .addColumn("c1", DataType.TEXT)
            .addColumn("c2", DataType.BIGINT)
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
    admin.addTableMetadata(connection, namespace, table, metadata, false, true);

    // Assert
    for (int i = 0; i < expectedSqlStatements.length; i++) {
      verify(mockedStatements.get(i)).execute(expectedSqlStatements[i]);
    }
  }

  @Test
  public void addTableMetadata_ifNotExistsAndOverwriteMetadataForMysql_ShouldWorkProperly()
      throws Exception {
    addTableMetadata_createMetadataTableIfNotExistsForXAndOverwriteMetadata_ShouldWorkProperly(
        RdbEngine.MYSQL,
        "CREATE SCHEMA IF NOT EXISTS `" + METADATA_SCHEMA + "`",
        "CREATE TABLE IF NOT EXISTS `"
            + METADATA_SCHEMA
            + "`.`metadata`("
            + "`full_table_name` VARCHAR(128),"
            + "`column_name` VARCHAR(128),"
            + "`data_type` VARCHAR(20) NOT NULL,"
            + "`key_type` VARCHAR(20),"
            + "`clustering_order` VARCHAR(10),"
            + "`indexed` BOOLEAN NOT NULL,"
            + "`ordinal_position` INTEGER NOT NULL,"
            + "PRIMARY KEY (`full_table_name`, `column_name`))",
        "DELETE FROM `"
            + METADATA_SCHEMA
            + "`.`metadata` WHERE `full_table_name` = 'my_ns.foo_table'",
        "INSERT INTO `"
            + METADATA_SCHEMA
            + "`.`metadata` VALUES ('my_ns.foo_table','c1','TEXT','PARTITION',NULL,false,1)",
        "INSERT INTO `"
            + METADATA_SCHEMA
            + "`.`metadata` VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,false,2)");
  }

  @Test
  public void addTableMetadata_ifNotExistsAndOverwriteMetadataForPostgresql_ShouldWorkProperly()
      throws Exception {
    addTableMetadata_createMetadataTableIfNotExistsForXAndOverwriteMetadata_ShouldWorkProperly(
        RdbEngine.POSTGRESQL,
        "CREATE SCHEMA IF NOT EXISTS \"" + METADATA_SCHEMA + "\"",
        "CREATE TABLE IF NOT EXISTS \""
            + METADATA_SCHEMA
            + "\".\"metadata\"("
            + "\"full_table_name\" VARCHAR(128),"
            + "\"column_name\" VARCHAR(128),"
            + "\"data_type\" VARCHAR(20) NOT NULL,"
            + "\"key_type\" VARCHAR(20),"
            + "\"clustering_order\" VARCHAR(10),"
            + "\"indexed\" BOOLEAN NOT NULL,"
            + "\"ordinal_position\" INTEGER NOT NULL,"
            + "PRIMARY KEY (\"full_table_name\", \"column_name\"))",
        "DELETE FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\" = 'my_ns.foo_table'",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c1','TEXT','PARTITION',NULL,false,1)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,false,2)");
  }

  @Test
  public void addTableMetadata_ifNotExistsAndOverwriteMetadataForSqlServer_ShouldWorkProperly()
      throws Exception {
    addTableMetadata_createMetadataTableIfNotExistsForXAndOverwriteMetadata_ShouldWorkProperly(
        RdbEngine.SQL_SERVER,
        "CREATE SCHEMA [" + METADATA_SCHEMA + "]",
        "CREATE TABLE ["
            + METADATA_SCHEMA
            + "].[metadata]("
            + "[full_table_name] VARCHAR(128),"
            + "[column_name] VARCHAR(128),"
            + "[data_type] VARCHAR(20) NOT NULL,"
            + "[key_type] VARCHAR(20),"
            + "[clustering_order] VARCHAR(10),"
            + "[indexed] BIT NOT NULL,"
            + "[ordinal_position] INTEGER NOT NULL,"
            + "PRIMARY KEY ([full_table_name], [column_name]))",
        "DELETE FROM ["
            + METADATA_SCHEMA
            + "].[metadata] WHERE [full_table_name] = 'my_ns.foo_table'",
        "INSERT INTO ["
            + METADATA_SCHEMA
            + "].[metadata] VALUES ('my_ns.foo_table','c1','TEXT','PARTITION',NULL,0,1)",
        "INSERT INTO ["
            + METADATA_SCHEMA
            + "].[metadata] VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,0,2)");
  }

  @Test
  public void addTableMetadata_ifNotExistsAndOverwriteMetadataForOracle_ShouldWorkProperly()
      throws Exception {
    addTableMetadata_createMetadataTableIfNotExistsForXAndOverwriteMetadata_ShouldWorkProperly(
        RdbEngine.ORACLE,
        "CREATE USER \"" + METADATA_SCHEMA + "\" IDENTIFIED BY \"Oracle1234!@#$\"",
        "ALTER USER \"" + METADATA_SCHEMA + "\" quota unlimited on USERS",
        "CREATE TABLE \""
            + METADATA_SCHEMA
            + "\".\"metadata\"(\"full_table_name\" VARCHAR2(128),\"column_name\" VARCHAR2(128),\"data_type\" VARCHAR2(20) NOT NULL,\"key_type\" VARCHAR2(20),\"clustering_order\" VARCHAR2(10),\"indexed\" NUMBER(1) NOT NULL,\"ordinal_position\" INTEGER NOT NULL,PRIMARY KEY (\"full_table_name\", \"column_name\"))",
        "DELETE FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\" = 'my_ns.foo_table'",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c1','TEXT','PARTITION',NULL,0,1)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,0,2)");
  }

  @Test
  public void addTableMetadata_ifNotExistsAndOverwriteMetadataForSqlite_ShouldWorkProperly()
      throws Exception {
    addTableMetadata_createMetadataTableIfNotExistsForXAndOverwriteMetadata_ShouldWorkProperly(
        RdbEngine.SQLITE,
        "CREATE TABLE IF NOT EXISTS \""
            + METADATA_SCHEMA
            + "$metadata\"("
            + "\"full_table_name\" TEXT,"
            + "\"column_name\" TEXT,"
            + "\"data_type\" TEXT NOT NULL,"
            + "\"key_type\" TEXT,"
            + "\"clustering_order\" TEXT,"
            + "\"indexed\" BOOLEAN NOT NULL,"
            + "\"ordinal_position\" INTEGER NOT NULL,"
            + "PRIMARY KEY (\"full_table_name\", \"column_name\"))",
        "DELETE FROM \""
            + METADATA_SCHEMA
            + "$metadata\" WHERE \"full_table_name\" = 'my_ns.foo_table'",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "$metadata\" VALUES ('my_ns.foo_table','c1','TEXT','PARTITION',NULL,FALSE,1)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "$metadata\" VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,FALSE,2)");
  }

  private void
      addTableMetadata_createMetadataTableIfNotExistsForXAndOverwriteMetadata_ShouldWorkProperly(
          RdbEngine rdbEngine, String... expectedSqlStatements) throws Exception {
    // Arrange
    String namespace = "my_ns";
    String table = "foo_table";
    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addPartitionKey("c1")
            .addColumn("c1", DataType.TEXT)
            .addColumn("c2", DataType.BIGINT)
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
    admin.addTableMetadata(connection, namespace, table, metadata, true, true);

    // Assert
    for (int i = 0; i < expectedSqlStatements.length; i++) {
      verify(mockedStatements.get(i)).execute(expectedSqlStatements[i]);
    }
  }

  @Test
  public void addTableMetadata_ifNotExistsAndDoNotOverwriteMetadataForMysql_ShouldWorkProperly()
      throws Exception {
    addTableMetadata_createMetadataTableIfNotExistsForX_ShouldWorkProperly(
        RdbEngine.MYSQL,
        "CREATE SCHEMA IF NOT EXISTS `" + METADATA_SCHEMA + "`",
        "CREATE TABLE IF NOT EXISTS `"
            + METADATA_SCHEMA
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
            + METADATA_SCHEMA
            + "`.`metadata` VALUES ('my_ns.foo_table','c3','BOOLEAN','PARTITION',NULL,false,1)",
        "INSERT INTO `"
            + METADATA_SCHEMA
            + "`.`metadata` VALUES ('my_ns.foo_table','c1','TEXT','CLUSTERING','DESC',true,2)",
        "INSERT INTO `"
            + METADATA_SCHEMA
            + "`.`metadata` VALUES ('my_ns.foo_table','c4','BLOB','CLUSTERING','ASC',true,3)",
        "INSERT INTO `"
            + METADATA_SCHEMA
            + "`.`metadata` VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,false,4)",
        "INSERT INTO `"
            + METADATA_SCHEMA
            + "`.`metadata` VALUES ('my_ns.foo_table','c5','INT',NULL,NULL,false,5)",
        "INSERT INTO `"
            + METADATA_SCHEMA
            + "`.`metadata` VALUES ('my_ns.foo_table','c6','DOUBLE',NULL,NULL,false,6)",
        "INSERT INTO `"
            + METADATA_SCHEMA
            + "`.`metadata` VALUES ('my_ns.foo_table','c7','FLOAT',NULL,NULL,false,7)");
  }

  @Test
  public void
      addTableMetadata_ifNotExistsAndDoNotOverwriteMetadataForPostgresql_ShouldWorkProperly()
          throws Exception {
    addTableMetadata_createMetadataTableIfNotExistsForX_ShouldWorkProperly(
        RdbEngine.POSTGRESQL,
        "CREATE SCHEMA IF NOT EXISTS \"" + METADATA_SCHEMA + "\"",
        "CREATE TABLE IF NOT EXISTS \""
            + METADATA_SCHEMA
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
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c3','BOOLEAN','PARTITION',NULL,false,1)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c1','TEXT','CLUSTERING','DESC',true,2)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c4','BLOB','CLUSTERING','ASC',true,3)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,false,4)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c5','INT',NULL,NULL,false,5)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c6','DOUBLE',NULL,NULL,false,6)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c7','FLOAT',NULL,NULL,false,7)");
  }

  @Test
  public void addTableMetadata_ifNotExistsAndDoNotOverwriteMetadataForSqlServer_ShouldWorkProperly()
      throws Exception {
    addTableMetadata_createMetadataTableIfNotExistsForX_ShouldWorkProperly(
        RdbEngine.SQL_SERVER,
        "CREATE SCHEMA [" + METADATA_SCHEMA + "]",
        "CREATE TABLE ["
            + METADATA_SCHEMA
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
            + METADATA_SCHEMA
            + "].[metadata] VALUES ('my_ns.foo_table','c3','BOOLEAN','PARTITION',NULL,0,1)",
        "INSERT INTO ["
            + METADATA_SCHEMA
            + "].[metadata] VALUES ('my_ns.foo_table','c1','TEXT','CLUSTERING','DESC',1,2)",
        "INSERT INTO ["
            + METADATA_SCHEMA
            + "].[metadata] VALUES ('my_ns.foo_table','c4','BLOB','CLUSTERING','ASC',1,3)",
        "INSERT INTO ["
            + METADATA_SCHEMA
            + "].[metadata] VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,0,4)",
        "INSERT INTO ["
            + METADATA_SCHEMA
            + "].[metadata] VALUES ('my_ns.foo_table','c5','INT',NULL,NULL,0,5)",
        "INSERT INTO ["
            + METADATA_SCHEMA
            + "].[metadata] VALUES ('my_ns.foo_table','c6','DOUBLE',NULL,NULL,0,6)",
        "INSERT INTO ["
            + METADATA_SCHEMA
            + "].[metadata] VALUES ('my_ns.foo_table','c7','FLOAT',NULL,NULL,0,7)");
  }

  @Test
  public void addTableMetadata_ifNotExistsAndDoNotOverwriteMetadataForOracle_ShouldWorkProperly()
      throws Exception {
    addTableMetadata_createMetadataTableIfNotExistsForX_ShouldWorkProperly(
        RdbEngine.ORACLE,
        "CREATE USER \"" + METADATA_SCHEMA + "\" IDENTIFIED BY \"Oracle1234!@#$\"",
        "ALTER USER \"" + METADATA_SCHEMA + "\" quota unlimited on USERS",
        "CREATE TABLE \""
            + METADATA_SCHEMA
            + "\".\"metadata\"(\"full_table_name\" VARCHAR2(128),\"column_name\" VARCHAR2(128),\"data_type\" VARCHAR2(20) NOT NULL,\"key_type\" VARCHAR2(20),\"clustering_order\" VARCHAR2(10),\"indexed\" NUMBER(1) NOT NULL,\"ordinal_position\" INTEGER NOT NULL,PRIMARY KEY (\"full_table_name\", \"column_name\"))",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c3','BOOLEAN','PARTITION',NULL,0,1)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c1','TEXT','CLUSTERING','DESC',1,2)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c4','BLOB','CLUSTERING','ASC',1,3)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,0,4)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c5','INT',NULL,NULL,0,5)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c6','DOUBLE',NULL,NULL,0,6)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c7','FLOAT',NULL,NULL,0,7)");
  }

  @Test
  public void addTableMetadata_ifNotExistsAndDoNotOverwriteMetadataForSqlite_ShouldWorkProperly()
      throws Exception {
    addTableMetadata_createMetadataTableIfNotExistsForX_ShouldWorkProperly(
        RdbEngine.SQLITE,
        "CREATE TABLE IF NOT EXISTS \""
            + METADATA_SCHEMA
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
            + METADATA_SCHEMA
            + "$metadata\" VALUES ('my_ns.foo_table','c3','BOOLEAN','PARTITION',NULL,FALSE,1)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "$metadata\" VALUES ('my_ns.foo_table','c1','TEXT','CLUSTERING','DESC',TRUE,2)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "$metadata\" VALUES ('my_ns.foo_table','c4','BLOB','CLUSTERING','ASC',TRUE,3)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "$metadata\" VALUES ('my_ns.foo_table','c2','BIGINT',NULL,NULL,FALSE,4)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "$metadata\" VALUES ('my_ns.foo_table','c5','INT',NULL,NULL,FALSE,5)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "$metadata\" VALUES ('my_ns.foo_table','c6','DOUBLE',NULL,NULL,FALSE,6)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "$metadata\" VALUES ('my_ns.foo_table','c7','FLOAT',NULL,NULL,FALSE,7)");
  }

  private void addTableMetadata_createMetadataTableIfNotExistsForX_ShouldWorkProperly(
      RdbEngine rdbEngine, String... expectedSqlStatements) throws Exception {
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
    admin.addTableMetadata(connection, namespace, table, metadata, true, false);

    // Assert
    for (int i = 0; i < expectedSqlStatements.length; i++) {
      verify(mockedStatements.get(i)).execute(expectedSqlStatements[i]);
    }
  }

  @ParameterizedTest
  @EnumSource(RdbEngine.class)
  public void createTable_ShouldCallCreateTableAndAddTableMetadataCorrectly(RdbEngine rdbEngine)
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
    when(connection.createStatement()).thenReturn(mock(Statement.class));
    when(dataSource.getConnection()).thenReturn(connection);

    JdbcAdmin adminSpy = spy(createJdbcAdminFor(rdbEngine));

    // Act
    adminSpy.createTable(namespace, table, metadata, Collections.emptyMap());

    // Assert
    verify(adminSpy).createTableInternal(connection, namespace, table, metadata, false);
    verify(adminSpy).addTableMetadata(connection, namespace, table, metadata, true, false);
  }

  @ParameterizedTest
  @EnumSource(RdbEngine.class)
  public void repairTable_ShouldCallCreateTableAndAddTableMetadataCorrectly(RdbEngine rdbEngine)
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
    when(connection.createStatement()).thenReturn(mock(Statement.class));
    when(dataSource.getConnection()).thenReturn(connection);

    JdbcAdmin adminSpy = spy(createJdbcAdminFor(rdbEngine));

    // Act
    adminSpy.repairTable(namespace, table, metadata, Collections.emptyMap());

    // Assert
    verify(adminSpy).createTableInternal(connection, namespace, table, metadata, true);
    verify(adminSpy).addTableMetadata(connection, namespace, table, metadata, true, true);
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
            + METADATA_SCHEMA
            + "`.`metadata` WHERE `full_table_name` = 'my_ns.foo_table'",
        "SELECT DISTINCT `full_table_name` FROM `" + METADATA_SCHEMA + "`.`metadata`",
        "DROP TABLE `" + METADATA_SCHEMA + "`.`metadata`",
        "SELECT * FROM `" + METADATA_SCHEMA + "`.`namespaces`");
  }

  @Test
  public void
      dropTable_forPostgresqlWithNoMoreMetadataAfterDeletion_shouldDropTableAndDeleteMetadata()
          throws Exception {
    dropTable_forXWithNoMoreMetadataAfterDeletion_shouldDropTableAndDeleteMetadata(
        RdbEngine.POSTGRESQL,
        "DROP TABLE \"my_ns\".\"foo_table\"",
        "DELETE FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\" = 'my_ns.foo_table'",
        "SELECT DISTINCT \"full_table_name\" FROM \"" + METADATA_SCHEMA + "\".\"metadata\"",
        "DROP TABLE \"" + METADATA_SCHEMA + "\".\"metadata\"",
        "SELECT * FROM \"" + METADATA_SCHEMA + "\".\"namespaces\"");
  }

  @Test
  public void
      dropTable_forSqlServerWithNoMoreMetadataAfterDeletion_shouldDropTableAndDeleteMetadata()
          throws Exception {
    dropTable_forXWithNoMoreMetadataAfterDeletion_shouldDropTableAndDeleteMetadata(
        RdbEngine.SQL_SERVER,
        "DROP TABLE [my_ns].[foo_table]",
        "DELETE FROM ["
            + METADATA_SCHEMA
            + "].[metadata] WHERE [full_table_name] = 'my_ns.foo_table'",
        "SELECT DISTINCT [full_table_name] FROM [" + METADATA_SCHEMA + "].[metadata]",
        "DROP TABLE [" + METADATA_SCHEMA + "].[metadata]",
        "SELECT * FROM [" + METADATA_SCHEMA + "].[namespaces]");
  }

  @Test
  public void dropTable_forOracleWithNoMoreMetadataAfterDeletion_shouldDropTableAndDeleteMetadata()
      throws Exception {
    dropTable_forXWithNoMoreMetadataAfterDeletion_shouldDropTableAndDeleteMetadata(
        RdbEngine.ORACLE,
        "DROP TABLE \"my_ns\".\"foo_table\"",
        "DELETE FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\" = 'my_ns.foo_table'",
        "SELECT DISTINCT \"full_table_name\" FROM \"" + METADATA_SCHEMA + "\".\"metadata\"",
        "DROP TABLE \"" + METADATA_SCHEMA + "\".\"metadata\"",
        "SELECT * FROM \"" + METADATA_SCHEMA + "\".\"namespaces\"");
  }

  @Test
  public void dropTable_forSqliteWithNoMoreMetadataAfterDeletion_shouldDropTableAndDeleteMetadata()
      throws Exception {
    dropTable_forXWithNoMoreMetadataAfterDeletion_shouldDropTableAndDeleteMetadata(
        RdbEngine.SQLITE,
        "DROP TABLE \"my_ns$foo_table\"",
        "DELETE FROM \""
            + METADATA_SCHEMA
            + "$metadata\" WHERE \"full_table_name\" = 'my_ns.foo_table'",
        "SELECT DISTINCT \"full_table_name\" FROM \"" + METADATA_SCHEMA + "$metadata\"",
        "DROP TABLE \"" + METADATA_SCHEMA + "$metadata\"",
        "SELECT * FROM \"" + METADATA_SCHEMA + "$namespaces\"");
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

    ResultSet resultSetForSelectAllNamespacesTable =
        mockResultSet(
            new SelectNamespaceNameFromNamespaceTableResultSetMocker.Row(METADATA_SCHEMA),
            new SelectNamespaceNameFromNamespaceTableResultSetMocker.Row(namespace));
    when(mockedStatements.get(mockedStatements.size() - 1).executeQuery(anyString()))
        .thenReturn(resultSetForSelectAllNamespacesTable);

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
            + METADATA_SCHEMA
            + "`.`metadata` WHERE `full_table_name` = 'my_ns.foo_table'",
        "SELECT DISTINCT `full_table_name` FROM `" + METADATA_SCHEMA + "`.`metadata`",
        "SELECT * FROM `" + METADATA_SCHEMA + "`.`namespaces`");
  }

  @Test
  public void
      dropTable_forPostgresqlWithOtherMetadataAfterDeletion_ShouldDropTableAndDeleteMetadataButNotMetadataTable()
          throws Exception {
    dropTable_forXWithOtherMetadataAfterDeletion_ShouldDropTableAndDeleteMetadataButNotMetadataTable(
        RdbEngine.POSTGRESQL,
        "DROP TABLE \"my_ns\".\"foo_table\"",
        "DELETE FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\" = 'my_ns.foo_table'",
        "SELECT DISTINCT \"full_table_name\" FROM \"" + METADATA_SCHEMA + "\".\"metadata\"",
        "SELECT * FROM \"" + METADATA_SCHEMA + "\".\"namespaces\"");
  }

  @Test
  public void
      dropTable_forSqlServerWithOtherMetadataAfterDeletion_ShouldDropTableAndDeleteMetadataButNotMetadataTable()
          throws Exception {
    dropTable_forXWithOtherMetadataAfterDeletion_ShouldDropTableAndDeleteMetadataButNotMetadataTable(
        RdbEngine.SQL_SERVER,
        "DROP TABLE [my_ns].[foo_table]",
        "DELETE FROM ["
            + METADATA_SCHEMA
            + "].[metadata] WHERE [full_table_name] = 'my_ns.foo_table'",
        "SELECT DISTINCT [full_table_name] FROM [" + METADATA_SCHEMA + "].[metadata]",
        "SELECT * FROM [" + METADATA_SCHEMA + "].[namespaces]");
  }

  @Test
  public void
      dropTable_forOracleWithOtherMetadataAfterDeletion_ShouldDropTableAndDeleteMetadataButNotMetadataTable()
          throws Exception {
    dropTable_forXWithOtherMetadataAfterDeletion_ShouldDropTableAndDeleteMetadataButNotMetadataTable(
        RdbEngine.ORACLE,
        "DROP TABLE \"my_ns\".\"foo_table\"",
        "DELETE FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\" = 'my_ns.foo_table'",
        "SELECT DISTINCT \"full_table_name\" FROM \"" + METADATA_SCHEMA + "\".\"metadata\"",
        "SELECT * FROM \"" + METADATA_SCHEMA + "\".\"namespaces\"");
  }

  @Test
  public void
      dropTable_forSqliteWithOtherMetadataAfterDeletion_ShouldDropTableAndDeleteMetadataButNotMetadataTable()
          throws Exception {
    dropTable_forXWithOtherMetadataAfterDeletion_ShouldDropTableAndDeleteMetadataButNotMetadataTable(
        RdbEngine.SQLITE,
        "DROP TABLE \"my_ns$foo_table\"",
        "DELETE FROM \""
            + METADATA_SCHEMA
            + "$metadata\" WHERE \"full_table_name\" = 'my_ns.foo_table'",
        "SELECT DISTINCT \"full_table_name\" FROM \"" + METADATA_SCHEMA + "$metadata\"",
        "SELECT * FROM \"" + METADATA_SCHEMA + "$namespaces\"");
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

    ResultSet resultSetForSelectAllNamespacesTable =
        mockResultSet(
            new SelectNamespaceNameFromNamespaceTableResultSetMocker.Row(METADATA_SCHEMA),
            new SelectNamespaceNameFromNamespaceTableResultSetMocker.Row(namespace));
    when(mockedStatements.get(mockedStatements.size() - 1).executeQuery(anyString()))
        .thenReturn(resultSetForSelectAllNamespacesTable);

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
  public void dropNamespace_WithOnlyNamespaceSchemaLeftForMysql_shouldDropSchemaAndNamespacesTable()
      throws Exception {
    dropNamespace_WithOnlyNamespaceSchemaLeftForX_shouldDropSchemaAndNamespacesTable(
        RdbEngine.MYSQL,
        "DROP SCHEMA `my_ns`",
        "DELETE FROM `" + METADATA_SCHEMA + "`.`namespaces` WHERE `namespace_name` = ?",
        "SELECT * FROM `" + METADATA_SCHEMA + "`.`namespaces`",
        "SELECT 1 FROM `" + METADATA_SCHEMA + "`.`metadata` LIMIT 1",
        "DROP TABLE `" + METADATA_SCHEMA + "`.`namespaces`",
        "DROP SCHEMA `" + METADATA_SCHEMA + "`");
  }

  @Test
  public void
      dropNamespace_WithOnlyNamespaceSchemaLeftForPostgresql_shouldDropSchemaAndNamespacesTable()
          throws Exception {
    dropNamespace_WithOnlyNamespaceSchemaLeftForX_shouldDropSchemaAndNamespacesTable(
        RdbEngine.POSTGRESQL,
        "DROP SCHEMA \"my_ns\"",
        "DELETE FROM \"" + METADATA_SCHEMA + "\".\"namespaces\" WHERE \"namespace_name\" = ?",
        "SELECT * FROM \"" + METADATA_SCHEMA + "\".\"namespaces\"",
        "SELECT 1 FROM \"" + METADATA_SCHEMA + "\".\"metadata\" LIMIT 1",
        "DROP TABLE \"" + METADATA_SCHEMA + "\".\"namespaces\"",
        "DROP SCHEMA \"" + METADATA_SCHEMA + "\"");
  }

  @Test
  public void
      dropNamespace_WithOnlyNamespaceSchemaLeftForSqlServer_shouldDropSchemaAndNamespacesTable()
          throws Exception {
    dropNamespace_WithOnlyNamespaceSchemaLeftForX_shouldDropSchemaAndNamespacesTable(
        RdbEngine.SQL_SERVER,
        "DROP SCHEMA [my_ns]",
        "DELETE FROM [" + METADATA_SCHEMA + "].[namespaces] WHERE [namespace_name] = ?",
        "SELECT * FROM [" + METADATA_SCHEMA + "].[namespaces]",
        "SELECT TOP 1 1 FROM [" + METADATA_SCHEMA + "].[metadata]",
        "DROP TABLE [" + METADATA_SCHEMA + "].[namespaces]",
        "DROP SCHEMA [" + METADATA_SCHEMA + "]");
  }

  @Test
  public void
      dropNamespace_WithOnlyNamespaceSchemaLeftForOracle_shouldDropSchemaAndNamespacesTable()
          throws Exception {
    dropNamespace_WithOnlyNamespaceSchemaLeftForX_shouldDropSchemaAndNamespacesTable(
        RdbEngine.ORACLE,
        "DROP USER \"my_ns\"",
        "DELETE FROM \"" + METADATA_SCHEMA + "\".\"namespaces\" WHERE \"namespace_name\" = ?",
        "SELECT * FROM \"" + METADATA_SCHEMA + "\".\"namespaces\"",
        "SELECT 1 FROM \"" + METADATA_SCHEMA + "\".\"metadata\" FETCH FIRST 1 ROWS ONLY",
        "DROP TABLE \"" + METADATA_SCHEMA + "\".\"namespaces\"",
        "DROP USER \"" + METADATA_SCHEMA + "\"");
  }

  @Test
  public void dropNamespace_forSqlite_shouldDropNamespace() throws Exception {
    // Arrange
    String deleteFromNamespaceTableQuery =
        "DELETE FROM \"" + METADATA_SCHEMA + "$namespaces\" WHERE \"namespace_name\" = ?";
    String selectAllFromNamespaceTableQuery =
        "SELECT * FROM \"" + METADATA_SCHEMA + "$namespaces\"";
    String selectAllFromMetadataTableQuery =
        "SELECT 1 FROM \"" + METADATA_SCHEMA + "$metadata\" LIMIT 1";
    String dropNamespaceTableQuery = "DROP TABLE \"" + METADATA_SCHEMA + "$namespaces\"";

    String namespace = "my_ns";
    JdbcAdmin admin = createJdbcAdminFor(RdbEngine.SQLITE);

    Connection connection = mock(Connection.class);
    PreparedStatement deleteFromNamespaceTablePrepStmt = mock(PreparedStatement.class);
    Statement selectAllFromNamespaceTablePrepStmt = mock(Statement.class);
    Statement selectAllFromMetadataTablePrepStmt = mock(Statement.class);
    Statement dropNamespaceTableStmt = mock(Statement.class);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.createStatement())
        .thenReturn(
            selectAllFromNamespaceTablePrepStmt,
            selectAllFromMetadataTablePrepStmt,
            dropNamespaceTableStmt);
    when(connection.prepareStatement(anyString())).thenReturn(deleteFromNamespaceTablePrepStmt);
    when(dataSource.getConnection()).thenReturn(connection);
    // Only the metadata schema is left
    ResultSet resultSet1 =
        mockResultSet(
            new SelectNamespaceNameFromNamespaceTableResultSetMocker.Row(METADATA_SCHEMA));
    when(selectAllFromNamespaceTablePrepStmt.executeQuery(anyString())).thenReturn(resultSet1);

    SQLException sqlException = mock(SQLException.class);
    mockUndefinedTableError(RdbEngine.SQLITE, sqlException);
    when(selectAllFromMetadataTablePrepStmt.execute(anyString())).thenThrow(sqlException);

    // Act
    admin.dropNamespace(namespace);

    // Assert
    verify(deleteFromNamespaceTablePrepStmt).setString(1, namespace);
    verify(connection).prepareStatement(deleteFromNamespaceTableQuery);
    verify(selectAllFromNamespaceTablePrepStmt).executeQuery(selectAllFromNamespaceTableQuery);
    verify(selectAllFromMetadataTablePrepStmt).execute(selectAllFromMetadataTableQuery);
    verify(dropNamespaceTableStmt).execute(dropNamespaceTableQuery);
  }

  private void dropNamespace_WithOnlyNamespaceSchemaLeftForX_shouldDropSchemaAndNamespacesTable(
      RdbEngine rdbEngine,
      String dropNamespaceQuery,
      String deleteFromNamespaceTableQuery,
      String selectAllFromNamespaceTableQuery,
      String selectAllFromMetadataTableQuery,
      String dropNamespaceTableQuery,
      String dropMetadataSchemaQuery)
      throws Exception {
    // Arrange
    String namespace = "my_ns";
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    Connection connection = mock(Connection.class);
    Statement dropNamespaceStmt = mock(Statement.class);
    PreparedStatement deleteFromNamespaceTablePrepStmt = mock(PreparedStatement.class);
    Statement selectAllFromNamespaceTablePrepStmt = mock(Statement.class);
    Statement selectAllFromMetadataTablePrepStmt = mock(Statement.class);
    Statement dropNamespaceTableStmt = mock(Statement.class);
    Statement dropMetadataSchemaStmt = mock(Statement.class);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.createStatement())
        .thenReturn(
            dropNamespaceStmt,
            selectAllFromNamespaceTablePrepStmt,
            selectAllFromMetadataTablePrepStmt,
            dropNamespaceTableStmt,
            dropMetadataSchemaStmt);
    when(connection.prepareStatement(anyString())).thenReturn(deleteFromNamespaceTablePrepStmt);
    when(dataSource.getConnection()).thenReturn(connection);
    // Only the metadata schema is left
    ResultSet resultSet =
        mockResultSet(
            new SelectNamespaceNameFromNamespaceTableResultSetMocker.Row(METADATA_SCHEMA));
    when(selectAllFromNamespaceTablePrepStmt.executeQuery(anyString())).thenReturn(resultSet);

    SQLException sqlException = mock(SQLException.class);
    mockUndefinedTableError(rdbEngine, sqlException);
    when(selectAllFromMetadataTablePrepStmt.execute(anyString())).thenThrow(sqlException);

    // Act
    admin.dropNamespace(namespace);

    // Assert
    verify(dropNamespaceStmt).execute(dropNamespaceQuery);
    verify(deleteFromNamespaceTablePrepStmt).setString(1, namespace);
    verify(connection).prepareStatement(deleteFromNamespaceTableQuery);
    verify(deleteFromNamespaceTablePrepStmt).execute();
    verify(selectAllFromNamespaceTablePrepStmt).executeQuery(selectAllFromNamespaceTableQuery);
    verify(selectAllFromMetadataTablePrepStmt).execute(selectAllFromMetadataTableQuery);
    verify(dropNamespaceTableStmt).execute(dropNamespaceTableQuery);
    verify(dropMetadataSchemaStmt).execute(dropMetadataSchemaQuery);
  }

  @Test
  public void dropNamespace_WithOtherNamespaceLeftForMysql_shouldOnlyDropNamespace()
      throws Exception {
    dropNamespace_WithOtherNamespaceLeftForX_shouldOnlyDropNamespace(
        RdbEngine.MYSQL,
        "DROP SCHEMA `my_ns`",
        "DELETE FROM `" + METADATA_SCHEMA + "`.`namespaces` WHERE `namespace_name` = ?",
        "SELECT * FROM `" + METADATA_SCHEMA + "`.`namespaces`");
  }

  @Test
  public void dropNamespace_WithOtherNamespaceLeftForPostgresql_shouldOnlyDropNamespace()
      throws Exception {
    dropNamespace_WithOtherNamespaceLeftForX_shouldOnlyDropNamespace(
        RdbEngine.POSTGRESQL,
        "DROP SCHEMA \"my_ns\"",
        "DELETE FROM \"" + METADATA_SCHEMA + "\".\"namespaces\" WHERE \"namespace_name\" = ?",
        "SELECT * FROM \"" + METADATA_SCHEMA + "\".\"namespaces\"");
  }

  @Test
  public void dropNamespace_WithOtherNamespaceLeftForSqlServer_shouldOnlyDropNamespace()
      throws Exception {
    dropNamespace_WithOtherNamespaceLeftForX_shouldOnlyDropNamespace(
        RdbEngine.SQL_SERVER,
        "DROP SCHEMA [my_ns]",
        "DELETE FROM [" + METADATA_SCHEMA + "].[namespaces] WHERE [namespace_name] = ?",
        "SELECT * FROM [" + METADATA_SCHEMA + "].[namespaces]");
  }

  @Test
  public void dropNamespace_WithOtherNamespaceLeftForOracle_shouldOnlyDropNamespace()
      throws Exception {
    dropNamespace_WithOtherNamespaceLeftForX_shouldOnlyDropNamespace(
        RdbEngine.ORACLE,
        "DROP USER \"my_ns\"",
        "DELETE FROM \"" + METADATA_SCHEMA + "\".\"namespaces\" WHERE \"namespace_name\" = ?",
        "SELECT * FROM \"" + METADATA_SCHEMA + "\".\"namespaces\"");
  }

  @Test
  public void dropNamespace_WithOtherNamespaceLeftForSqlLite_shouldOnlyDropNamespace()
      throws Exception {
    dropNamespace_WithOtherNamespaceLeftForX_shouldOnlyDropNamespace(
        RdbEngine.SQLITE,
        "unused",
        "DELETE FROM \"" + METADATA_SCHEMA + "$namespaces\" WHERE \"namespace_name\" = ?",
        "SELECT * FROM \"" + METADATA_SCHEMA + "$namespaces\"");
  }

  private void dropNamespace_WithOtherNamespaceLeftForX_shouldOnlyDropNamespace(
      RdbEngine rdbEngine,
      String dropNamespaceStatement,
      String deleteFromNamespaceTable,
      String selectNamespaceStatement)
      throws Exception {
    // Arrange
    String namespace = "my_ns";
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    Connection connection = mock(Connection.class);
    Statement dropNamespaceStatementMock = mock(Statement.class);
    PreparedStatement deleteFromNamespaceTableMock = mock(PreparedStatement.class);
    Statement selectNamespaceStatementMock = mock(Statement.class);
    if (rdbEngine != RdbEngine.SQLITE) {
      when(connection.createStatement())
          .thenReturn(dropNamespaceStatementMock, selectNamespaceStatementMock);
    } else {
      when(connection.createStatement()).thenReturn(selectNamespaceStatementMock);
    }
    when(connection.prepareStatement(anyString())).thenReturn(deleteFromNamespaceTableMock);
    when(dataSource.getConnection()).thenReturn(connection);
    // Namespaces table contains other namespaces
    ResultSet resultSet =
        mockResultSet(
            new SelectFullTableNameFromMetadataTableResultSetMocker.Row(namespace + ".tbl1"));
    when(selectNamespaceStatementMock.executeQuery(anyString())).thenReturn(resultSet);

    // Act
    admin.dropNamespace(namespace);

    // Assert
    if (rdbEngine != RdbEngine.SQLITE) {
      verify(dropNamespaceStatementMock).execute(dropNamespaceStatement);
    }
    verify(connection).prepareStatement(deleteFromNamespaceTable);
    verify(deleteFromNamespaceTableMock).setString(1, namespace);
    verify(deleteFromNamespaceTableMock).execute();
    verify(selectNamespaceStatementMock).executeQuery(selectNamespaceStatement);
  }

  @Test
  public void getNamespaceTables_forMysql_ShouldReturnTableNames() throws Exception {
    getNamespaceTables_forX_ShouldReturnTableNames(
        RdbEngine.MYSQL,
        "SELECT DISTINCT `full_table_name` FROM `"
            + METADATA_SCHEMA
            + "`.`metadata` WHERE `full_table_name` LIKE ?");
  }

  @Test
  public void getNamespaceTables_forPostgresql_ShouldReturnTableNames() throws Exception {
    getNamespaceTables_forX_ShouldReturnTableNames(
        RdbEngine.POSTGRESQL,
        "SELECT DISTINCT \"full_table_name\" FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\" LIKE ?");
  }

  @Test
  public void getNamespaceTables_forSqlServer_ShouldReturnTableNames() throws Exception {
    getNamespaceTables_forX_ShouldReturnTableNames(
        RdbEngine.SQL_SERVER,
        "SELECT DISTINCT [full_table_name] FROM ["
            + METADATA_SCHEMA
            + "].[metadata] WHERE [full_table_name] LIKE ?");
  }

  @Test
  public void getNamespaceTables_forOracle_ShouldReturnTableNames() throws Exception {
    getNamespaceTables_forX_ShouldReturnTableNames(
        RdbEngine.ORACLE,
        "SELECT DISTINCT \"full_table_name\" FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\" LIKE ?");
  }

  @Test
  public void getNamespaceTables_forSqlite_ShouldReturnTableNames() throws Exception {
    getNamespaceTables_forX_ShouldReturnTableNames(
        RdbEngine.SQLITE,
        "SELECT DISTINCT \"full_table_name\" FROM \""
            + METADATA_SCHEMA
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
            new SelectFullTableNameFromMetadataTableResultSetMocker(
                Arrays.asList(
                    new SelectFullTableNameFromMetadataTableResultSetMocker.Row(namespace + ".t1"),
                    new SelectFullTableNameFromMetadataTableResultSetMocker.Row(
                        namespace + ".t2"))))
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
        "SELECT 1 FROM `" + METADATA_SCHEMA + "`.`namespaces` WHERE `namespace_name` = ?");
  }

  @Test
  public void namespaceExists_forPostgresqlWithExistingNamespace_shouldReturnTrue()
      throws Exception {
    namespaceExists_forXWithExistingNamespace_ShouldReturnTrue(
        RdbEngine.POSTGRESQL,
        "SELECT 1 FROM \"" + METADATA_SCHEMA + "\".\"namespaces\" WHERE \"namespace_name\" = ?");
  }

  @Test
  public void namespaceExists_forSqlServerWithExistingNamespace_shouldReturnTrue()
      throws Exception {
    namespaceExists_forXWithExistingNamespace_ShouldReturnTrue(
        RdbEngine.SQL_SERVER,
        "SELECT 1 FROM [" + METADATA_SCHEMA + "].[namespaces] WHERE [namespace_name] = ?");
  }

  @Test
  public void namespaceExists_forOracleWithExistingNamespace_shouldReturnTrue() throws Exception {
    namespaceExists_forXWithExistingNamespace_ShouldReturnTrue(
        RdbEngine.ORACLE,
        "SELECT 1 FROM \"" + METADATA_SCHEMA + "\".\"namespaces\" WHERE \"namespace_name\" = ?");
  }

  @Test
  public void namespaceExists_forSqliteWithExistingNamespace_shouldReturnTrue() throws Exception {
    namespaceExists_forXWithExistingNamespace_ShouldReturnTrue(
        RdbEngine.SQLITE,
        "SELECT 1 FROM \"" + METADATA_SCHEMA + "$namespaces\" WHERE \"namespace_name\" = ?");
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
            + METADATA_SCHEMA
            + "`.`metadata` WHERE `full_table_name`=? ORDER BY `ordinal_position` ASC",
        "CREATE INDEX `index_my_ns_my_tbl_my_column` ON `my_ns`.`my_tbl` (`my_column`)",
        "UPDATE `"
            + METADATA_SCHEMA
            + "`.`metadata` SET `indexed`=true WHERE `full_table_name`='my_ns.my_tbl' AND `column_name`='my_column'");
  }

  @Test
  public void
      createIndex_ForColumnTypeWithoutRequiredAlterationForPostgresql_ShouldCreateIndexProperly()
          throws Exception {
    createIndex_ForColumnTypeWithoutRequiredAlterationForX_ShouldCreateIndexProperly(
        RdbEngine.POSTGRESQL,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "CREATE INDEX \"index_my_ns_my_tbl_my_column\" ON \"my_ns\".\"my_tbl\" (\"my_column\")",
        "UPDATE \""
            + METADATA_SCHEMA
            + "\".\"metadata\" SET \"indexed\"=true WHERE \"full_table_name\"='my_ns.my_tbl' AND \"column_name\"='my_column'");
  }

  @Test
  public void
      createIndex_ForColumnTypeWithoutRequiredAlterationForSqlServer_ShouldCreateIndexProperly()
          throws Exception {
    createIndex_ForColumnTypeWithoutRequiredAlterationForX_ShouldCreateIndexProperly(
        RdbEngine.SQL_SERVER,
        "SELECT [column_name],[data_type],[key_type],[clustering_order],[indexed] FROM ["
            + METADATA_SCHEMA
            + "].[metadata] WHERE [full_table_name]=? ORDER BY [ordinal_position] ASC",
        "CREATE INDEX [index_my_ns_my_tbl_my_column] ON [my_ns].[my_tbl] ([my_column])",
        "UPDATE ["
            + METADATA_SCHEMA
            + "].[metadata] SET [indexed]=1 WHERE [full_table_name]='my_ns.my_tbl' AND [column_name]='my_column'");
  }

  @Test
  public void
      createIndex_ForColumnTypeWithoutRequiredAlterationForOracle_ShouldCreateIndexProperly()
          throws Exception {
    createIndex_ForColumnTypeWithoutRequiredAlterationForX_ShouldCreateIndexProperly(
        RdbEngine.ORACLE,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "CREATE INDEX \"index_my_ns_my_tbl_my_column\" ON \"my_ns\".\"my_tbl\" (\"my_column\")",
        "UPDATE \""
            + METADATA_SCHEMA
            + "\".\"metadata\" SET \"indexed\"=1 WHERE \"full_table_name\"='my_ns.my_tbl' AND \"column_name\"='my_column'");
  }

  @Test
  public void
      createIndex_ForColumnTypeWithoutRequiredAlterationForSqlite_ShouldCreateIndexProperly()
          throws Exception {
    createIndex_ForColumnTypeWithoutRequiredAlterationForX_ShouldCreateIndexProperly(
        RdbEngine.SQLITE,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + METADATA_SCHEMA
            + "$metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "CREATE INDEX \"index_my_ns_my_tbl_my_column\" ON \"my_ns$my_tbl\" (\"my_column\")",
        "UPDATE \""
            + METADATA_SCHEMA
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

    PreparedStatement selectStatement = mock(PreparedStatement.class);
    ResultSet resultSet =
        mockResultSet(
            new SelectAllFromMetadataTableResultSetMocker.Row(
                "c1", DataType.BOOLEAN.toString(), "PARTITION", null, false),
            new SelectAllFromMetadataTableResultSetMocker.Row(
                indexColumn, DataType.BOOLEAN.toString(), null, null, false));
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
            + METADATA_SCHEMA
            + "`.`metadata` WHERE `full_table_name`=? ORDER BY `ordinal_position` ASC",
        "ALTER TABLE `my_ns`.`my_tbl` MODIFY`my_column` VARCHAR(128)",
        "CREATE INDEX `index_my_ns_my_tbl_my_column` ON `my_ns`.`my_tbl` (`my_column`)",
        "UPDATE `"
            + METADATA_SCHEMA
            + "`.`metadata` SET `indexed`=true WHERE `full_table_name`='my_ns.my_tbl' AND `column_name`='my_column'");
  }

  @Test
  public void
      createIndex_forColumnTypeWithRequiredAlterationForPostgresql_ShouldAlterColumnAndCreateIndexProperly()
          throws Exception {
    createIndex_forColumnTypeWithRequiredAlterationForX_ShouldAlterColumnAndCreateIndexProperly(
        RdbEngine.POSTGRESQL,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "ALTER TABLE \"my_ns\".\"my_tbl\" ALTER COLUMN\"my_column\" TYPE VARCHAR(10485760)",
        "CREATE INDEX \"index_my_ns_my_tbl_my_column\" ON \"my_ns\".\"my_tbl\" (\"my_column\")",
        "UPDATE \""
            + METADATA_SCHEMA
            + "\".\"metadata\" SET \"indexed\"=true WHERE \"full_table_name\"='my_ns.my_tbl' AND \"column_name\"='my_column'");
  }

  @Test
  public void
      createIndex_forColumnTypeWithRequiredAlterationForOracle_ShouldAlterColumnAndCreateIndexProperly()
          throws Exception {
    createIndex_forColumnTypeWithRequiredAlterationForX_ShouldAlterColumnAndCreateIndexProperly(
        RdbEngine.ORACLE,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "ALTER TABLE \"my_ns\".\"my_tbl\" MODIFY ( \"my_column\" VARCHAR2(128) )",
        "CREATE INDEX \"index_my_ns_my_tbl_my_column\" ON \"my_ns\".\"my_tbl\" (\"my_column\")",
        "UPDATE \""
            + METADATA_SCHEMA
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

    PreparedStatement selectStatement = mock(PreparedStatement.class);
    ResultSet resultSet =
        mockResultSet(
            new SelectAllFromMetadataTableResultSetMocker.Row(
                "c1", DataType.BOOLEAN.toString(), "PARTITION", null, false),
            new SelectAllFromMetadataTableResultSetMocker.Row(
                indexColumn, DataType.TEXT.toString(), null, null, false));
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
            + METADATA_SCHEMA
            + "`.`metadata` WHERE `full_table_name`=? ORDER BY `ordinal_position` ASC",
        "DROP INDEX `index_my_ns_my_tbl_my_column` ON `my_ns`.`my_tbl`",
        "UPDATE `"
            + METADATA_SCHEMA
            + "`.`metadata` SET `indexed`=false WHERE `full_table_name`='my_ns.my_tbl' AND `column_name`='my_column'");
  }

  @Test
  public void
      dropIndex_forColumnTypeWithoutRequiredAlterationForPostgresql_ShouldDropIndexProperly()
          throws Exception {
    dropIndex_forColumnTypeWithoutRequiredAlterationForX_ShouldDropIndexProperly(
        RdbEngine.POSTGRESQL,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "DROP INDEX \"my_ns\".\"index_my_ns_my_tbl_my_column\"",
        "UPDATE \""
            + METADATA_SCHEMA
            + "\".\"metadata\" SET \"indexed\"=false WHERE \"full_table_name\"='my_ns.my_tbl' AND \"column_name\"='my_column'");
  }

  @Test
  public void dropIndex_forColumnTypeWithoutRequiredAlterationForServer_ShouldDropIndexProperly()
      throws Exception {
    dropIndex_forColumnTypeWithoutRequiredAlterationForX_ShouldDropIndexProperly(
        RdbEngine.SQL_SERVER,
        "SELECT [column_name],[data_type],[key_type],[clustering_order],[indexed] FROM ["
            + METADATA_SCHEMA
            + "].[metadata] WHERE [full_table_name]=? ORDER BY [ordinal_position] ASC",
        "DROP INDEX [index_my_ns_my_tbl_my_column] ON [my_ns].[my_tbl]",
        "UPDATE ["
            + METADATA_SCHEMA
            + "].[metadata] SET [indexed]=0 WHERE [full_table_name]='my_ns.my_tbl' AND [column_name]='my_column'");
  }

  @Test
  public void dropIndex_forColumnTypeWithoutRequiredAlterationForOracle_ShouldDropIndexProperly()
      throws Exception {
    dropIndex_forColumnTypeWithoutRequiredAlterationForX_ShouldDropIndexProperly(
        RdbEngine.ORACLE,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "DROP INDEX \"index_my_ns_my_tbl_my_column\"",
        "UPDATE \""
            + METADATA_SCHEMA
            + "\".\"metadata\" SET \"indexed\"=0 WHERE \"full_table_name\"='my_ns.my_tbl' AND \"column_name\"='my_column'");
  }

  @Test
  public void dropIndex_forColumnTypeWithoutRequiredAlterationForSqlite_ShouldDropIndexProperly()
      throws Exception {
    dropIndex_forColumnTypeWithoutRequiredAlterationForX_ShouldDropIndexProperly(
        RdbEngine.SQLITE,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + METADATA_SCHEMA
            + "$metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "DROP INDEX \"index_my_ns_my_tbl_my_column\"",
        "UPDATE \""
            + METADATA_SCHEMA
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
    PreparedStatement selectStatement = mock(PreparedStatement.class);
    ResultSet resultSet =
        mockResultSet(
            new SelectAllFromMetadataTableResultSetMocker.Row(
                "c1", DataType.BOOLEAN.toString(), "PARTITION", null, false),
            new SelectAllFromMetadataTableResultSetMocker.Row(
                indexColumn, DataType.BOOLEAN.toString(), null, null, false));
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
            + METADATA_SCHEMA
            + "`.`metadata` WHERE `full_table_name`=? ORDER BY `ordinal_position` ASC",
        "DROP INDEX `index_my_ns_my_tbl_my_column` ON `my_ns`.`my_tbl`",
        "ALTER TABLE `my_ns`.`my_tbl` MODIFY`my_column` LONGTEXT",
        "UPDATE `"
            + METADATA_SCHEMA
            + "`.`metadata` SET `indexed`=false WHERE `full_table_name`='my_ns.my_tbl' AND `column_name`='my_column'");
  }

  @Test
  public void dropIndex_forColumnTypeWithRequiredAlterationForPostgresql_ShouldDropIndexProperly()
      throws Exception {
    dropIndex_forColumnTypeWithRequiredAlterationForX_ShouldDropIndexProperly(
        RdbEngine.POSTGRESQL,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "DROP INDEX \"my_ns\".\"index_my_ns_my_tbl_my_column\"",
        "ALTER TABLE \"my_ns\".\"my_tbl\" ALTER COLUMN\"my_column\" TYPE TEXT",
        "UPDATE \""
            + METADATA_SCHEMA
            + "\".\"metadata\" SET \"indexed\"=false WHERE \"full_table_name\"='my_ns.my_tbl' AND \"column_name\"='my_column'");
  }

  @Test
  public void dropIndex_forColumnTypeWithRequiredAlterationForOracle_ShouldDropIndexProperly()
      throws Exception {
    dropIndex_forColumnTypeWithRequiredAlterationForX_ShouldDropIndexProperly(
        RdbEngine.ORACLE,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "DROP INDEX \"index_my_ns_my_tbl_my_column\"",
        "ALTER TABLE \"my_ns\".\"my_tbl\" MODIFY ( \"my_column\" VARCHAR2(4000) )",
        "UPDATE \""
            + METADATA_SCHEMA
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
    PreparedStatement selectStatement = mock(PreparedStatement.class);
    ResultSet resultSet =
        mockResultSet(
            new SelectAllFromMetadataTableResultSetMocker.Row(
                "c1", DataType.BOOLEAN.toString(), "PARTITION", null, false),
            new SelectAllFromMetadataTableResultSetMocker.Row(
                indexColumn, DataType.TEXT.toString(), null, null, false));
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
  public void addNewColumnToTable_ForMysql_ShouldWorkProperly()
      throws SQLException, ExecutionException {
    addNewColumnToTable_ForX_ShouldWorkProperly(
        RdbEngine.MYSQL,
        "SELECT `column_name`,`data_type`,`key_type`,`clustering_order`,`indexed` FROM `"
            + METADATA_SCHEMA
            + "`.`metadata` WHERE `full_table_name`=? ORDER BY `ordinal_position` ASC",
        "ALTER TABLE `ns`.`table` ADD `c2` INT",
        "DELETE FROM `" + METADATA_SCHEMA + "`.`metadata` WHERE `full_table_name` = 'ns.table'",
        "INSERT INTO `"
            + METADATA_SCHEMA
            + "`.`metadata` VALUES ('ns.table','c1','TEXT','PARTITION',NULL,false,1)",
        "INSERT INTO `"
            + METADATA_SCHEMA
            + "`.`metadata` VALUES ('ns.table','c2','INT',NULL,NULL,false,2)");
  }

  @Test
  public void addNewColumnToTable_ForOracle_ShouldWorkProperly()
      throws SQLException, ExecutionException {
    addNewColumnToTable_ForX_ShouldWorkProperly(
        RdbEngine.ORACLE,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "ALTER TABLE \"ns\".\"table\" ADD \"c2\" NUMBER(10)",
        "DELETE FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\" = 'ns.table'",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('ns.table','c1','TEXT','PARTITION',NULL,0,1)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('ns.table','c2','INT',NULL,NULL,0,2)");
  }

  @Test
  public void addNewColumnToTable_ForPostgresql_ShouldWorkProperly()
      throws SQLException, ExecutionException {
    addNewColumnToTable_ForX_ShouldWorkProperly(
        RdbEngine.POSTGRESQL,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "ALTER TABLE \"ns\".\"table\" ADD \"c2\" INT",
        "DELETE FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\" = 'ns.table'",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('ns.table','c1','TEXT','PARTITION',NULL,false,1)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('ns.table','c2','INT',NULL,NULL,false,2)");
  }

  @Test
  public void addNewColumnToTable_ForSqlServer_ShouldWorkProperly()
      throws SQLException, ExecutionException {
    addNewColumnToTable_ForX_ShouldWorkProperly(
        RdbEngine.SQL_SERVER,
        "SELECT [column_name],[data_type],[key_type],[clustering_order],[indexed] FROM ["
            + METADATA_SCHEMA
            + "].[metadata] WHERE [full_table_name]=? ORDER BY [ordinal_position] ASC",
        "ALTER TABLE [ns].[table] ADD [c2] INT",
        "DELETE FROM [" + METADATA_SCHEMA + "].[metadata] WHERE [full_table_name] = 'ns.table'",
        "INSERT INTO ["
            + METADATA_SCHEMA
            + "].[metadata] VALUES ('ns.table','c1','TEXT','PARTITION',NULL,0,1)",
        "INSERT INTO ["
            + METADATA_SCHEMA
            + "].[metadata] VALUES ('ns.table','c2','INT',NULL,NULL,0,2)");
  }

  @Test
  public void addNewColumnToTable_ForSqlite_ShouldWorkProperly()
      throws SQLException, ExecutionException {
    addNewColumnToTable_ForX_ShouldWorkProperly(
        RdbEngine.SQLITE,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + METADATA_SCHEMA
            + "$metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "ALTER TABLE \"ns$table\" ADD \"c2\" INT",
        "DELETE FROM \"" + METADATA_SCHEMA + "$metadata\" WHERE \"full_table_name\" = 'ns.table'",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "$metadata\" VALUES ('ns.table','c1','TEXT','PARTITION',NULL,FALSE,1)",
        "INSERT INTO \""
            + METADATA_SCHEMA
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

    PreparedStatement selectStatement = mock(PreparedStatement.class);
    ResultSet resultSet =
        mockResultSet(
            new SelectAllFromMetadataTableResultSetMocker.Row(
                currentColumn, DataType.TEXT.toString(), "PARTITION", null, false));
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

  @Test
  public void getNamespaceNames_forMysql_ShouldReturnNamespaceNames() throws Exception {
    getNamespaceNames_forX_ShouldReturnNamespaceNames(
        RdbEngine.MYSQL, "SELECT * FROM `" + METADATA_SCHEMA + "`.`namespaces`");
  }

  @Test
  public void getNamespaceNames_forPostgresql_ShouldReturnNamespaceNames() throws Exception {
    getNamespaceNames_forX_ShouldReturnNamespaceNames(
        RdbEngine.POSTGRESQL, "SELECT * FROM \"" + METADATA_SCHEMA + "\".\"namespaces\"");
  }

  @Test
  public void getNamespaceNames_forSqlServer_ShouldReturnNamespaceNames() throws Exception {
    getNamespaceNames_forX_ShouldReturnNamespaceNames(
        RdbEngine.SQL_SERVER, "SELECT * FROM [" + METADATA_SCHEMA + "].[namespaces]");
  }

  @Test
  public void getNamespaceNames_forOracle_ShouldReturnNamespaceNames() throws Exception {
    getNamespaceNames_forX_ShouldReturnNamespaceNames(
        RdbEngine.ORACLE, "SELECT * FROM \"" + METADATA_SCHEMA + "\".\"namespaces\"");
  }

  @Test
  public void getNamespaceNames_forSqlLite_ShouldReturnNamespaceNames() throws Exception {
    getNamespaceNames_forX_ShouldReturnNamespaceNames(
        RdbEngine.SQLITE, "SELECT * FROM \"" + METADATA_SCHEMA + "$" + "namespaces\"");
  }

  private void getNamespaceNames_forX_ShouldReturnNamespaceNames(
      RdbEngine rdbEngine, String expectedSelectStatement) throws Exception {
    // Arrange
    String namespace1 = "ns1";
    String namespace2 = "ns2";
    ResultSet resultSet =
        mockResultSet(
            new SelectNamespaceNameFromNamespaceTableResultSetMocker.Row(namespace1),
            new SelectNamespaceNameFromNamespaceTableResultSetMocker.Row(namespace2));
    PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(resultSet);

    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    // Act
    Set<String> actualNamespaceNames = admin.getNamespaceNames();

    // Assert
    verify(connection).prepareStatement(expectedSelectStatement);
    verify(mockPreparedStatement).executeQuery();
    assertThat(actualNamespaceNames).containsOnly(namespace1, namespace2);
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
    RdbEngineStrategy rdbEngineStrategy = getRdbEngineStrategy(rdbEngine);
    if (rdbEngineStrategy instanceof RdbEngineMysql) {
      when(metadata.getPrimaryKeys(NAMESPACE, NAMESPACE, TABLE)).thenReturn(primaryKeyResults);
      when(metadata.getColumns(NAMESPACE, NAMESPACE, TABLE, "%")).thenReturn(columnResults);
    } else {
      when(metadata.getPrimaryKeys(null, NAMESPACE, TABLE)).thenReturn(primaryKeyResults);
      when(metadata.getColumns(null, NAMESPACE, TABLE, "%")).thenReturn(columnResults);
    }

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
    RdbEngineStrategy rdbEngineStrategy = getRdbEngineStrategy(rdbEngine);
    if (rdbEngineStrategy instanceof RdbEngineMysql) {
      when(metadata.getPrimaryKeys(NAMESPACE, NAMESPACE, TABLE)).thenReturn(primaryKeyResults);
    } else {
      when(metadata.getPrimaryKeys(null, NAMESPACE, TABLE)).thenReturn(primaryKeyResults);
    }

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

    RdbEngineStrategy rdbEngineStrategy = getRdbEngineStrategy(rdbEngine);
    if (rdbEngineStrategy instanceof RdbEngineMysql) {
      when(metadata.getPrimaryKeys(NAMESPACE, NAMESPACE, TABLE)).thenReturn(primaryKeyResults);
      when(metadata.getColumns(NAMESPACE, NAMESPACE, TABLE, "%")).thenReturn(columnResults);
    } else {
      when(metadata.getPrimaryKeys(null, NAMESPACE, TABLE)).thenReturn(primaryKeyResults);
      when(metadata.getColumns(null, NAMESPACE, TABLE, "%")).thenReturn(columnResults);
    }

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

  @ParameterizedTest
  @EnumSource(
      value = RdbEngine.class,
      mode = Mode.EXCLUDE,
      names = {
        "SQLITE",
      })
  public void importTable_ForXBesidesSqlite_ShouldWorkProperly(RdbEngine rdbEngine)
      throws SQLException, ExecutionException {
    // Arrange
    JdbcAdmin adminSpy = spy(createJdbcAdminFor(rdbEngine));

    when(dataSource.getConnection()).thenReturn(connection);
    TableMetadata importedTableMetadata = mock(TableMetadata.class);
    doReturn(importedTableMetadata).when(adminSpy).getImportTableMetadata(anyString(), anyString());
    doNothing().when(adminSpy).createNamespacesTableIfNotExists(connection);
    doNothing().when(adminSpy).upsertIntoNamespacesTable(any(), anyString());
    doNothing()
        .when(adminSpy)
        .addTableMetadata(any(), anyString(), anyString(), any(), anyBoolean(), anyBoolean());

    // Act
    adminSpy.importTable(NAMESPACE, TABLE, Collections.emptyMap());

    // Assert
    verify(adminSpy).getImportTableMetadata(NAMESPACE, TABLE);
    verify(adminSpy).createNamespacesTableIfNotExists(connection);
    verify(adminSpy).upsertIntoNamespacesTable(connection, NAMESPACE);
    verify(adminSpy)
        .addTableMetadata(connection, NAMESPACE, TABLE, importedTableMetadata, true, false);
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
    String intType;
    if (rdbEngineStrategy instanceof RdbEngineOracle) {
      intType = "NUMBER(10)";
    } else {
      intType = "INT";
    }
    return "ALTER TABLE "
        + rdbEngineStrategy.encloseFullTableName(NAMESPACE, TABLE)
        + " ADD "
        + rdbEngineStrategy.enclose(column)
        + " "
        + intType;
  }

  private RdbEngineStrategy getRdbEngineStrategy(RdbEngine rdbEngine) {
    return RDB_ENGINES.getOrDefault(rdbEngine, new RdbEngineMysql());
  }

  @Test
  public void repairNamespace_forMysql_shouldCreateNamespaceIfNotExistsAndUpsertMetadata()
      throws ExecutionException, SQLException {
    repairNamespace_forX_shouldWorkProperly(
        RdbEngine.MYSQL,
        Collections.singletonList("CREATE SCHEMA IF NOT EXISTS `my_ns`"),
        "SELECT 1 FROM `scalardb`.`namespaces` LIMIT 1",
        Collections.singletonList("CREATE SCHEMA IF NOT EXISTS `" + METADATA_SCHEMA + "`"),
        "CREATE TABLE IF NOT EXISTS `"
            + METADATA_SCHEMA
            + "`.`namespaces`(`namespace_name` VARCHAR(128), PRIMARY KEY (`namespace_name`))",
        "INSERT INTO `" + METADATA_SCHEMA + "`.`namespaces` VALUES (?)");
  }

  @Test
  public void repairNamespace_forPostgresql_shouldCreateNamespaceIfNotExistsAndUpsertMetadata()
      throws ExecutionException, SQLException {
    repairNamespace_forX_shouldWorkProperly(
        RdbEngine.POSTGRESQL,
        Collections.singletonList("CREATE SCHEMA IF NOT EXISTS \"my_ns\""),
        "SELECT 1 FROM \"" + METADATA_SCHEMA + "\".\"namespaces\" LIMIT 1",
        Collections.singletonList("CREATE SCHEMA IF NOT EXISTS \"" + METADATA_SCHEMA + "\""),
        "CREATE TABLE IF NOT EXISTS \""
            + METADATA_SCHEMA
            + "\".\"namespaces\"(\"namespace_name\" VARCHAR(128), PRIMARY KEY (\"namespace_name\"))",
        "INSERT INTO \"" + METADATA_SCHEMA + "\".\"namespaces\" VALUES (?)");
  }

  @Test
  public void repairNamespace_forSqlServer_shouldCreateNamespaceIfNotExistsAndUpsertMetadata()
      throws ExecutionException, SQLException {
    repairNamespace_forX_shouldWorkProperly(
        RdbEngine.SQL_SERVER,
        Collections.singletonList("CREATE SCHEMA [my_ns]"),
        "SELECT TOP 1 1 FROM [" + METADATA_SCHEMA + "].[namespaces]",
        Collections.singletonList("CREATE SCHEMA [" + METADATA_SCHEMA + "]"),
        "CREATE TABLE ["
            + METADATA_SCHEMA
            + "].[namespaces]([namespace_name] VARCHAR(128), PRIMARY KEY ([namespace_name]))",
        "INSERT INTO [" + METADATA_SCHEMA + "].[namespaces] VALUES (?)");
  }

  @Test
  public void repairNamespace_forOracle_shouldCreateNamespaceIfNotExistsAndUpsertMetadata()
      throws ExecutionException, SQLException {
    repairNamespace_forX_shouldWorkProperly(
        RdbEngine.ORACLE,
        Arrays.asList(
            "CREATE USER \"my_ns\" IDENTIFIED BY \"Oracle1234!@#$\"",
            "ALTER USER \"my_ns\" quota unlimited on USERS"),
        "SELECT 1 FROM \"" + METADATA_SCHEMA + "\".\"namespaces\" FETCH FIRST 1 ROWS ONLY",
        Arrays.asList(
            "CREATE USER \"" + METADATA_SCHEMA + "\" IDENTIFIED BY \"Oracle1234!@#$\"",
            "ALTER USER \"" + METADATA_SCHEMA + "\" quota unlimited on USERS"),
        "CREATE TABLE \""
            + METADATA_SCHEMA
            + "\".\"namespaces\"(\"namespace_name\" VARCHAR2(128), PRIMARY KEY (\"namespace_name\"))",
        "INSERT INTO \"" + METADATA_SCHEMA + "\".\"namespaces\" VALUES (?)");
  }

  @Test
  public void repairNamespace_forSqlite_shouldUpsertNamespaceMetadata()
      throws SQLException, ExecutionException {
    repairNamespace_forX_shouldWorkProperly(
        RdbEngine.SQLITE,
        Collections.emptyList(),
        "SELECT 1 FROM \"" + METADATA_SCHEMA + "$namespaces\" LIMIT 1",
        Collections.emptyList(),
        "CREATE TABLE IF NOT EXISTS \""
            + METADATA_SCHEMA
            + "$namespaces\"(\"namespace_name\" TEXT, PRIMARY KEY (\"namespace_name\"))",
        "INSERT INTO \"" + METADATA_SCHEMA + "$namespaces\" VALUES (?)");
  }

  private void repairNamespace_forX_shouldWorkProperly(
      RdbEngine rdbEngine,
      List<String> createSchemaIfNotExistsSqls,
      String namespacesTableExistsSql,
      List<String> createMetadataSchemaSqls,
      String createNamespacesTableSql,
      String insertNamespaceSql)
      throws SQLException, ExecutionException {
    // Arrange
    String namespace = "my_ns";
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    List<Statement> mockedCreateSchemaIfNotExistsStatements = new ArrayList<>();
    for (int i = 0; i < createSchemaIfNotExistsSqls.size(); i++) {
      mockedCreateSchemaIfNotExistsStatements.add(mock(Statement.class));
    }

    Statement mockedNamespacesTableExistsStatement = mock(Statement.class);
    SQLException sqlException = mock(SQLException.class);
    mockUndefinedTableError(rdbEngine, sqlException);
    when(mockedNamespacesTableExistsStatement.execute(namespacesTableExistsSql))
        .thenThrow(sqlException);

    List<Statement> mockedCreateMetadataSchemaStatements = new ArrayList<>();
    for (int i = 0; i < createMetadataSchemaSqls.size(); i++) {
      mockedCreateMetadataSchemaStatements.add(mock(Statement.class));
    }

    Statement mockedCreateNamespacesTableStatement = mock(Statement.class);

    List<Statement> statementsMock =
        ImmutableList.<Statement>builder()
            .addAll(mockedCreateSchemaIfNotExistsStatements)
            .add(mockedNamespacesTableExistsStatement)
            .addAll(mockedCreateMetadataSchemaStatements)
            .add(mockedCreateNamespacesTableStatement)
            .build();

    when(connection.createStatement())
        .thenReturn(
            statementsMock.get(0),
            statementsMock.subList(1, statementsMock.size()).toArray(new Statement[0]));

    PreparedStatement mockedInsertNamespaceStatement1 = mock(PreparedStatement.class);
    PreparedStatement mockedInsertNamespaceStatement2 = mock(PreparedStatement.class);
    when(connection.prepareStatement(insertNamespaceSql))
        .thenReturn(mockedInsertNamespaceStatement1, mockedInsertNamespaceStatement2);
    when(dataSource.getConnection()).thenReturn(connection);

    // Act
    admin.repairNamespace(namespace, Collections.emptyMap());

    // Assert
    for (int i = 0; i < createSchemaIfNotExistsSqls.size(); i++) {
      verify(mockedCreateSchemaIfNotExistsStatements.get(i))
          .execute(createSchemaIfNotExistsSqls.get(i));
    }
    verify(mockedNamespacesTableExistsStatement).execute(namespacesTableExistsSql);
    for (int i = 0; i < createMetadataSchemaSqls.size(); i++) {
      verify(mockedCreateMetadataSchemaStatements.get(i)).execute(createMetadataSchemaSqls.get(i));
    }
    verify(mockedCreateNamespacesTableStatement).execute(createNamespacesTableSql);
    verify(mockedInsertNamespaceStatement1).setString(1, METADATA_SCHEMA);
    verify(mockedInsertNamespaceStatement2).setString(1, namespace);
    verify(mockedInsertNamespaceStatement1).execute();
    verify(mockedInsertNamespaceStatement2).execute();
  }

  @Test
  public void upgrade_ForMysql_ShouldInsertAllNamespacesFromMetadataTable()
      throws SQLException, ExecutionException {
    upgrade_ForX_ShouldInsertAllNamespacesFromMetadataTable(
        RdbEngine.MYSQL,
        "SELECT 1 FROM `" + METADATA_SCHEMA + "`.`metadata` LIMIT 1",
        "SELECT 1 FROM `" + METADATA_SCHEMA + "`.`namespaces` LIMIT 1",
        ImmutableList.of("CREATE SCHEMA IF NOT EXISTS `" + METADATA_SCHEMA + "`"),
        "CREATE TABLE IF NOT EXISTS `"
            + METADATA_SCHEMA
            + "`.`namespaces`(`namespace_name` VARCHAR(128), PRIMARY KEY (`namespace_name`))",
        "SELECT DISTINCT `full_table_name` FROM `" + METADATA_SCHEMA + "`.`metadata`",
        "INSERT INTO `" + METADATA_SCHEMA + "`.`namespaces` VALUES (?)");
  }

  @Test
  public void upgrade_ForPosgresql_ShouldInsertAllNamespacesFromMetadataTable()
      throws SQLException, ExecutionException {
    upgrade_ForX_ShouldInsertAllNamespacesFromMetadataTable(
        RdbEngine.POSTGRESQL,
        "SELECT 1 FROM \"" + METADATA_SCHEMA + "\".\"metadata\" LIMIT 1",
        "SELECT 1 FROM \"" + METADATA_SCHEMA + "\".\"namespaces\" LIMIT 1",
        ImmutableList.of("CREATE SCHEMA IF NOT EXISTS \"" + METADATA_SCHEMA + "\""),
        "CREATE TABLE IF NOT EXISTS \""
            + METADATA_SCHEMA
            + "\".\"namespaces\"(\"namespace_name\" VARCHAR(128), PRIMARY KEY (\"namespace_name\"))",
        "SELECT DISTINCT \"full_table_name\" FROM \"" + METADATA_SCHEMA + "\".\"metadata\"",
        "INSERT INTO \"" + METADATA_SCHEMA + "\".\"namespaces\" VALUES (?)");
  }

  @Test
  public void upgrade_ForOracle_ShouldInsertAllNamespacesFromMetadataTable()
      throws SQLException, ExecutionException {
    upgrade_ForX_ShouldInsertAllNamespacesFromMetadataTable(
        RdbEngine.ORACLE,
        "SELECT 1 FROM \"" + METADATA_SCHEMA + "\".\"metadata\" FETCH FIRST 1 ROWS ONLY",
        "SELECT 1 FROM \"" + METADATA_SCHEMA + "\".\"namespaces\" FETCH FIRST 1 ROWS ONLY",
        ImmutableList.of(
            "CREATE USER \"" + METADATA_SCHEMA + "\" IDENTIFIED BY \"Oracle1234!@#$\"",
            "ALTER USER \"" + METADATA_SCHEMA + "\" quota unlimited on USERS"),
        "CREATE TABLE \""
            + METADATA_SCHEMA
            + "\".\"namespaces\"(\"namespace_name\" VARCHAR2(128), PRIMARY KEY (\"namespace_name\"))",
        "SELECT DISTINCT \"full_table_name\" FROM \"" + METADATA_SCHEMA + "\".\"metadata\"",
        "INSERT INTO \"" + METADATA_SCHEMA + "\".\"namespaces\" VALUES (?)");
  }

  @Test
  public void upgrade_ForSqlServer_ShouldInsertAllNamespacesFromMetadataTable()
      throws SQLException, ExecutionException {
    upgrade_ForX_ShouldInsertAllNamespacesFromMetadataTable(
        RdbEngine.SQL_SERVER,
        "SELECT TOP 1 1 FROM [" + METADATA_SCHEMA + "].[metadata]",
        "SELECT TOP 1 1 FROM [" + METADATA_SCHEMA + "].[namespaces]",
        ImmutableList.of("CREATE SCHEMA [" + METADATA_SCHEMA + "]"),
        "CREATE TABLE ["
            + METADATA_SCHEMA
            + "].[namespaces]([namespace_name] VARCHAR(128), PRIMARY KEY ([namespace_name]))",
        "SELECT DISTINCT [full_table_name] FROM [" + METADATA_SCHEMA + "].[metadata]",
        "INSERT INTO [" + METADATA_SCHEMA + "].[namespaces] VALUES (?)");
  }

  @Test
  public void upgrade_ForSqlite_ShouldInsertAllNamespacesFromMetadataTable()
      throws SQLException, ExecutionException {
    upgrade_ForX_ShouldInsertAllNamespacesFromMetadataTable(
        RdbEngine.SQLITE,
        "SELECT 1 FROM \"" + METADATA_SCHEMA + "$metadata\" LIMIT 1",
        "SELECT 1 FROM \"" + METADATA_SCHEMA + "$namespaces\" LIMIT 1",
        Collections.emptyList(),
        "CREATE TABLE IF NOT EXISTS \""
            + METADATA_SCHEMA
            + "$namespaces\"(\"namespace_name\" TEXT, PRIMARY KEY (\"namespace_name\"))",
        "SELECT DISTINCT \"full_table_name\" FROM \"" + METADATA_SCHEMA + "$metadata\"",
        "INSERT INTO \"" + METADATA_SCHEMA + "$namespaces\" VALUES (?)");
  }

  private void upgrade_ForX_ShouldInsertAllNamespacesFromMetadataTable(
      RdbEngine rdbEngine,
      String tableMetadataExistStatement,
      String namespacesTableExistsStatement,
      List<String> createMetadataNamespaceStatements,
      String createNamespaceTableStatement,
      String getTableMetadataNamespacesStatement,
      String insertNamespaceStatement)
      throws SQLException, ExecutionException {
    // Arrange
    // Instantiate mocks
    Statement tableMetadataExistsStatementMock = mock(Statement.class);
    Statement namespacesTableExistsStatementMock = mock(Statement.class);
    List<Statement> createMetadataNamespaceStatementsMock =
        prepareMockStatements(createMetadataNamespaceStatements.size());
    Statement createNamespaceTableStatementMock = mock(Statement.class);
    Statement getTableMetadataNamespacesStatementMock = mock(Statement.class);
    PreparedStatement insertNamespacePrepStmt1 = mock(PreparedStatement.class);
    PreparedStatement insertNamespacePrepStmt2 = mock(PreparedStatement.class);
    PreparedStatement insertNamespacePrepStmt3 = mock(PreparedStatement.class);

    when(connection.prepareStatement(anyString()))
        .thenReturn(insertNamespacePrepStmt1, insertNamespacePrepStmt2, insertNamespacePrepStmt3);
    List<Statement> statementsMock =
        ImmutableList.<Statement>builder()
            .add(tableMetadataExistsStatementMock)
            .add(namespacesTableExistsStatementMock)
            .addAll(createMetadataNamespaceStatementsMock)
            .add(createNamespaceTableStatementMock)
            .add(getTableMetadataNamespacesStatementMock)
            .build();

    // Prepare calls
    when(connection.createStatement())
        .thenReturn(
            statementsMock.get(0),
            statementsMock.subList(1, statementsMock.size()).toArray(new Statement[0]));
    Connection connection2 = mock(Connection.class);
    when(dataSource.getConnection()).thenReturn(connection, connection2);

    SQLException sqlException = mock(SQLException.class);
    mockUndefinedTableError(rdbEngine, sqlException);
    when(namespacesTableExistsStatementMock.execute(namespacesTableExistsStatement))
        .thenThrow(sqlException);

    ResultSet resultSet1 =
        mockResultSet(
            new SelectFullTableNameFromMetadataTableResultSetMocker.Row("ns1.tbl1"),
            new SelectFullTableNameFromMetadataTableResultSetMocker.Row("ns1.tbl2"),
            new SelectFullTableNameFromMetadataTableResultSetMocker.Row("ns2.tbl3"));
    when(getTableMetadataNamespacesStatementMock.executeQuery(anyString())).thenReturn(resultSet1);
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    // Act
    admin.upgrade(Collections.emptyMap());

    // Assert
    verify(tableMetadataExistsStatementMock).execute(tableMetadataExistStatement);
    verify(namespacesTableExistsStatementMock).execute(namespacesTableExistsStatement);
    for (int i = 0; i < createMetadataNamespaceStatementsMock.size(); i++) {
      verify(createMetadataNamespaceStatementsMock.get(i))
          .execute(createMetadataNamespaceStatements.get(i));
    }
    verify(createNamespaceTableStatementMock).execute(createNamespaceTableStatement);
    verify(getTableMetadataNamespacesStatementMock)
        .executeQuery(getTableMetadataNamespacesStatement);
    verify(connection, times(3)).prepareStatement(insertNamespaceStatement);
    verify(insertNamespacePrepStmt1).setString(1, METADATA_SCHEMA);
    verify(insertNamespacePrepStmt2).setString(1, "ns2");
    verify(insertNamespacePrepStmt3).setString(1, "ns1");
    verify(insertNamespacePrepStmt1).execute();
    verify(insertNamespacePrepStmt2).execute();
    verify(insertNamespacePrepStmt3).execute();
  }

  private List<Statement> prepareMockStatements(int count) {
    List<Statement> statements = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      Statement statement = mock(Statement.class);
      statements.add(statement);
    }
    return statements;
  }

  @ParameterizedTest
  @EnumSource(value = RdbEngine.class)
  public void upsertIntoNamespacesTable_ForNonExistingNamespace_ShouldInsertNamespacesMetadata(
      RdbEngine rdbEngine) throws SQLException {
    // Arrange
    PreparedStatement insertStatement = mock(PreparedStatement.class);
    when(connection.prepareStatement(anyString())).thenReturn(insertStatement);
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    // Act
    admin.upsertIntoNamespacesTable(connection, NAMESPACE);

    // Assert
    RdbEngineStrategy rdbEngineStrategy = getRdbEngineStrategy(rdbEngine);
    verify(connection)
        .prepareStatement(
            "INSERT INTO "
                + rdbEngineStrategy.encloseFullTableName(
                    METADATA_SCHEMA, JdbcAdmin.NAMESPACES_TABLE)
                + " VALUES (?)");
    verify(insertStatement).setString(1, NAMESPACE);
    verify(insertStatement).execute();
  }

  @ParameterizedTest
  @EnumSource(value = RdbEngine.class)
  public void upsertIntoNamespacesTable_ForExistingNamespace_ShouldNotThrowException(
      RdbEngine rdbEngine) throws SQLException {
    // Arrange
    PreparedStatement insertStatement = mock(PreparedStatement.class);
    when(connection.prepareStatement(anyString())).thenReturn(insertStatement);
    doThrow(prepareDuplicatedKeyException(rdbEngine)).when(insertStatement).execute();
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    // Act
    admin.upsertIntoNamespacesTable(connection, NAMESPACE);

    // Assert
    RdbEngineStrategy rdbEngineStrategy = getRdbEngineStrategy(rdbEngine);
    verify(connection)
        .prepareStatement(
            "INSERT INTO "
                + rdbEngineStrategy.encloseFullTableName(
                    METADATA_SCHEMA, JdbcAdmin.NAMESPACES_TABLE)
                + " VALUES (?)");
    verify(insertStatement).setString(1, NAMESPACE);
    verify(insertStatement).execute();
  }

  @Test
  public void
      createNamespaceTableIfNotExists_forMysql_shouldCreateMetadataSchemaAndNamespacesTableIfNotExists()
          throws ExecutionException, SQLException {
    createNamespaceTableIfNotExists_forX_shouldCreateMetadataSchemaAndNamespacesTableIfNotExists(
        RdbEngine.MYSQL,
        "SELECT 1 FROM `" + METADATA_SCHEMA + "`.`namespaces` LIMIT 1",
        Collections.singletonList("CREATE SCHEMA IF NOT EXISTS `" + METADATA_SCHEMA + "`"),
        "CREATE TABLE IF NOT EXISTS `"
            + METADATA_SCHEMA
            + "`.`namespaces`(`namespace_name` VARCHAR(128), PRIMARY KEY (`namespace_name`))",
        "INSERT INTO `" + METADATA_SCHEMA + "`.`namespaces` VALUES (?)");
  }

  @Test
  public void
      createNamespaceTableIfNotExists_forPosgresql_shouldCreateMetadataSchemaAndNamespacesTableIfNotExists()
          throws ExecutionException, SQLException {
    createNamespaceTableIfNotExists_forX_shouldCreateMetadataSchemaAndNamespacesTableIfNotExists(
        RdbEngine.POSTGRESQL,
        "SELECT 1 FROM \"" + METADATA_SCHEMA + "\".\"namespaces\" LIMIT 1",
        Collections.singletonList("CREATE SCHEMA IF NOT EXISTS \"" + METADATA_SCHEMA + "\""),
        "CREATE TABLE IF NOT EXISTS \""
            + METADATA_SCHEMA
            + "\".\"namespaces\"(\"namespace_name\" VARCHAR(128), PRIMARY KEY (\"namespace_name\"))",
        "INSERT INTO \"" + METADATA_SCHEMA + "\".\"namespaces\" VALUES (?)");
  }

  @Test
  public void
      createNamespaceTableIfNotExists_forSqlServer_shouldCreateMetadataSchemaAndNamespacesTableIfNotExists()
          throws ExecutionException, SQLException {
    createNamespaceTableIfNotExists_forX_shouldCreateMetadataSchemaAndNamespacesTableIfNotExists(
        RdbEngine.SQL_SERVER,
        "SELECT TOP 1 1 FROM [" + METADATA_SCHEMA + "].[namespaces]",
        Collections.singletonList("CREATE SCHEMA [" + METADATA_SCHEMA + "]"),
        "CREATE TABLE ["
            + METADATA_SCHEMA
            + "].[namespaces]([namespace_name] VARCHAR(128), PRIMARY KEY ([namespace_name]))",
        "INSERT INTO [" + METADATA_SCHEMA + "].[namespaces] VALUES (?)");
  }

  @Test
  public void
      createNamespaceTableIfNotExists_forOracle_shouldCreateMetadataSchemaAndNamespacesTableIfNotExists()
          throws ExecutionException, SQLException {
    createNamespaceTableIfNotExists_forX_shouldCreateMetadataSchemaAndNamespacesTableIfNotExists(
        RdbEngine.ORACLE,
        "SELECT 1 FROM \"" + METADATA_SCHEMA + "\".\"namespaces\" FETCH FIRST 1 ROWS ONLY",
        Arrays.asList(
            "CREATE USER \"" + METADATA_SCHEMA + "\" IDENTIFIED BY \"Oracle1234!@#$\"",
            "ALTER USER \"" + METADATA_SCHEMA + "\" quota unlimited on USERS"),
        "CREATE TABLE \""
            + METADATA_SCHEMA
            + "\".\"namespaces\"(\"namespace_name\" VARCHAR2(128), PRIMARY KEY (\"namespace_name\"))",
        "INSERT INTO \"" + METADATA_SCHEMA + "\".\"namespaces\" VALUES (?)");
  }

  @Test
  public void
      createNamespaceTableIfNotExists_forSqlite_shouldCreateMetadataSchemaAndNamespacesTableIfNotExists()
          throws ExecutionException, SQLException {
    createNamespaceTableIfNotExists_forX_shouldCreateMetadataSchemaAndNamespacesTableIfNotExists(
        RdbEngine.SQLITE,
        "SELECT 1 FROM \"" + METADATA_SCHEMA + "$namespaces\" LIMIT 1",
        Collections.emptyList(),
        "CREATE TABLE IF NOT EXISTS \""
            + METADATA_SCHEMA
            + "$namespaces\"(\"namespace_name\" TEXT, PRIMARY KEY (\"namespace_name\"))",
        "INSERT INTO \"" + METADATA_SCHEMA + "$namespaces\" VALUES (?)");
  }

  public void
      createNamespaceTableIfNotExists_forX_shouldCreateMetadataSchemaAndNamespacesTableIfNotExists(
          RdbEngine rdbEngine,
          String namespacesTableExistsSql,
          List<String> createMetadataSchemaSqls,
          String createNamespacesTableSql,
          String insertNamespaceSql)
          throws SQLException, ExecutionException {
    // Arrange
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    Statement mockedNamespacesTableExistsStatement = mock(Statement.class);
    SQLException sqlException = mock(SQLException.class);
    mockUndefinedTableError(rdbEngine, sqlException);
    when(mockedNamespacesTableExistsStatement.execute(namespacesTableExistsSql))
        .thenThrow(sqlException);

    List<Statement> mockedCreateMetadataSchemaStatements = new ArrayList<>();
    for (int i = 0; i < createMetadataSchemaSqls.size(); i++) {
      mockedCreateMetadataSchemaStatements.add(mock(Statement.class));
    }

    Statement mockedCreateNamespacesTableStatement = mock(Statement.class);

    if (!mockedCreateMetadataSchemaStatements.isEmpty()) {
      when(connection.createStatement())
          .thenReturn(mockedNamespacesTableExistsStatement)
          .thenReturn(
              mockedCreateMetadataSchemaStatements.get(0),
              mockedCreateMetadataSchemaStatements
                  .subList(1, mockedCreateMetadataSchemaStatements.size())
                  .toArray(new Statement[0]))
          .thenReturn(mockedCreateNamespacesTableStatement);
    } else {
      when(connection.createStatement())
          .thenReturn(mockedNamespacesTableExistsStatement)
          .thenReturn(mockedCreateNamespacesTableStatement);
    }

    List<Statement> statementsMock =
        ImmutableList.<Statement>builder()
            .add(mockedNamespacesTableExistsStatement)
            .addAll(mockedCreateMetadataSchemaStatements)
            .add(mockedCreateNamespacesTableStatement)
            .build();
    when(connection.createStatement())
        .thenReturn(
            statementsMock.get(0),
            statementsMock.subList(1, statementsMock.size()).toArray(new Statement[0]));

    PreparedStatement mockedInsertNamespaceStatement = mock(PreparedStatement.class);
    when(connection.prepareStatement(insertNamespaceSql))
        .thenReturn(mockedInsertNamespaceStatement);

    // Act
    admin.createNamespacesTableIfNotExists(connection);

    // Assert
    verify(mockedNamespacesTableExistsStatement).execute(namespacesTableExistsSql);
    for (int i = 0; i < createMetadataSchemaSqls.size(); i++) {
      verify(mockedCreateMetadataSchemaStatements.get(i)).execute(createMetadataSchemaSqls.get(i));
    }
    verify(mockedCreateNamespacesTableStatement).execute(createNamespacesTableSql);
    verify(mockedInsertNamespaceStatement).setString(1, METADATA_SCHEMA);
    verify(mockedInsertNamespaceStatement).execute();
  }

  private SQLException prepareDuplicatedKeyException(RdbEngine rdbEngine) {
    SQLException duplicateKeyException;
    switch (rdbEngine) {
      case SQL_SERVER:
      case MYSQL:
      case ORACLE:
      case MARIADB:
        duplicateKeyException = mock(SQLException.class);
        when(duplicateKeyException.getSQLState()).thenReturn("23000");
        break;
      case POSTGRESQL:
      case YUGABYTE:
        duplicateKeyException = mock(SQLException.class);
        when(duplicateKeyException.getSQLState()).thenReturn("23505");
        break;
      case SQLITE:
        SQLiteException sqLiteException = mock(SQLiteException.class);
        when(sqLiteException.getResultCode())
            .thenReturn(SQLiteErrorCode.SQLITE_CONSTRAINT_PRIMARYKEY);
        duplicateKeyException = sqLiteException;
        break;
      default:
        throw new AssertionError("Unsupported rdbEngine " + rdbEngine);
    }
    return duplicateKeyException;
  }

  @ParameterizedTest
  @EnumSource(value = RdbEngine.class)
  public void upsertIntoNamespacesTable_ForNonDuplicatedKeyException_ShouldThrowSqlException(
      RdbEngine rdbEngine) throws SQLException {
    // Arrange
    PreparedStatement insertStatement = mock(PreparedStatement.class);
    when(connection.prepareStatement(anyString())).thenReturn(insertStatement);
    SQLException anySqlException;
    if (rdbEngine == RdbEngine.SQLITE) {
      SQLiteException sqLiteException = mock(SQLiteException.class);
      when(sqLiteException.getResultCode()).thenReturn(SQLiteErrorCode.SQLITE_IOERR);
      anySqlException = sqLiteException;
    } else {
      anySqlException = mock(SQLException.class);
      when(anySqlException.getSQLState()).thenReturn("foo");
    }
    doThrow(anySqlException).when(insertStatement).execute();
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    // Act
    assertThatThrownBy(() -> admin.upsertIntoNamespacesTable(connection, NAMESPACE))
        .isEqualTo(anySqlException);

    // Assert
    RdbEngineStrategy rdbEngineStrategy = getRdbEngineStrategy(rdbEngine);
    verify(connection)
        .prepareStatement(
            "INSERT INTO "
                + rdbEngineStrategy.encloseFullTableName(
                    METADATA_SCHEMA, JdbcAdmin.NAMESPACES_TABLE)
                + " VALUES (?)");
    verify(insertStatement).setString(1, NAMESPACE);
    verify(insertStatement).execute();
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

  // Utility class used to mock ResultSet for a "select * from" query on the metadata table
  static class SelectAllFromMetadataTableResultSetMocker
      implements org.mockito.stubbing.Answer<Object> {

    final List<SelectAllFromMetadataTableResultSetMocker.Row> rows;
    int row = -1;

    public SelectAllFromMetadataTableResultSetMocker(
        List<SelectAllFromMetadataTableResultSetMocker.Row> rows) {
      this.rows = rows;
    }

    @Override
    public Object answer(InvocationOnMock invocation) throws Throwable {
      row++;
      if (row >= rows.size()) {
        return false;
      }
      SelectAllFromMetadataTableResultSetMocker.Row currentRow = rows.get(row);
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

  // Utility class used to mock ResultSet for a "select full_table_name from" query on the metadata
  // table
  static class SelectFullTableNameFromMetadataTableResultSetMocker
      implements org.mockito.stubbing.Answer<Object> {

    final List<SelectFullTableNameFromMetadataTableResultSetMocker.Row> rows;
    int row = -1;

    public SelectFullTableNameFromMetadataTableResultSetMocker(
        List<SelectFullTableNameFromMetadataTableResultSetMocker.Row> rows) {
      this.rows = rows;
    }

    @Override
    public Object answer(InvocationOnMock invocation) throws Throwable {
      row++;
      if (row >= rows.size()) {
        return false;
      }
      SelectFullTableNameFromMetadataTableResultSetMocker.Row currentRow = rows.get(row);
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

  // Utility class used to mock ResultSet for a "select namespace_name from" query on the namespace
  // table
  static class SelectNamespaceNameFromNamespaceTableResultSetMocker
      implements org.mockito.stubbing.Answer<Object> {

    final List<SelectNamespaceNameFromNamespaceTableResultSetMocker.Row> rows;
    int row = -1;

    public SelectNamespaceNameFromNamespaceTableResultSetMocker(
        List<SelectNamespaceNameFromNamespaceTableResultSetMocker.Row> rows) {
      this.rows = rows;
    }

    @Override
    public Object answer(InvocationOnMock invocation) throws Throwable {
      row++;
      if (row >= rows.size()) {
        return false;
      }
      SelectNamespaceNameFromNamespaceTableResultSetMocker.Row currentRow = rows.get(row);
      ResultSet mock = (ResultSet) invocation.getMock();
      when(mock.getString(JdbcAdmin.NAMESPACE_COL_NAMESPACE_NAME))
          .thenReturn(currentRow.namespaceName);
      return true;
    }

    static class Row {

      final String namespaceName;

      public Row(String namespaceName) {
        this.namespaceName = namespaceName;
      }
    }
  }
}
