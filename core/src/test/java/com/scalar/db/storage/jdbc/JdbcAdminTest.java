package com.scalar.db.storage.jdbc;

import static com.scalar.db.storage.jdbc.JdbcAdmin.JDBC_COL_COLUMN_NAME;
import static com.scalar.db.storage.jdbc.JdbcAdmin.JDBC_COL_COLUMN_SIZE;
import static com.scalar.db.storage.jdbc.JdbcAdmin.JDBC_COL_DATA_TYPE;
import static com.scalar.db.storage.jdbc.JdbcAdmin.JDBC_COL_DECIMAL_DIGITS;
import static com.scalar.db.storage.jdbc.JdbcAdmin.JDBC_COL_TYPE_NAME;
import static com.scalar.db.storage.jdbc.JdbcAdmin.hasDifferentClusteringOrders;
import static com.scalar.db.util.ScalarDbUtils.getFullTableName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.description;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.mysql.cj.jdbc.exceptions.CommunicationsException;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.StorageInfo;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.VirtualTableInfo;
import com.scalar.db.api.VirtualTableJoinType;
import com.scalar.db.common.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
          new RdbEngineMariaDB(),
          RdbEngine.DB2,
          new RdbEngineDb2());

  @Mock private BasicDataSource dataSource;
  @Mock private Connection connection;
  @Mock private JdbcConfig config;
  @Mock private TableMetadataService tableMetadataService;
  @Mock private VirtualTableMetadataService virtualTableMetadataService;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    when(config.getTableMetadataSchema()).thenReturn(Optional.of(METADATA_SCHEMA));
  }

  private JdbcAdmin createJdbcAdminFor(RdbEngine rdbEngine) {
    // Arrange
    RdbEngineStrategy st = RdbEngine.createRdbEngineStrategy(rdbEngine);
    try (MockedStatic<RdbEngineFactory> mocked = mockStatic(RdbEngineFactory.class)) {
      mocked.when(() -> RdbEngineFactory.create(any(JdbcConfig.class))).thenReturn(st);
      return new JdbcAdmin(dataSource, config, virtualTableMetadataService);
    }
  }

  private JdbcAdmin createJdbcAdminFor(RdbEngineStrategy rdbEngineStrategy) {
    // Arrange
    try (MockedStatic<RdbEngineFactory> mocked = mockStatic(RdbEngineFactory.class)) {
      mocked
          .when(() -> RdbEngineFactory.create(any(JdbcConfig.class)))
          .thenReturn(rdbEngineStrategy);
      return new JdbcAdmin(dataSource, config, virtualTableMetadataService);
    }
  }

  private JdbcAdmin createJdbcAdmin() {
    // Arrange
    RdbEngineStrategy st = RdbEngine.createRdbEngineStrategy(RdbEngine.MYSQL);
    try (MockedStatic<RdbEngineFactory> mocked = mockStatic(RdbEngineFactory.class)) {
      mocked.when(() -> RdbEngineFactory.create(any(JdbcConfig.class))).thenReturn(st);
      return new JdbcAdmin(dataSource, config, tableMetadataService, virtualTableMetadataService);
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
      case DB2:
        when(sqlException.getErrorCode()).thenReturn(-204);
        break;
      default:
        throw new AssertionError("Unsupported rdbEngine " + rdbEngine);
    }
  }

  private ResultSet mockResultSet(SelectAllFromMetadataTableResultSetMocker.Row... rows)
      throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);
    // Everytime the ResultSet.next() method will be called, the ResultSet.getXXX methods call be
    // mocked to return the current row data
    doAnswer(new SelectAllFromMetadataTableResultSetMocker(Arrays.asList(rows)))
        .when(resultSet)
        .next();
    return resultSet;
  }

  private ResultSet mockResultSet(SelectFullTableNameFromMetadataTableResultSetMocker.Row... rows)
      throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);
    // Everytime the ResultSet.next() method will be called, the ResultSet.getXXX methods call be
    // mocked to return the current row data
    doAnswer(new SelectFullTableNameFromMetadataTableResultSetMocker(Arrays.asList(rows)))
        .when(resultSet)
        .next();
    return resultSet;
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

  @Test
  public void getTableMetadata_forDb2_ShouldReturnTableMetadata()
      throws SQLException, ExecutionException {
    getTableMetadata_forX_ShouldReturnTableMetadata(
        RdbEngine.DB2,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + METADATA_SCHEMA
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
                "c7", DataType.FLOAT.toString(), null, null, false),
            new SelectAllFromMetadataTableResultSetMocker.Row(
                "c8", DataType.DATE.toString(), null, null, false),
            new SelectAllFromMetadataTableResultSetMocker.Row(
                "c9", DataType.TIME.toString(), null, null, false),
            new SelectAllFromMetadataTableResultSetMocker.Row(
                "c10", DataType.TIMESTAMP.toString(), null, null, false),
            new SelectAllFromMetadataTableResultSetMocker.Row(
                "c11", DataType.TIMESTAMPTZ.toString(), null, null, false));
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
            .addColumn("c8", DataType.DATE)
            .addColumn("c9", DataType.TIME)
            .addColumn("c10", DataType.TIMESTAMP)
            .addColumn("c11", DataType.TIMESTAMPTZ)
            .addSecondaryIndex("c4")
            .build();
    assertThat(actualMetadata).isEqualTo(expectedMetadata);
    if (rdbEngine == RdbEngine.MYSQL || rdbEngine == RdbEngine.SQLITE) {
      verify(connection, never()).setReadOnly(anyBoolean());
    } else {
      verify(connection).setReadOnly(true);
    }
    verify(connection).prepareStatement(expectedSelectStatements);
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
  public void getTableMetadata_VirtualTableExists_ShouldReturnMergedTableMetadata()
      throws Exception {
    // Arrange
    String namespace = "ns";
    String table = "vtable";
    String leftSourceNamespace = "ns";
    String leftSourceTable = "left_table";
    String rightSourceNamespace = "ns";
    String rightSourceTable = "right_table";

    when(dataSource.getConnection()).thenReturn(connection);

    // Mock tableMetadataService to return null (not a regular table)
    when(tableMetadataService.getTableMetadata(any(Connection.class), eq(namespace), eq(table)))
        .thenReturn(null);

    // Mock virtualTableMetadataService to return virtual table info
    VirtualTableInfo virtualTableInfo = mock(VirtualTableInfo.class);
    when(virtualTableInfo.getNamespaceName()).thenReturn(namespace);
    when(virtualTableInfo.getTableName()).thenReturn(table);
    when(virtualTableInfo.getLeftSourceNamespaceName()).thenReturn(leftSourceNamespace);
    when(virtualTableInfo.getLeftSourceTableName()).thenReturn(leftSourceTable);
    when(virtualTableInfo.getRightSourceNamespaceName()).thenReturn(rightSourceNamespace);
    when(virtualTableInfo.getRightSourceTableName()).thenReturn(rightSourceTable);
    when(virtualTableInfo.getJoinType()).thenReturn(VirtualTableJoinType.INNER);

    when(virtualTableMetadataService.getVirtualTableInfo(
            any(Connection.class), eq(namespace), eq(table)))
        .thenReturn(virtualTableInfo);

    // Mock source table metadata
    TableMetadata leftSourceTableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addPartitionKey("pk2")
            .addClusteringKey("ck1", Order.ASC)
            .addClusteringKey("ck2", Order.DESC)
            .addColumn("pk1", DataType.INT)
            .addColumn("pk2", DataType.TEXT)
            .addColumn("ck1", DataType.BIGINT)
            .addColumn("ck2", DataType.TEXT)
            .addColumn("col1", DataType.BOOLEAN)
            .addColumn("col2", DataType.DOUBLE)
            .addColumn("col3", DataType.BLOB)
            .addSecondaryIndex("ck1")
            .addSecondaryIndex("col1")
            .addSecondaryIndex("col2")
            .build();

    TableMetadata rightSourceTableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addPartitionKey("pk2")
            .addClusteringKey("ck1", Order.ASC)
            .addClusteringKey("ck2", Order.DESC)
            .addColumn("pk1", DataType.INT)
            .addColumn("pk2", DataType.TEXT)
            .addColumn("ck1", DataType.BIGINT)
            .addColumn("ck2", DataType.TEXT)
            .addColumn("col4", DataType.FLOAT)
            .addColumn("col5", DataType.DATE)
            .addColumn("col6", DataType.TIMESTAMP)
            .addSecondaryIndex("ck2")
            .addSecondaryIndex("col4")
            .build();

    when(tableMetadataService.getTableMetadata(
            any(Connection.class), eq(leftSourceNamespace), eq(leftSourceTable)))
        .thenReturn(leftSourceTableMetadata);
    when(tableMetadataService.getTableMetadata(
            any(Connection.class), eq(rightSourceNamespace), eq(rightSourceTable)))
        .thenReturn(rightSourceTableMetadata);

    JdbcAdmin admin = createJdbcAdmin();

    // Act
    TableMetadata result = admin.getTableMetadata(namespace, table);

    // Assert
    assertThat(result).isNotNull();
    assertThat(result.getPartitionKeyNames()).containsExactly("pk1", "pk2");
    assertThat(result.getClusteringKeyNames()).containsExactly("ck1", "ck2");
    assertThat(result.getClusteringOrder("ck1")).isEqualTo(Order.ASC);
    assertThat(result.getClusteringOrder("ck2")).isEqualTo(Order.DESC);
    assertThat(result.getColumnNames())
        .containsExactlyInAnyOrder(
            "pk1", "pk2", "ck1", "ck2", "col1", "col2", "col3", "col4", "col5", "col6");
    assertThat(result.getColumnDataType("pk1")).isEqualTo(DataType.INT);
    assertThat(result.getColumnDataType("pk2")).isEqualTo(DataType.TEXT);
    assertThat(result.getColumnDataType("ck1")).isEqualTo(DataType.BIGINT);
    assertThat(result.getColumnDataType("ck2")).isEqualTo(DataType.TEXT);
    assertThat(result.getColumnDataType("col1")).isEqualTo(DataType.BOOLEAN);
    assertThat(result.getColumnDataType("col2")).isEqualTo(DataType.DOUBLE);
    assertThat(result.getColumnDataType("col3")).isEqualTo(DataType.BLOB);
    assertThat(result.getColumnDataType("col4")).isEqualTo(DataType.FLOAT);
    assertThat(result.getColumnDataType("col5")).isEqualTo(DataType.DATE);
    assertThat(result.getColumnDataType("col6")).isEqualTo(DataType.TIMESTAMP);
    assertThat(result.getSecondaryIndexNames())
        .containsExactlyInAnyOrder("ck1", "ck2", "col1", "col2", "col4");
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
        "CREATE USER \"my_ns\" IDENTIFIED BY \"Oracle1234!@#$\"",
        "ALTER USER \"my_ns\" quota unlimited on USERS");
  }

  @Test
  public void createNamespace_forSqlite_shouldExecuteCreateNamespaceStatement() {
    // no sql is executed
  }

  @Test
  public void createNamespace_forDb2_shouldExecuteCreateNamespaceStatement()
      throws ExecutionException, SQLException {
    createNamespace_forX_shouldExecuteCreateNamespaceStatement(
        RdbEngine.DB2, "CREATE SCHEMA \"my_ns\"");
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
  public void createNamespace_ForSqlite_withInvalidNamespaceName_ShouldThrowExecutionException() {
    // Arrange
    String namespace = "my$ns"; // contains namespace separator

    JdbcAdmin admin = createJdbcAdminFor(RdbEngine.SQLITE);

    // Act
    // Assert
    assertThatThrownBy(() -> admin.createNamespace(namespace))
        .isInstanceOf(IllegalArgumentException.class);
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
        "CREATE TABLE `my_ns`.`foo_table`(`c3` BOOLEAN,`c1` VARCHAR(128),`c5` INT,`c2` BIGINT,`c4` LONGBLOB,`c6` DOUBLE,`c7` REAL,`c8` DATE,`c9` TIME(6),`c10` DATETIME(3),`c11` DATETIME(3), PRIMARY KEY (`c3` ASC,`c1` DESC,`c5` ASC))",
        "CREATE INDEX `index_my_ns_foo_table_c5` ON `my_ns`.`foo_table` (`c5`)",
        "CREATE INDEX `index_my_ns_foo_table_c1` ON `my_ns`.`foo_table` (`c1`)");
  }

  @Test
  public void
      createTableInternal_ForMysqlWithModifiedKeyColumnSize_ShouldCreateTableAndIndexesWithModifiedKeyColumnSize()
          throws SQLException {
    when(config.getMysqlVariableKeyColumnSize()).thenReturn(64);
    createTableInternal_ForX_CreateTableAndIndexes(
        new RdbEngineMysql(config),
        "CREATE TABLE `my_ns`.`foo_table`(`c3` BOOLEAN,`c1` VARCHAR(64),`c5` INT,`c2` BIGINT,`c4` LONGBLOB,`c6` DOUBLE,`c7` REAL,`c8` DATE,`c9` TIME(6),`c10` DATETIME(3),`c11` DATETIME(3), PRIMARY KEY (`c3` ASC,`c1` DESC,`c5` ASC))",
        "CREATE INDEX `index_my_ns_foo_table_c5` ON `my_ns`.`foo_table` (`c5`)",
        "CREATE INDEX `index_my_ns_foo_table_c1` ON `my_ns`.`foo_table` (`c1`)");
  }

  @Test
  public void createTableInternal_ForPostgresql_ShouldCreateTableAndIndexes() throws SQLException {
    createTableInternal_ForX_CreateTableAndIndexes(
        RdbEngine.POSTGRESQL,
        "CREATE TABLE \"my_ns\".\"foo_table\"(\"c3\" BOOLEAN,\"c1\" VARCHAR(10485760),\"c5\" INT,\"c2\" BIGINT,\"c4\" BYTEA,\"c6\" DOUBLE PRECISION,\"c7\" REAL,\"c8\" DATE,\"c9\" TIME,\"c10\" TIMESTAMP,\"c11\" TIMESTAMP WITH TIME ZONE, PRIMARY KEY (\"c3\",\"c1\",\"c5\"))",
        "CREATE UNIQUE INDEX \"my_ns.foo_table_clustering_order_idx\" ON \"my_ns\".\"foo_table\" (\"c3\" ASC,\"c1\" DESC,\"c5\" ASC)",
        "CREATE INDEX \"index_my_ns_foo_table_c5\" ON \"my_ns\".\"foo_table\" (\"c5\")",
        "CREATE INDEX \"index_my_ns_foo_table_c1\" ON \"my_ns\".\"foo_table\" (\"c1\")");
  }

  @Test
  public void createTableInternal_ForSqlServer_ShouldCreateTableAndIndexes() throws SQLException {
    createTableInternal_ForX_CreateTableAndIndexes(
        RdbEngine.SQL_SERVER,
        "CREATE TABLE [my_ns].[foo_table]([c3] BIT,[c1] VARCHAR(8000),[c5] INT,[c2] BIGINT,[c4] VARBINARY(8000),[c6] FLOAT,[c7] FLOAT(24),[c8] DATE,[c9] TIME(6),[c10] DATETIME2(3),[c11] DATETIMEOFFSET(3), PRIMARY KEY ([c3] ASC,[c1] DESC,[c5] ASC))",
        "CREATE INDEX [index_my_ns_foo_table_c5] ON [my_ns].[foo_table] ([c5])",
        "CREATE INDEX [index_my_ns_foo_table_c1] ON [my_ns].[foo_table] ([c1])");
  }

  @Test
  public void createTableInternal_ForOracle_ShouldCreateTableAndIndexes() throws SQLException {
    createTableInternal_ForX_CreateTableAndIndexes(
        RdbEngine.ORACLE,
        "CREATE TABLE \"my_ns\".\"foo_table\"(\"c3\" NUMBER(1),\"c1\" VARCHAR2(128),\"c5\" NUMBER(10),\"c2\" NUMBER(16),\"c4\" BLOB,\"c6\" BINARY_DOUBLE,\"c7\" BINARY_FLOAT,\"c8\" DATE,\"c9\" TIMESTAMP(6),\"c10\" TIMESTAMP(3),\"c11\" TIMESTAMP(3) WITH TIME ZONE, PRIMARY KEY (\"c3\",\"c1\",\"c5\")) ROWDEPENDENCIES",
        "ALTER TABLE \"my_ns\".\"foo_table\" INITRANS 3 MAXTRANS 255",
        "CREATE UNIQUE INDEX \"my_ns.foo_table_clustering_order_idx\" ON \"my_ns\".\"foo_table\" (\"c3\" ASC,\"c1\" DESC,\"c5\" ASC)",
        "CREATE INDEX \"my_ns\".\"index_my_ns_foo_table_c5\" ON \"my_ns\".\"foo_table\" (\"c5\")",
        "CREATE INDEX \"my_ns\".\"index_my_ns_foo_table_c1\" ON \"my_ns\".\"foo_table\" (\"c1\")");
  }

  @Test
  public void
      createTableInternal_ForOracleWithModifiedKeyColumnSize_ShouldCreateTableAndIndexesWithModifiedKeyColumnSize()
          throws SQLException {
    when(config.getOracleVariableKeyColumnSize()).thenReturn(64);
    createTableInternal_ForX_CreateTableAndIndexes(
        new RdbEngineOracle(config),
        "CREATE TABLE \"my_ns\".\"foo_table\"(\"c3\" NUMBER(1),\"c1\" VARCHAR2(64),\"c5\" NUMBER(10),\"c2\" NUMBER(16),\"c4\" BLOB,\"c6\" BINARY_DOUBLE,\"c7\" BINARY_FLOAT,\"c8\" DATE,\"c9\" TIMESTAMP(6),\"c10\" TIMESTAMP(3),\"c11\" TIMESTAMP(3) WITH TIME ZONE, PRIMARY KEY (\"c3\",\"c1\",\"c5\")) ROWDEPENDENCIES",
        "ALTER TABLE \"my_ns\".\"foo_table\" INITRANS 3 MAXTRANS 255",
        "CREATE UNIQUE INDEX \"my_ns.foo_table_clustering_order_idx\" ON \"my_ns\".\"foo_table\" (\"c3\" ASC,\"c1\" DESC,\"c5\" ASC)",
        "CREATE INDEX \"my_ns\".\"index_my_ns_foo_table_c5\" ON \"my_ns\".\"foo_table\" (\"c5\")",
        "CREATE INDEX \"my_ns\".\"index_my_ns_foo_table_c1\" ON \"my_ns\".\"foo_table\" (\"c1\")");
  }

  @Test
  public void createTableInternal_ForSqlite_ShouldCreateTableAndIndexes() throws SQLException {
    createTableInternal_ForX_CreateTableAndIndexes(
        RdbEngine.SQLITE,
        "CREATE TABLE \"my_ns$foo_table\"(\"c3\" BOOLEAN,\"c1\" TEXT,\"c5\" INT,\"c2\" BIGINT,\"c4\" BLOB,\"c6\" DOUBLE,\"c7\" FLOAT,\"c8\" INT,\"c9\" BIGINT,\"c10\" BIGINT,\"c11\" BIGINT, PRIMARY KEY (\"c3\",\"c1\",\"c5\"))",
        "CREATE INDEX \"index_my_ns_foo_table_c5\" ON \"my_ns$foo_table\" (\"c5\")",
        "CREATE INDEX \"index_my_ns_foo_table_c1\" ON \"my_ns$foo_table\" (\"c1\")");
  }

  @Test
  public void createTableInternal_ForDb2_ShouldCreateTableAndIndexes() throws SQLException {
    when(config.getDb2VariableKeyColumnSize()).thenReturn(64);
    createTableInternal_ForX_CreateTableAndIndexes(
        new RdbEngineDb2(config),
        "CREATE TABLE \"my_ns\".\"foo_table\"(\"c3\" BOOLEAN NOT NULL,\"c1\" VARCHAR(64) NOT NULL,\"c5\" INT NOT NULL,\"c2\" BIGINT,\"c4\" BLOB(2G),\"c6\" DOUBLE,\"c7\" REAL,\"c8\" DATE,\"c9\" TIMESTAMP(6),\"c10\" TIMESTAMP(3),\"c11\" TIMESTAMP(3), PRIMARY KEY (\"c3\",\"c1\",\"c5\"))",
        "CREATE UNIQUE INDEX \"my_ns.foo_table_clustering_order_idx\" ON \"my_ns\".\"foo_table\" (\"c3\" ASC,\"c1\" DESC,\"c5\" ASC)",
        "CREATE INDEX \"my_ns\".\"index_my_ns_foo_table_c5\" ON \"my_ns\".\"foo_table\" (\"c5\")",
        "CREATE INDEX \"my_ns\".\"index_my_ns_foo_table_c1\" ON \"my_ns\".\"foo_table\" (\"c1\")");
  }

  @Test
  public void
      createTableInternal_ForDb2WithModifiedKeyColumnSize_ShouldCreateTableAndIndexesWithModifiedKeyColumnSize()
          throws SQLException {
    createTableInternal_ForX_CreateTableAndIndexes(
        RdbEngine.DB2,
        "CREATE TABLE \"my_ns\".\"foo_table\"(\"c3\" BOOLEAN NOT NULL,\"c1\" VARCHAR(128) NOT NULL,\"c5\" INT NOT NULL,\"c2\" BIGINT,\"c4\" BLOB(2G),\"c6\" DOUBLE,\"c7\" REAL,\"c8\" DATE,\"c9\" TIMESTAMP(6),\"c10\" TIMESTAMP(3),\"c11\" TIMESTAMP(3), PRIMARY KEY (\"c3\",\"c1\",\"c5\"))",
        "CREATE UNIQUE INDEX \"my_ns.foo_table_clustering_order_idx\" ON \"my_ns\".\"foo_table\" (\"c3\" ASC,\"c1\" DESC,\"c5\" ASC)",
        "CREATE INDEX \"my_ns\".\"index_my_ns_foo_table_c5\" ON \"my_ns\".\"foo_table\" (\"c5\")",
        "CREATE INDEX \"my_ns\".\"index_my_ns_foo_table_c1\" ON \"my_ns\".\"foo_table\" (\"c1\")");
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
            .addClusteringKey("c5", Order.ASC)
            .addColumn("c1", DataType.TEXT)
            .addColumn("c2", DataType.BIGINT)
            .addColumn("c3", DataType.BOOLEAN)
            .addColumn("c4", DataType.BLOB)
            .addColumn("c5", DataType.INT)
            .addColumn("c6", DataType.DOUBLE)
            .addColumn("c7", DataType.FLOAT)
            .addColumn("c8", DataType.DATE)
            .addColumn("c9", DataType.TIME)
            .addColumn("c10", DataType.TIMESTAMP)
            .addColumn("c11", DataType.TIMESTAMPTZ)
            .addSecondaryIndex("c1")
            .addSecondaryIndex("c5")
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
        "CREATE TABLE IF NOT EXISTS `my_ns`.`foo_table`(`c3` BOOLEAN,`c1` VARCHAR(128),`c5` INT,`c2` BIGINT,`c4` LONGBLOB,`c6` DOUBLE,`c7` REAL,`c8` DATE,`c9` TIME(6),`c10` DATETIME(3),`c11` DATETIME(3), PRIMARY KEY (`c3` ASC,`c1` DESC,`c5` ASC))",
        "CREATE INDEX `index_my_ns_foo_table_c5` ON `my_ns`.`foo_table` (`c5`)",
        "CREATE INDEX `index_my_ns_foo_table_c1` ON `my_ns`.`foo_table` (`c1`)");
  }

  @Test
  public void createTableInternal_IfNotExistsForPostgresql_ShouldCreateTableAndIndexesIfNotExists()
      throws SQLException {
    createTableInternal_IfNotExistsForX_createTableAndIndexesIfNotExists(
        RdbEngine.POSTGRESQL,
        "CREATE TABLE IF NOT EXISTS \"my_ns\".\"foo_table\"(\"c3\" BOOLEAN,\"c1\" VARCHAR(10485760),\"c5\" INT,\"c2\" BIGINT,\"c4\" BYTEA,\"c6\" DOUBLE PRECISION,\"c7\" REAL,\"c8\" DATE,\"c9\" TIME,\"c10\" TIMESTAMP,\"c11\" TIMESTAMP WITH TIME ZONE, PRIMARY KEY (\"c3\",\"c1\",\"c5\"))",
        "CREATE UNIQUE INDEX IF NOT EXISTS \"my_ns.foo_table_clustering_order_idx\" ON \"my_ns\".\"foo_table\" (\"c3\" ASC,\"c1\" DESC,\"c5\" ASC)",
        "CREATE INDEX IF NOT EXISTS \"index_my_ns_foo_table_c5\" ON \"my_ns\".\"foo_table\" (\"c5\")",
        "CREATE INDEX IF NOT EXISTS \"index_my_ns_foo_table_c1\" ON \"my_ns\".\"foo_table\" (\"c1\")");
  }

  @Test
  public void createTableInternal_IfNotExistsForSqlServer_ShouldCreateTableAndIndexesIfNotExists()
      throws SQLException {
    createTableInternal_IfNotExistsForX_createTableAndIndexesIfNotExists(
        RdbEngine.SQL_SERVER,
        "CREATE TABLE [my_ns].[foo_table]([c3] BIT,[c1] VARCHAR(8000),[c5] INT,[c2] BIGINT,[c4] VARBINARY(8000),[c6] FLOAT,[c7] FLOAT(24),[c8] DATE,[c9] TIME(6),[c10] DATETIME2(3),[c11] DATETIMEOFFSET(3), PRIMARY KEY ([c3] ASC,[c1] DESC,[c5] ASC))",
        "CREATE INDEX [index_my_ns_foo_table_c5] ON [my_ns].[foo_table] ([c5])",
        "CREATE INDEX [index_my_ns_foo_table_c1] ON [my_ns].[foo_table] ([c1])");
  }

  @Test
  public void createTableInternal_IfNotExistsForOracle_ShouldCreateTableAndIndexesIfNotExists()
      throws SQLException {
    createTableInternal_IfNotExistsForX_createTableAndIndexesIfNotExists(
        RdbEngine.ORACLE,
        "CREATE TABLE \"my_ns\".\"foo_table\"(\"c3\" NUMBER(1),\"c1\" VARCHAR2(128),\"c5\" NUMBER(10),\"c2\" NUMBER(16),\"c4\" BLOB,\"c6\" BINARY_DOUBLE,\"c7\" BINARY_FLOAT,\"c8\" DATE,\"c9\" TIMESTAMP(6),\"c10\" TIMESTAMP(3),\"c11\" TIMESTAMP(3) WITH TIME ZONE, PRIMARY KEY (\"c3\",\"c1\",\"c5\")) ROWDEPENDENCIES",
        "ALTER TABLE \"my_ns\".\"foo_table\" INITRANS 3 MAXTRANS 255",
        "CREATE UNIQUE INDEX \"my_ns.foo_table_clustering_order_idx\" ON \"my_ns\".\"foo_table\" (\"c3\" ASC,\"c1\" DESC,\"c5\" ASC)",
        "CREATE INDEX \"my_ns\".\"index_my_ns_foo_table_c5\" ON \"my_ns\".\"foo_table\" (\"c5\")",
        "CREATE INDEX \"my_ns\".\"index_my_ns_foo_table_c1\" ON \"my_ns\".\"foo_table\" (\"c1\")");
  }

  @Test
  public void createTableInternal_IfNotExistsForSqlite_ShouldCreateTableAndIndexesIfNotExists()
      throws SQLException {
    createTableInternal_IfNotExistsForX_createTableAndIndexesIfNotExists(
        RdbEngine.SQLITE,
        "CREATE TABLE IF NOT EXISTS \"my_ns$foo_table\"(\"c3\" BOOLEAN,\"c1\" TEXT,\"c5\" INT,\"c2\" BIGINT,\"c4\" BLOB,\"c6\" DOUBLE,\"c7\" FLOAT,\"c8\" INT,\"c9\" BIGINT,\"c10\" BIGINT,\"c11\" BIGINT, PRIMARY KEY (\"c3\",\"c1\",\"c5\"))",
        "CREATE INDEX IF NOT EXISTS \"index_my_ns_foo_table_c5\" ON \"my_ns$foo_table\" (\"c5\")",
        "CREATE INDEX IF NOT EXISTS \"index_my_ns_foo_table_c1\" ON \"my_ns$foo_table\" (\"c1\")");
  }

  @Test
  public void createTableInternal_IfNotExistsForDb2_ShouldCreateTableAndIndexesIfNotExists()
      throws SQLException {
    createTableInternal_IfNotExistsForX_createTableAndIndexesIfNotExists(
        RdbEngine.DB2,
        "CREATE TABLE IF NOT EXISTS \"my_ns\".\"foo_table\"(\"c3\" BOOLEAN NOT NULL,\"c1\" VARCHAR(128) NOT NULL,\"c5\" INT NOT NULL,\"c2\" BIGINT,\"c4\" BLOB(2G),\"c6\" DOUBLE,\"c7\" REAL,\"c8\" DATE,\"c9\" TIMESTAMP(6),\"c10\" TIMESTAMP(3),\"c11\" TIMESTAMP(3), PRIMARY KEY (\"c3\",\"c1\",\"c5\"))",
        "CREATE UNIQUE INDEX \"my_ns.foo_table_clustering_order_idx\" ON \"my_ns\".\"foo_table\" (\"c3\" ASC,\"c1\" DESC,\"c5\" ASC)",
        "CREATE INDEX \"my_ns\".\"index_my_ns_foo_table_c5\" ON \"my_ns\".\"foo_table\" (\"c5\")",
        "CREATE INDEX \"my_ns\".\"index_my_ns_foo_table_c1\" ON \"my_ns\".\"foo_table\" (\"c1\")");
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
            .addClusteringKey("c5", Order.ASC)
            .addColumn("c1", DataType.TEXT)
            .addColumn("c2", DataType.BIGINT)
            .addColumn("c3", DataType.BOOLEAN)
            .addColumn("c4", DataType.BLOB)
            .addColumn("c5", DataType.INT)
            .addColumn("c6", DataType.DOUBLE)
            .addColumn("c7", DataType.FLOAT)
            .addColumn("c8", DataType.DATE)
            .addColumn("c9", DataType.TIME)
            .addColumn("c10", DataType.TIMESTAMP)
            .addColumn("c11", DataType.TIMESTAMPTZ)
            .addSecondaryIndex("c1")
            .addSecondaryIndex("c5")
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

  @Test
  public void addTableMetadata_OverwriteMetadataForDb2_ShouldWorkProperly() throws Exception {
    addTableMetadata_overwriteMetadataForX_ShouldWorkProperly(
        RdbEngine.DB2,
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

  @Test
  public void addTableMetadata_ifNotExistsAndOverwriteMetadataForDb2_ShouldWorkProperly()
      throws Exception {
    addTableMetadata_createMetadataTableIfNotExistsForXAndOverwriteMetadata_ShouldWorkProperly(
        RdbEngine.DB2,
        "CREATE TABLE IF NOT EXISTS \""
            + METADATA_SCHEMA
            + "\".\"metadata\"("
            + "\"full_table_name\" VARCHAR(128) NOT NULL,"
            + "\"column_name\" VARCHAR(128) NOT NULL,"
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
            + "`.`metadata` VALUES ('my_ns.foo_table','c7','FLOAT',NULL,NULL,false,7)",
        "INSERT INTO `scalardb`.`metadata` VALUES ('my_ns.foo_table','c8','DATE',NULL,NULL,false,8)",
        "INSERT INTO `scalardb`.`metadata` VALUES ('my_ns.foo_table','c9','TIME',NULL,NULL,false,9)",
        "INSERT INTO `scalardb`.`metadata` VALUES ('my_ns.foo_table','c10','TIMESTAMP',NULL,NULL,false,10)",
        "INSERT INTO `scalardb`.`metadata` VALUES ('my_ns.foo_table','c11','TIMESTAMPTZ',NULL,NULL,false,11)");
  }

  @Test
  public void
      addTableMetadata_ifNotExistsAndDoNotOverwriteMetadataForPostgresql_ShouldWorkProperly()
          throws Exception {
    addTableMetadata_createMetadataTableIfNotExistsForX_ShouldWorkProperly(
        RdbEngine.POSTGRESQL,
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
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c7','FLOAT',NULL,NULL,false,7)",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c8','DATE',NULL,NULL,false,8)",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c9','TIME',NULL,NULL,false,9)",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c10','TIMESTAMP',NULL,NULL,false,10)",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c11','TIMESTAMPTZ',NULL,NULL,false,11)");
  }

  @Test
  public void addTableMetadata_ifNotExistsAndDoNotOverwriteMetadataForSqlServer_ShouldWorkProperly()
      throws Exception {
    addTableMetadata_createMetadataTableIfNotExistsForX_ShouldWorkProperly(
        RdbEngine.SQL_SERVER,
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
            + "].[metadata] VALUES ('my_ns.foo_table','c7','FLOAT',NULL,NULL,0,7)",
        "INSERT INTO [scalardb].[metadata] VALUES ('my_ns.foo_table','c8','DATE',NULL,NULL,0,8)",
        "INSERT INTO [scalardb].[metadata] VALUES ('my_ns.foo_table','c9','TIME',NULL,NULL,0,9)",
        "INSERT INTO [scalardb].[metadata] VALUES ('my_ns.foo_table','c10','TIMESTAMP',NULL,NULL,0,10)",
        "INSERT INTO [scalardb].[metadata] VALUES ('my_ns.foo_table','c11','TIMESTAMPTZ',NULL,NULL,0,11)");
  }

  @Test
  public void addTableMetadata_ifNotExistsAndDoNotOverwriteMetadataForOracle_ShouldWorkProperly()
      throws Exception {
    addTableMetadata_createMetadataTableIfNotExistsForX_ShouldWorkProperly(
        RdbEngine.ORACLE,
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
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c7','FLOAT',NULL,NULL,0,7)",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c8','DATE',NULL,NULL,0,8)",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c9','TIME',NULL,NULL,0,9)",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c10','TIMESTAMP',NULL,NULL,0,10)",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c11','TIMESTAMPTZ',NULL,NULL,0,11)");
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
            + "$metadata\" VALUES ('my_ns.foo_table','c7','FLOAT',NULL,NULL,FALSE,7)",
        "INSERT INTO \"scalardb$metadata\" VALUES ('my_ns.foo_table','c8','DATE',NULL,NULL,FALSE,8)",
        "INSERT INTO \"scalardb$metadata\" VALUES ('my_ns.foo_table','c9','TIME',NULL,NULL,FALSE,9)",
        "INSERT INTO \"scalardb$metadata\" VALUES ('my_ns.foo_table','c10','TIMESTAMP',NULL,NULL,FALSE,10)",
        "INSERT INTO \"scalardb$metadata\" VALUES ('my_ns.foo_table','c11','TIMESTAMPTZ',NULL,NULL,FALSE,11)");
  }

  @Test
  public void addTableMetadata_ifNotExistsAndDoNotOverwriteMetadataForDb2_ShouldWorkProperly()
      throws Exception {
    addTableMetadata_createMetadataTableIfNotExistsForX_ShouldWorkProperly(
        RdbEngine.DB2,
        "CREATE TABLE IF NOT EXISTS \""
            + METADATA_SCHEMA
            + "\".\"metadata\"("
            + "\"full_table_name\" VARCHAR(128) NOT NULL,"
            + "\"column_name\" VARCHAR(128) NOT NULL,"
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
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c7','FLOAT',NULL,NULL,false,7)",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c8','DATE',NULL,NULL,false,8)",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c9','TIME',NULL,NULL,false,9)",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c10','TIMESTAMP',NULL,NULL,false,10)",
        "INSERT INTO \"scalardb\".\"metadata\" VALUES ('my_ns.foo_table','c11','TIMESTAMPTZ',NULL,NULL,false,11)");
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
            .addColumn("c8", DataType.DATE)
            .addColumn("c9", DataType.TIME)
            .addColumn("c10", DataType.TIMESTAMP)
            .addColumn("c11", DataType.TIMESTAMPTZ)
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
            .addClusteringKey("c5", Order.ASC)
            .addColumn("c1", DataType.TEXT)
            .addColumn("c2", DataType.BIGINT)
            .addColumn("c3", DataType.BOOLEAN)
            .addColumn("c4", DataType.BLOB)
            .addColumn("c5", DataType.INT)
            .addColumn("c6", DataType.DOUBLE)
            .addColumn("c7", DataType.FLOAT)
            .addColumn("c8", DataType.DATE)
            .addColumn("c9", DataType.TIME)
            .addColumn("c10", DataType.TIMESTAMP)
            .addColumn("c11", DataType.TIMESTAMPTZ)
            .addSecondaryIndex("c1")
            .addSecondaryIndex("c5")
            .build();
    when(connection.createStatement()).thenReturn(mock(Statement.class));
    when(dataSource.getConnection()).thenReturn(connection);

    JdbcAdmin adminSpy = spy(createJdbcAdminFor(rdbEngine));

    // Act
    adminSpy.createTable(namespace, table, metadata, Collections.emptyMap());

    // Assert
    verify(adminSpy).createMetadataSchemaIfNotExists(connection);
    verify(adminSpy).createTableInternal(connection, namespace, table, metadata, false);
    verify(adminSpy).addTableMetadata(connection, namespace, table, metadata, true, false);
  }

  @Test
  public void repairTable_ForMysql_shouldCreateMetadataTableAndAddMetadataForTable()
      throws SQLException, ExecutionException {
    repairTable_ForX_shouldCreateMetadataTableAndAddMetadataForTable(
        RdbEngine.MYSQL,
        "SELECT 1 FROM `my_ns`.`foo_table` LIMIT 1",
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
            + "`.`metadata` VALUES ('my_ns.foo_table','c1','TEXT','PARTITION',NULL,false,1)");
  }

  @Test
  public void repairTable_ForOracle_shouldCreateMetadataTableAndAddMetadataForTable()
      throws SQLException, ExecutionException {
    repairTable_ForX_shouldCreateMetadataTableAndAddMetadataForTable(
        RdbEngine.ORACLE,
        "SELECT 1 FROM \"my_ns\".\"foo_table\" FETCH FIRST 1 ROWS ONLY",
        "CREATE USER \"" + METADATA_SCHEMA + "\" IDENTIFIED BY \"Oracle1234!@#$\"",
        "ALTER USER \"" + METADATA_SCHEMA + "\" quota unlimited on USERS",
        "CREATE TABLE \""
            + METADATA_SCHEMA
            + "\".\"metadata\"("
            + "\"full_table_name\" VARCHAR2(128),"
            + "\"column_name\" VARCHAR2(128),"
            + "\"data_type\" VARCHAR2(20) NOT NULL,"
            + "\"key_type\" VARCHAR2(20),"
            + "\"clustering_order\" VARCHAR2(10),"
            + "\"indexed\" NUMBER(1) NOT NULL,"
            + "\"ordinal_position\" INTEGER NOT NULL,"
            + "PRIMARY KEY (\"full_table_name\", \"column_name\"))",
        "DELETE FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\" = 'my_ns.foo_table'",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c1','TEXT','PARTITION',NULL,0,1)");
  }

  @Test
  public void repairTable_ForPosgresql_shouldCreateMetadataTableAndAddMetadataForTable()
      throws SQLException, ExecutionException {
    repairTable_ForX_shouldCreateMetadataTableAndAddMetadataForTable(
        RdbEngine.POSTGRESQL,
        "SELECT 1 FROM \"my_ns\".\"foo_table\" LIMIT 1",
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
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c1','TEXT','PARTITION',NULL,false,1)");
  }

  @Test
  public void repairTable_ForSqlServer_shouldCreateMetadataTableAndAddMetadataForTable()
      throws SQLException, ExecutionException {
    repairTable_ForX_shouldCreateMetadataTableAndAddMetadataForTable(
        RdbEngine.SQL_SERVER,
        "SELECT TOP 1 1 FROM [my_ns].[foo_table]",
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
            + "].[metadata] VALUES ('my_ns.foo_table','c1','TEXT','PARTITION',NULL,0,1)");
  }

  @Test
  public void repairTable_ForSqlite_shouldCreateMetadataTableAndAddMetadataForTable()
      throws SQLException, ExecutionException {
    repairTable_ForX_shouldCreateMetadataTableAndAddMetadataForTable(
        RdbEngine.SQLITE,
        "SELECT 1 FROM \"my_ns$foo_table\" LIMIT 1",
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
            + "$metadata\" VALUES ('my_ns.foo_table','c1','TEXT','PARTITION',NULL,FALSE,1)");
  }

  @Test
  public void repairTable_ForDb2_shouldCreateMetadataTableAndAddMetadataForTable()
      throws SQLException, ExecutionException {
    repairTable_ForX_shouldCreateMetadataTableAndAddMetadataForTable(
        RdbEngine.DB2,
        "SELECT 1 FROM \"my_ns\".\"foo_table\" LIMIT 1",
        "CREATE SCHEMA \"" + METADATA_SCHEMA + "\"",
        "CREATE TABLE IF NOT EXISTS \""
            + METADATA_SCHEMA
            + "\".\"metadata\"("
            + "\"full_table_name\" VARCHAR(128) NOT NULL,"
            + "\"column_name\" VARCHAR(128) NOT NULL,"
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
            + "\".\"metadata\" VALUES ('my_ns.foo_table','c1','TEXT','PARTITION',NULL,false,1)");
  }

  private void repairTable_ForX_shouldCreateMetadataTableAndAddMetadataForTable(
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

  @Test
  public void repairTable_WithNonExistingTableToRepairForDb2_shouldThrowIllegalArgumentException()
      throws SQLException {
    repairTable_WithNonExistingTableToRepairForX_shouldThrowIllegalArgumentException(
        RdbEngine.DB2, "SELECT 1 FROM \"my_ns\".\"foo_table\" LIMIT 1");
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
  public void
      createMetadataTableIfNotExists_WithInternalDbError_forMysql_shouldThrowInternalDbError()
          throws SQLException {
    createTableMetadataTableIfNotExists_WithInternalDbError_forX_shouldThrowInternalDbError(
        RdbEngine.MYSQL, new CommunicationsException("", null));
  }

  @Test
  public void
      createMetadataTableIfNotExists_WithInternalDbError_forPostgresql_shouldThrowInternalDbError()
          throws SQLException {
    createTableMetadataTableIfNotExists_WithInternalDbError_forX_shouldThrowInternalDbError(
        RdbEngine.POSTGRESQL, new PSQLException("", PSQLState.CONNECTION_FAILURE));
  }

  @Test
  public void
      createMetadataTableIfNotExists_WithInternalDbError_forSqlite_shouldThrowInternalDbError()
          throws SQLException {
    createTableMetadataTableIfNotExists_WithInternalDbError_forX_shouldThrowInternalDbError(
        RdbEngine.SQLITE, new SQLiteException("", SQLiteErrorCode.SQLITE_IOERR));
  }

  private void
      createTableMetadataTableIfNotExists_WithInternalDbError_forX_shouldThrowInternalDbError(
          RdbEngine rdbEngine, SQLException internalDbError) throws SQLException {
    // Arrange
    when(connection.createStatement()).thenThrow(internalDbError);
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    // Act
    // Assert
    assertThatThrownBy(() -> admin.createTableMetadataTableIfNotExists(connection))
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

  @Test
  public void truncateTable_forDb2_shouldExecuteTruncateTableStatement()
      throws SQLException, ExecutionException {
    truncateTable_forX_shouldExecuteTruncateTableStatement(
        RdbEngine.DB2, "TRUNCATE TABLE \"my_ns\".\"foo_table\" IMMEDIATE");
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
  public void truncateTable_VirtualTableExists_ShouldTruncateSourceTables() throws Exception {
    // Arrange
    String namespace = "ns";
    String table = "vtable";
    String leftSourceNamespace = "ns";
    String leftSourceTable = "left_table";
    String rightSourceNamespace = "ns";
    String rightSourceTable = "right_table";

    when(dataSource.getConnection()).thenReturn(connection);

    VirtualTableInfo virtualTableInfo = mock(VirtualTableInfo.class);
    when(virtualTableInfo.getLeftSourceNamespaceName()).thenReturn(leftSourceNamespace);
    when(virtualTableInfo.getLeftSourceTableName()).thenReturn(leftSourceTable);
    when(virtualTableInfo.getRightSourceNamespaceName()).thenReturn(rightSourceNamespace);
    when(virtualTableInfo.getRightSourceTableName()).thenReturn(rightSourceTable);

    when(virtualTableMetadataService.getVirtualTableInfo(connection, namespace, table))
        .thenReturn(virtualTableInfo);

    Statement statement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(statement);

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);

    JdbcAdmin admin = createJdbcAdmin();

    // Act
    admin.truncateTable(namespace, table);

    // Assert
    verify(statement, times(2)).execute(sqlCaptor.capture());
    List<String> executedSqls = sqlCaptor.getAllValues();
    assertThat(executedSqls).hasSize(2);
    assertThat(executedSqls.get(0)).isEqualTo("TRUNCATE TABLE `ns`.`left_table`");
    assertThat(executedSqls.get(1)).isEqualTo("TRUNCATE TABLE `ns`.`right_table`");
  }

  @Test
  public void truncateTable_SQLExceptionThrown_ShouldThrowExecutionException() throws Exception {
    // Arrange
    String namespace = "ns";
    String table = "vtable";
    String leftSourceNamespace = "ns";
    String leftSourceTable = "left_table";
    String rightSourceNamespace = "ns";
    String rightSourceTable = "right_table";

    when(dataSource.getConnection()).thenReturn(connection);

    VirtualTableInfo virtualTableInfo = mock(VirtualTableInfo.class);
    when(virtualTableInfo.getLeftSourceNamespaceName()).thenReturn(leftSourceNamespace);
    when(virtualTableInfo.getLeftSourceTableName()).thenReturn(leftSourceTable);
    when(virtualTableInfo.getRightSourceNamespaceName()).thenReturn(rightSourceNamespace);
    when(virtualTableInfo.getRightSourceTableName()).thenReturn(rightSourceTable);

    when(virtualTableMetadataService.getVirtualTableInfo(connection, namespace, table))
        .thenReturn(virtualTableInfo);

    Statement statement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(statement);

    SQLException sqlException = new SQLException("SQL error occurred");
    when(statement.execute(anyString())).thenThrow(sqlException);

    JdbcAdmin admin = createJdbcAdmin();

    // Act Assert
    assertThatThrownBy(() -> admin.truncateTable(namespace, table))
        .isInstanceOf(ExecutionException.class)
        .hasCause(sqlException);
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
        "DROP SCHEMA `" + METADATA_SCHEMA + "`");
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
        "DROP SCHEMA \"" + METADATA_SCHEMA + "\"");
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
        "DROP SCHEMA [" + METADATA_SCHEMA + "]");
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
        "DROP USER \"" + METADATA_SCHEMA + "\"");
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
        "DROP TABLE \"" + METADATA_SCHEMA + "$metadata\"");
  }

  @Test
  public void dropTable_forDb2WithNoMoreMetadataAfterDeletion_shouldDropTableAndDeleteMetadata()
      throws Exception {
    dropTable_forXWithNoMoreMetadataAfterDeletion_shouldDropTableAndDeleteMetadata(
        RdbEngine.DB2,
        "DROP TABLE \"my_ns\".\"foo_table\"",
        "DELETE FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\" = 'my_ns.foo_table'",
        "SELECT DISTINCT \"full_table_name\" FROM \"" + METADATA_SCHEMA + "\".\"metadata\"",
        "DROP TABLE \"" + METADATA_SCHEMA + "\".\"metadata\"",
        "DROP SCHEMA \"" + METADATA_SCHEMA + "\" RESTRICT");
  }

  private void dropTable_forXWithNoMoreMetadataAfterDeletion_shouldDropTableAndDeleteMetadata(
      RdbEngine rdbEngine, String... expectedSqlStatements) throws Exception {
    // Arrange
    String namespace = "my_ns";
    String table = "foo_table";

    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(false);

    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    when(preparedStatement.executeQuery()).thenReturn(resultSet);
    when(connection.prepareStatement(any())).thenReturn(preparedStatement);

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
            + METADATA_SCHEMA
            + "`.`metadata` WHERE `full_table_name` = 'my_ns.foo_table'",
        "SELECT DISTINCT `full_table_name` FROM `" + METADATA_SCHEMA + "`.`metadata`");
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
        "SELECT DISTINCT \"full_table_name\" FROM \"" + METADATA_SCHEMA + "\".\"metadata\"");
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
        "SELECT DISTINCT [full_table_name] FROM [" + METADATA_SCHEMA + "].[metadata]");
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
        "SELECT DISTINCT \"full_table_name\" FROM \"" + METADATA_SCHEMA + "\".\"metadata\"");
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
        "SELECT DISTINCT \"full_table_name\" FROM \"" + METADATA_SCHEMA + "$metadata\"");
  }

  @Test
  public void
      dropTable_forDb2WithOtherMetadataAfterDeletion_ShouldDropTableAndDeleteMetadataButNotMetadataTable()
          throws Exception {
    dropTable_forXWithOtherMetadataAfterDeletion_ShouldDropTableAndDeleteMetadataButNotMetadataTable(
        RdbEngine.DB2,
        "DROP TABLE \"my_ns\".\"foo_table\"",
        "DELETE FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\" = 'my_ns.foo_table'",
        "SELECT DISTINCT \"full_table_name\" FROM \"" + METADATA_SCHEMA + "\".\"metadata\"");
  }

  private void
      dropTable_forXWithOtherMetadataAfterDeletion_ShouldDropTableAndDeleteMetadataButNotMetadataTable(
          RdbEngine rdbEngine, String... expectedSqlStatements) throws Exception {
    // Arrange
    String namespace = "my_ns";
    String table = "foo_table";

    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(true, false);

    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    when(preparedStatement.executeQuery()).thenReturn(resultSet);
    when(connection.prepareStatement(any())).thenReturn(preparedStatement);

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
  public void dropTable_VirtualTableExists_ShouldDropViewAndDeleteMetadata() throws Exception {
    // Arrange
    String namespace = "ns";
    String table = "vtable";

    when(dataSource.getConnection()).thenReturn(connection);

    VirtualTableInfo virtualTableInfo = mock(VirtualTableInfo.class);
    when(virtualTableMetadataService.getVirtualTableInfo(connection, namespace, table))
        .thenReturn(virtualTableInfo);

    Statement statement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(statement);

    doNothing()
        .when(virtualTableMetadataService)
        .deleteFromVirtualTablesTable(any(Connection.class), eq(namespace), eq(table));
    doNothing()
        .when(virtualTableMetadataService)
        .deleteVirtualTablesTableIfEmpty(any(Connection.class));

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);

    JdbcAdmin admin = createJdbcAdmin();

    // Act
    admin.dropTable(namespace, table);

    // Assert
    verify(statement).execute(sqlCaptor.capture());
    String executedSql = sqlCaptor.getValue();
    assertThat(executedSql).isEqualTo("DROP VIEW `ns`.`vtable`");
    verify(virtualTableMetadataService)
        .deleteFromVirtualTablesTable(eq(connection), eq(namespace), eq(table));
    verify(virtualTableMetadataService).deleteVirtualTablesTableIfEmpty(eq(connection));
  }

  @Test
  public void dropTable_SourceTableUsedByVirtualTable_ShouldThrowIllegalArgumentException()
      throws Exception {
    // Arrange
    String namespace = "ns";
    String table = "source_table";

    when(dataSource.getConnection()).thenReturn(connection);

    // Not a virtual table itself
    when(virtualTableMetadataService.getVirtualTableInfo(connection, namespace, table))
        .thenReturn(null);

    // But used as a source table
    VirtualTableInfo virtualTableInfo1 = mock(VirtualTableInfo.class);
    when(virtualTableInfo1.getNamespaceName()).thenReturn("ns");
    when(virtualTableInfo1.getTableName()).thenReturn("vtable1");

    VirtualTableInfo virtualTableInfo2 = mock(VirtualTableInfo.class);
    when(virtualTableInfo2.getNamespaceName()).thenReturn("ns");
    when(virtualTableInfo2.getTableName()).thenReturn("vtable2");

    List<VirtualTableInfo> virtualTableInfos = Arrays.asList(virtualTableInfo1, virtualTableInfo2);

    when(virtualTableMetadataService.getVirtualTableInfosBySourceTable(
            connection, namespace, table))
        .thenReturn(virtualTableInfos);

    JdbcAdmin admin = createJdbcAdmin();

    // Act Assert
    assertThatThrownBy(() -> admin.dropTable(namespace, table))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("ns.source_table")
        .hasMessageContaining("ns.vtable1")
        .hasMessageContaining("ns.vtable2");
  }

  @Test
  public void dropTable_SQLExceptionThrown_ShouldThrowExecutionException() throws Exception {
    // Arrange
    String namespace = "ns";
    String table = "vtable";

    when(dataSource.getConnection()).thenReturn(connection);

    VirtualTableInfo virtualTableInfo = mock(VirtualTableInfo.class);
    when(virtualTableMetadataService.getVirtualTableInfo(connection, namespace, table))
        .thenReturn(virtualTableInfo);

    Statement statement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(statement);

    SQLException sqlException = new SQLException("SQL error occurred");
    when(statement.execute(anyString())).thenThrow(sqlException);

    JdbcAdmin admin = createJdbcAdmin();

    // Act Assert
    assertThatThrownBy(() -> admin.dropTable(namespace, table))
        .isInstanceOf(ExecutionException.class)
        .hasCause(sqlException);
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

  @Test
  public void dropNamespace_forDb2_shouldDropNamespace() throws Exception {
    dropSchema_forX_shouldDropSchema(RdbEngine.DB2, "DROP SCHEMA \"my_ns\" RESTRICT");
  }

  private void dropSchema_forX_shouldDropSchema(
      RdbEngine rdbEngine, String expectedDropSchemaStatement) throws Exception {
    // Arrange
    String namespace = "my_ns";
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    Connection connection = mock(Connection.class);
    Statement dropSchemaStatement = mock(Statement.class);
    PreparedStatement getTableNamesPrepStmt = mock(PreparedStatement.class);
    ResultSet emptyResultSet = mock(ResultSet.class);

    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.createStatement()).thenReturn(dropSchemaStatement);
    when(connection.prepareStatement(any())).thenReturn(getTableNamesPrepStmt);
    when(emptyResultSet.next()).thenReturn(false);
    when(getTableNamesPrepStmt.executeQuery()).thenReturn(emptyResultSet);

    // Act
    admin.dropNamespace(namespace);

    // Assert
    verify(dropSchemaStatement).execute(expectedDropSchemaStatement);
  }

  @Test
  public void dropNamespace_WithNonScalarDBTableLeftForMysql_ShouldThrowIllegalArgumentException()
      throws Exception {
    dropNamespace_WithNonScalarDBTableLeftForX_ShouldThrowIllegalArgumentException(RdbEngine.MYSQL);
  }

  @Test
  public void
      dropNamespace_WithNonScalarDBTableLeftForPostgresql_ShouldThrowIllegalArgumentException()
          throws Exception {
    dropNamespace_WithNonScalarDBTableLeftForX_ShouldThrowIllegalArgumentException(
        RdbEngine.POSTGRESQL);
  }

  @Test
  public void
      dropNamespace_WithNonScalarDBTableLeftForSqlServer_ShouldThrowIllegalArgumentException()
          throws Exception {
    dropNamespace_WithNonScalarDBTableLeftForX_ShouldThrowIllegalArgumentException(
        RdbEngine.SQL_SERVER);
  }

  @Test
  public void dropNamespace_WithNonScalarDBTableLeftForOracle_ShouldThrowIllegalArgumentException()
      throws Exception {
    dropNamespace_WithNonScalarDBTableLeftForX_ShouldThrowIllegalArgumentException(
        RdbEngine.ORACLE);
  }

  @Test
  public void dropNamespace_WithNonScalarDBTableLeftForSqlite_ShouldThrowIllegalArgumentException()
      throws Exception {
    // Do nothing. SQLite does not have a concept of namespaces.
  }

  @Test
  public void dropNamespace_WithNonScalarDBTableLeftForDb2_ShouldThrowIllegalArgumentException()
      throws Exception {
    dropNamespace_WithNonScalarDBTableLeftForX_ShouldThrowIllegalArgumentException(RdbEngine.DB2);
  }

  private void dropNamespace_WithNonScalarDBTableLeftForX_ShouldThrowIllegalArgumentException(
      RdbEngine rdbEngine) throws Exception {
    // Arrange
    String namespace = "my_ns";
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    Connection connection = mock(Connection.class);
    Statement dropNamespaceStatementMock = mock(Statement.class);
    Statement selectNamespaceStatementMock = mock(Statement.class);
    PreparedStatement getTableNamesPrepStmt = mock(PreparedStatement.class);
    when(connection.createStatement())
        .thenReturn(dropNamespaceStatementMock, selectNamespaceStatementMock);
    ResultSet emptyResultSet = mock(ResultSet.class);
    when(emptyResultSet.next()).thenReturn(true).thenReturn(false);
    when(getTableNamesPrepStmt.executeQuery()).thenReturn(emptyResultSet);
    when(connection.prepareStatement(any())).thenReturn(getTableNamesPrepStmt);
    when(dataSource.getConnection()).thenReturn(connection);
    // Namespaces table does not contain other namespaces
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(false);
    when(selectNamespaceStatementMock.executeQuery(anyString())).thenReturn(resultSet);

    // Act Assert
    assertThatCode(() -> admin.dropNamespace(namespace))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void getNamespaceTableNames_forMysql_ShouldReturnTableNames() throws Exception {
    getNamespaceTableNames_forX_ShouldReturnTableNames(
        RdbEngine.MYSQL,
        "SELECT DISTINCT `full_table_name` FROM `"
            + METADATA_SCHEMA
            + "`.`metadata` WHERE `full_table_name` LIKE ?");
  }

  @Test
  public void getNamespaceTableNames_forPostgresql_ShouldReturnTableNames() throws Exception {
    getNamespaceTableNames_forX_ShouldReturnTableNames(
        RdbEngine.POSTGRESQL,
        "SELECT DISTINCT \"full_table_name\" FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\" LIKE ?");
  }

  @Test
  public void getNamespaceTableNames_forSqlServer_ShouldReturnTableNames() throws Exception {
    getNamespaceTableNames_forX_ShouldReturnTableNames(
        RdbEngine.SQL_SERVER,
        "SELECT DISTINCT [full_table_name] FROM ["
            + METADATA_SCHEMA
            + "].[metadata] WHERE [full_table_name] LIKE ?");
  }

  @Test
  public void getNamespaceTableNames_forOracle_ShouldReturnTableNames() throws Exception {
    getNamespaceTableNames_forX_ShouldReturnTableNames(
        RdbEngine.ORACLE,
        "SELECT DISTINCT \"full_table_name\" FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\" LIKE ?");
  }

  @Test
  public void getNamespaceTableNames_forSqlite_ShouldReturnTableNames() throws Exception {
    getNamespaceTableNames_forX_ShouldReturnTableNames(
        RdbEngine.SQLITE,
        "SELECT DISTINCT \"full_table_name\" FROM \""
            + METADATA_SCHEMA
            + "$metadata\" WHERE \"full_table_name\" LIKE ?");
  }

  @Test
  public void getNamespaceTableNames_forDb2_ShouldReturnTableNames() throws Exception {
    getNamespaceTableNames_forX_ShouldReturnTableNames(
        RdbEngine.DB2,
        "SELECT DISTINCT \"full_table_name\" FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\" LIKE ?");
  }

  private void getNamespaceTableNames_forX_ShouldReturnTableNames(
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
    if (rdbEngine == RdbEngine.MYSQL || rdbEngine == RdbEngine.SQLITE) {
      verify(connection, never()).setReadOnly(anyBoolean());
    } else {
      verify(connection).setReadOnly(true);
    }
    verify(connection).prepareStatement(expectedSelectStatement);
    assertThat(actualTableNames).containsExactly(table1, table2);
    verify(preparedStatement).setString(1, namespace + ".%");
  }

  @Test
  public void getNamespaceTableNames_RegularAndVirtualTablesExist_ShouldReturnBoth()
      throws Exception {
    // Arrange
    String namespace = "ns";

    when(dataSource.getConnection()).thenReturn(connection);

    Set<String> regularTables = new HashSet<>(Arrays.asList("table1", "table2"));
    Set<String> virtualTables = new HashSet<>(Arrays.asList("vtable1", "vtable2"));

    when(tableMetadataService.getNamespaceTableNames(connection, namespace))
        .thenReturn(regularTables);
    when(virtualTableMetadataService.getNamespaceTableNames(connection, namespace))
        .thenReturn(virtualTables);

    JdbcAdmin admin = createJdbcAdmin();

    // Act
    Set<String> result = admin.getNamespaceTableNames(namespace);

    // Assert
    assertThat(result).containsExactlyInAnyOrder("table1", "table2", "vtable1", "vtable2");
    verify(tableMetadataService).getNamespaceTableNames(eq(connection), eq(namespace));
    verify(virtualTableMetadataService).getNamespaceTableNames(eq(connection), eq(namespace));
  }

  @Test
  public void getNamespaceTableNames_OnlyRegularTablesExist_ShouldReturnRegularTablesOnly()
      throws Exception {
    // Arrange
    String namespace = "ns";

    when(dataSource.getConnection()).thenReturn(connection);

    Set<String> regularTables = new HashSet<>(Arrays.asList("table1", "table2"));
    Set<String> virtualTables = Collections.emptySet();

    when(tableMetadataService.getNamespaceTableNames(connection, namespace))
        .thenReturn(regularTables);
    when(virtualTableMetadataService.getNamespaceTableNames(connection, namespace))
        .thenReturn(virtualTables);

    JdbcAdmin admin = createJdbcAdmin();

    // Act
    Set<String> result = admin.getNamespaceTableNames(namespace);

    // Assert
    assertThat(result).containsExactlyInAnyOrder("table1", "table2");
    verify(tableMetadataService).getNamespaceTableNames(eq(connection), eq(namespace));
    verify(virtualTableMetadataService).getNamespaceTableNames(eq(connection), eq(namespace));
  }

  @Test
  public void getNamespaceTableNames_OnlyVirtualTablesExist_ShouldReturnVirtualTablesOnly()
      throws Exception {
    // Arrange
    String namespace = "ns";

    when(dataSource.getConnection()).thenReturn(connection);

    Set<String> regularTables = Collections.emptySet();
    Set<String> virtualTables = new HashSet<>(Arrays.asList("vtable1", "vtable2"));

    when(tableMetadataService.getNamespaceTableNames(connection, namespace))
        .thenReturn(regularTables);
    when(virtualTableMetadataService.getNamespaceTableNames(connection, namespace))
        .thenReturn(virtualTables);

    JdbcAdmin admin = createJdbcAdmin();

    // Act
    Set<String> result = admin.getNamespaceTableNames(namespace);

    // Assert
    assertThat(result).containsExactlyInAnyOrder("vtable1", "vtable2");
    verify(tableMetadataService).getNamespaceTableNames(eq(connection), eq(namespace));
    verify(virtualTableMetadataService).getNamespaceTableNames(eq(connection), eq(namespace));
  }

  @Test
  public void getNamespaceTableNames_NoTablesExist_ShouldReturnEmptySet() throws Exception {
    // Arrange
    String namespace = "ns";

    when(dataSource.getConnection()).thenReturn(connection);

    Set<String> regularTables = Collections.emptySet();
    Set<String> virtualTables = Collections.emptySet();

    when(tableMetadataService.getNamespaceTableNames(connection, namespace))
        .thenReturn(regularTables);
    when(virtualTableMetadataService.getNamespaceTableNames(connection, namespace))
        .thenReturn(virtualTables);

    JdbcAdmin admin = createJdbcAdmin();

    // Act
    Set<String> result = admin.getNamespaceTableNames(namespace);

    // Assert
    assertThat(result).isEmpty();
    verify(tableMetadataService).getNamespaceTableNames(eq(connection), eq(namespace));
    verify(virtualTableMetadataService).getNamespaceTableNames(eq(connection), eq(namespace));
  }

  @Test
  public void getNamespaceTableNames_SQLExceptionThrown_ShouldThrowExecutionException()
      throws Exception {
    // Arrange
    String namespace = "ns";

    when(dataSource.getConnection()).thenReturn(connection);

    SQLException sqlException = new SQLException("SQL error occurred");

    when(tableMetadataService.getNamespaceTableNames(connection, namespace))
        .thenThrow(sqlException);

    JdbcAdmin admin = createJdbcAdmin();

    // Act Assert
    assertThatThrownBy(() -> admin.getNamespaceTableNames(namespace))
        .isInstanceOf(ExecutionException.class)
        .hasCause(sqlException);
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

  @Test
  public void namespaceExists_forDb2WithExistingNamespace_shouldReturnTrue() throws Exception {
    namespaceExists_forXWithExistingNamespace_ShouldReturnTrue(
        RdbEngine.DB2, "SELECT 1 FROM syscat.schemata WHERE schemaname = ?", "");
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

    if (rdbEngine == RdbEngine.MYSQL || rdbEngine == RdbEngine.SQLITE) {
      verify(connection, never()).setReadOnly(anyBoolean());
    } else {
      verify(connection).setReadOnly(true);
    }
    verify(selectStatement).executeQuery();
    verify(connection).prepareStatement(expectedSelectStatement);
    verify(selectStatement).setString(1, namespace + namespacePlaceholderSuffix);
  }

  @Test
  public void namespaceExists_WithMetadataSchema_ShouldReturnTrue() throws ExecutionException {
    // Arrange
    JdbcAdmin adminForMySql = createJdbcAdminFor(RdbEngine.MYSQL);
    JdbcAdmin adminForPostgresql = createJdbcAdminFor(RdbEngine.POSTGRESQL);
    JdbcAdmin adminForOracle = createJdbcAdminFor(RdbEngine.ORACLE);
    JdbcAdmin adminForSqlServer = createJdbcAdminFor(RdbEngine.SQL_SERVER);
    JdbcAdmin adminForSqlite = createJdbcAdminFor(RdbEngine.SQLITE);
    JdbcAdmin adminForYugabyte = createJdbcAdminFor(RdbEngine.YUGABYTE);
    JdbcAdmin adminForDb2 = createJdbcAdminFor(RdbEngine.DB2);

    // Act Assert
    assertThat(adminForMySql.namespaceExists(METADATA_SCHEMA)).isTrue();
    assertThat(adminForPostgresql.namespaceExists(METADATA_SCHEMA)).isTrue();
    assertThat(adminForOracle.namespaceExists(METADATA_SCHEMA)).isTrue();
    assertThat(adminForSqlServer.namespaceExists(METADATA_SCHEMA)).isTrue();
    assertThat(adminForSqlite.namespaceExists(METADATA_SCHEMA)).isTrue();
    assertThat(adminForYugabyte.namespaceExists(METADATA_SCHEMA)).isTrue();
    assertThat(adminForDb2.namespaceExists(METADATA_SCHEMA)).isTrue();
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
        "CREATE INDEX \"my_ns\".\"index_my_ns_my_tbl_my_column\" ON \"my_ns\".\"my_tbl\" (\"my_column\")",
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

  @Test
  public void createIndex_ForColumnTypeWithoutRequiredAlterationForDb2_ShouldCreateIndexProperly()
      throws Exception {
    createIndex_ForColumnTypeWithoutRequiredAlterationForX_ShouldCreateIndexProperly(
        RdbEngine.DB2,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "CREATE INDEX \"my_ns\".\"index_my_ns_my_tbl_my_column\" ON \"my_ns\".\"my_tbl\" (\"my_column\")",
        "UPDATE \""
            + METADATA_SCHEMA
            + "\".\"metadata\" SET \"indexed\"=true WHERE \"full_table_name\"='my_ns.my_tbl' AND \"column_name\"='my_column'");
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
        new String[] {"ALTER TABLE `my_ns`.`my_tbl` MODIFY `my_column` VARCHAR(128)"},
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
        new String[] {
          "ALTER TABLE \"my_ns\".\"my_tbl\" ALTER COLUMN \"my_column\" TYPE VARCHAR(10485760)"
        },
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
        new String[] {"ALTER TABLE \"my_ns\".\"my_tbl\" MODIFY ( \"my_column\" VARCHAR2(128) )"},
        "CREATE INDEX \"my_ns\".\"index_my_ns_my_tbl_my_column\" ON \"my_ns\".\"my_tbl\" (\"my_column\")",
        "UPDATE \""
            + METADATA_SCHEMA
            + "\".\"metadata\" SET \"indexed\"=1 WHERE \"full_table_name\"='my_ns.my_tbl' AND \"column_name\"='my_column'");
  }

  @Test
  public void
      createIndex_forColumnTypeWithRequiredAlterationForSqlite_ShouldAlterColumnAndCreateIndexProperly() {
    // SQLite does not require column type change on CREATE INDEX.
  }

  @Test
  public void
      createIndex_forColumnTypeWithRequiredAlterationForDb2_ShouldAlterColumnAndCreateIndexProperly()
          throws Exception {
    createIndex_forColumnTypeWithRequiredAlterationForX_ShouldAlterColumnAndCreateIndexProperly(
        RdbEngine.DB2,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        new String[] {
          "ALTER TABLE \"my_ns\".\"my_tbl\" ALTER COLUMN \"my_column\" SET DATA TYPE VARCHAR(128)",
          "CALL SYSPROC.ADMIN_CMD('REORG TABLE \"my_ns\".\"my_tbl\"')"
        },
        "CREATE INDEX \"my_ns\".\"index_my_ns_my_tbl_my_column\" ON \"my_ns\".\"my_tbl\" (\"my_column\")",
        "UPDATE \""
            + METADATA_SCHEMA
            + "\".\"metadata\" SET \"indexed\"=true WHERE \"full_table_name\"='my_ns.my_tbl' AND \"column_name\"='my_column'");
  }

  private void
      createIndex_forColumnTypeWithRequiredAlterationForX_ShouldAlterColumnAndCreateIndexProperly(
          RdbEngine rdbEngine,
          String expectedGetTableMetadataStatement,
          String[] expectedAlterColumnTypeStatements,
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

    verify(connection, times(2 + expectedAlterColumnTypeStatements.length)).createStatement();
    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(statement, times(2 + expectedAlterColumnTypeStatements.length))
        .execute(captor.capture());
    for (int i = 0; i < expectedAlterColumnTypeStatements.length; i++) {
      assertThat(captor.getAllValues().get(i)).isEqualTo(expectedAlterColumnTypeStatements[i]);
    }
    assertThat(captor.getAllValues().get(expectedAlterColumnTypeStatements.length))
        .isEqualTo(expectedCreateIndexStatement);
    assertThat(captor.getAllValues().get(expectedAlterColumnTypeStatements.length + 1))
        .isEqualTo(expectedUpdateTableMetadataStatement);
  }

  @Test
  public void
      createIndex_VirtualTableWithColumnInLeftSourceTable_ShouldCreateIndexOnLeftSourceTable()
          throws Exception {
    // Arrange
    String namespace = "ns";
    String table = "vtable";
    String leftSourceNamespace = "ns";
    String leftSourceTable = "left_table";
    String rightSourceNamespace = "ns";
    String rightSourceTable = "right_table";
    String columnName = "col1";

    when(dataSource.getConnection()).thenReturn(connection);

    VirtualTableInfo virtualTableInfo = mock(VirtualTableInfo.class);
    when(virtualTableInfo.getLeftSourceNamespaceName()).thenReturn(leftSourceNamespace);
    when(virtualTableInfo.getLeftSourceTableName()).thenReturn(leftSourceTable);
    when(virtualTableInfo.getRightSourceNamespaceName()).thenReturn(rightSourceNamespace);
    when(virtualTableInfo.getRightSourceTableName()).thenReturn(rightSourceTable);

    when(virtualTableMetadataService.getVirtualTableInfo(connection, namespace, table))
        .thenReturn(virtualTableInfo);

    TableMetadata leftSourceTableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addColumn("pk1", DataType.TEXT)
            .addColumn("col1", DataType.INT)
            .build();

    TableMetadata rightSourceTableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addColumn("pk1", DataType.TEXT)
            .addColumn("col2", DataType.INT)
            .build();

    when(tableMetadataService.getTableMetadata(connection, leftSourceNamespace, leftSourceTable))
        .thenReturn(leftSourceTableMetadata);
    when(tableMetadataService.getTableMetadata(connection, rightSourceNamespace, rightSourceTable))
        .thenReturn(rightSourceTableMetadata);

    Statement statement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(statement);

    JdbcAdmin admin = createJdbcAdmin();

    // Act
    admin.createIndex(namespace, table, columnName, Collections.emptyMap());

    // Assert
    verify(virtualTableMetadataService)
        .getVirtualTableInfo(eq(connection), eq(namespace), eq(table));
    verify(tableMetadataService)
        .getTableMetadata(eq(connection), eq(leftSourceNamespace), eq(leftSourceTable));

    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(statement).execute(captor.capture());
    assertThat(captor.getValue())
        .isEqualTo("CREATE INDEX `index_ns_left_table_col1` ON `ns`.`left_table` (`col1`)");
    verify(tableMetadataService)
        .updateTableMetadata(
            eq(connection), eq(leftSourceNamespace), eq(leftSourceTable), eq(columnName), eq(true));
  }

  @Test
  public void
      createIndex_VirtualTableWithColumnInRightSourceTable_ShouldCreateIndexOnRightSourceTable()
          throws Exception {
    // Arrange
    String namespace = "ns";
    String table = "vtable";
    String leftSourceNamespace = "ns";
    String leftSourceTable = "left_table";
    String rightSourceNamespace = "ns";
    String rightSourceTable = "right_table";
    String columnName = "col2";

    when(dataSource.getConnection()).thenReturn(connection);

    VirtualTableInfo virtualTableInfo = mock(VirtualTableInfo.class);
    when(virtualTableInfo.getLeftSourceNamespaceName()).thenReturn(leftSourceNamespace);
    when(virtualTableInfo.getLeftSourceTableName()).thenReturn(leftSourceTable);
    when(virtualTableInfo.getRightSourceNamespaceName()).thenReturn(rightSourceNamespace);
    when(virtualTableInfo.getRightSourceTableName()).thenReturn(rightSourceTable);

    when(virtualTableMetadataService.getVirtualTableInfo(connection, namespace, table))
        .thenReturn(virtualTableInfo);

    TableMetadata leftSourceTableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addColumn("pk1", DataType.TEXT)
            .addColumn("col1", DataType.INT)
            .build();

    TableMetadata rightSourceTableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addColumn("pk1", DataType.TEXT)
            .addColumn("col2", DataType.INT)
            .build();

    when(tableMetadataService.getTableMetadata(connection, leftSourceNamespace, leftSourceTable))
        .thenReturn(leftSourceTableMetadata);
    when(tableMetadataService.getTableMetadata(connection, rightSourceNamespace, rightSourceTable))
        .thenReturn(rightSourceTableMetadata);

    Statement statement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(statement);

    JdbcAdmin admin = createJdbcAdmin();

    // Act
    admin.createIndex(namespace, table, columnName, Collections.emptyMap());

    // Assert
    verify(virtualTableMetadataService)
        .getVirtualTableInfo(eq(connection), eq(namespace), eq(table));
    verify(tableMetadataService)
        .getTableMetadata(eq(connection), eq(leftSourceNamespace), eq(leftSourceTable));
    verify(tableMetadataService)
        .getTableMetadata(eq(connection), eq(rightSourceNamespace), eq(rightSourceTable));

    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(statement).execute(captor.capture());
    assertThat(captor.getValue())
        .isEqualTo("CREATE INDEX `index_ns_right_table_col2` ON `ns`.`right_table` (`col2`)");
    verify(tableMetadataService)
        .updateTableMetadata(
            eq(connection),
            eq(rightSourceNamespace),
            eq(rightSourceTable),
            eq(columnName),
            eq(true));
  }

  @Test
  public void createIndex_VirtualTableWithPrimaryKeyColumn_ShouldCreateIndexOnBothSourceTables()
      throws Exception {
    // Arrange
    String namespace = "ns";
    String table = "vtable";
    String leftSourceNamespace = "ns";
    String leftSourceTable = "left_table";
    String rightSourceNamespace = "ns";
    String rightSourceTable = "right_table";
    String columnName = "pk1";

    when(dataSource.getConnection()).thenReturn(connection);

    VirtualTableInfo virtualTableInfo = mock(VirtualTableInfo.class);
    when(virtualTableInfo.getLeftSourceNamespaceName()).thenReturn(leftSourceNamespace);
    when(virtualTableInfo.getLeftSourceTableName()).thenReturn(leftSourceTable);
    when(virtualTableInfo.getRightSourceNamespaceName()).thenReturn(rightSourceNamespace);
    when(virtualTableInfo.getRightSourceTableName()).thenReturn(rightSourceTable);

    when(virtualTableMetadataService.getVirtualTableInfo(connection, namespace, table))
        .thenReturn(virtualTableInfo);

    TableMetadata leftSourceTableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addColumn("pk1", DataType.INT)
            .addColumn("col1", DataType.INT)
            .build();

    TableMetadata rightSourceTableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addColumn("pk1", DataType.INT)
            .addColumn("col2", DataType.INT)
            .build();

    when(tableMetadataService.getTableMetadata(connection, leftSourceNamespace, leftSourceTable))
        .thenReturn(leftSourceTableMetadata);
    when(tableMetadataService.getTableMetadata(connection, rightSourceNamespace, rightSourceTable))
        .thenReturn(rightSourceTableMetadata);

    Statement statement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(statement);

    JdbcAdmin admin = createJdbcAdmin();

    // Act
    admin.createIndex(namespace, table, columnName, Collections.emptyMap());

    // Assert
    verify(virtualTableMetadataService)
        .getVirtualTableInfo(eq(connection), eq(namespace), eq(table));
    verify(tableMetadataService)
        .getTableMetadata(eq(connection), eq(leftSourceNamespace), eq(leftSourceTable));
    verify(tableMetadataService)
        .getTableMetadata(eq(connection), eq(rightSourceNamespace), eq(rightSourceTable));

    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(statement, times(2)).execute(captor.capture());
    assertThat(captor.getAllValues())
        .containsExactly(
            "CREATE INDEX `index_ns_left_table_pk1` ON `ns`.`left_table` (`pk1`)",
            "CREATE INDEX `index_ns_right_table_pk1` ON `ns`.`right_table` (`pk1`)");
    verify(tableMetadataService)
        .updateTableMetadata(
            eq(connection), eq(leftSourceNamespace), eq(leftSourceTable), eq(columnName), eq(true));
    verify(tableMetadataService)
        .updateTableMetadata(
            eq(connection),
            eq(rightSourceNamespace),
            eq(rightSourceTable),
            eq(columnName),
            eq(true));
  }

  @Test
  public void createIndex_SQLExceptionThrown_ShouldThrowExecutionException() throws Exception {
    // Arrange
    String namespace = "ns";
    String table = "vtable";
    String columnName = "col1";

    when(dataSource.getConnection()).thenReturn(connection);

    SQLException sqlException = new SQLException("SQL error occurred");
    when(virtualTableMetadataService.getVirtualTableInfo(connection, namespace, table))
        .thenThrow(sqlException);

    JdbcAdmin admin = createJdbcAdmin();

    // Act Assert
    assertThatThrownBy(
            () -> admin.createIndex(namespace, table, columnName, Collections.emptyMap()))
        .isInstanceOf(ExecutionException.class)
        .hasMessageContaining("Creating the secondary index")
        .hasMessageContaining(columnName)
        .hasMessageContaining(getFullTableName(namespace, table))
        .hasCause(sqlException);
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
        "DROP INDEX \"my_ns\".\"index_my_ns_my_tbl_my_column\"",
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

  @Test
  public void dropIndex_forColumnTypeWithoutRequiredAlterationForDb2_ShouldDropIndexProperly()
      throws Exception {
    dropIndex_forColumnTypeWithoutRequiredAlterationForX_ShouldDropIndexProperly(
        RdbEngine.DB2,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "DROP INDEX \"my_ns\".\"index_my_ns_my_tbl_my_column\"",
        "UPDATE \""
            + METADATA_SCHEMA
            + "\".\"metadata\" SET \"indexed\"=false WHERE \"full_table_name\"='my_ns.my_tbl' AND \"column_name\"='my_column'");
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
        new String[] {"ALTER TABLE `my_ns`.`my_tbl` MODIFY `my_column` LONGTEXT"},
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
        new String[] {"ALTER TABLE \"my_ns\".\"my_tbl\" ALTER COLUMN \"my_column\" TYPE TEXT"},
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
        "DROP INDEX \"my_ns\".\"index_my_ns_my_tbl_my_column\"",
        new String[] {"ALTER TABLE \"my_ns\".\"my_tbl\" MODIFY ( \"my_column\" VARCHAR2(4000) )"},
        "UPDATE \""
            + METADATA_SCHEMA
            + "\".\"metadata\" SET \"indexed\"=0 WHERE \"full_table_name\"='my_ns.my_tbl' AND \"column_name\"='my_column'");
  }

  @Test
  public void dropIndex_forColumnTypeWithRequiredAlterationForSqlite_ShouldDropIndexProperly() {
    // SQLite does not require column type change on CREATE INDEX.
  }

  @Test
  public void dropIndex_forColumnTypeWithRequiredAlterationForDb2_ShouldDropIndexProperly()
      throws Exception {
    dropIndex_forColumnTypeWithRequiredAlterationForX_ShouldDropIndexProperly(
        RdbEngine.DB2,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "DROP INDEX \"my_ns\".\"index_my_ns_my_tbl_my_column\"",
        new String[] {
          "ALTER TABLE \"my_ns\".\"my_tbl\" ALTER COLUMN \"my_column\" SET DATA TYPE VARCHAR(32672)",
          "CALL SYSPROC.ADMIN_CMD('REORG TABLE \"my_ns\".\"my_tbl\"')"
        },
        "UPDATE \""
            + METADATA_SCHEMA
            + "\".\"metadata\" SET \"indexed\"=false WHERE \"full_table_name\"='my_ns.my_tbl' AND \"column_name\"='my_column'");
  }

  private void dropIndex_forColumnTypeWithRequiredAlterationForX_ShouldDropIndexProperly(
      RdbEngine rdbEngine,
      String expectedGetTableMetadataStatement,
      String expectedDropIndexStatement,
      String[] expectedAlterColumnStatements,
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

    verify(connection, times(2 + expectedAlterColumnStatements.length)).createStatement();
    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(statement, times(2 + expectedAlterColumnStatements.length)).execute(captor.capture());
    assertThat(captor.getAllValues().get(0)).isEqualTo(expectedDropIndexStatement);
    for (int i = 0; i < expectedAlterColumnStatements.length; i++) {
      assertThat(captor.getAllValues().get(i + 1)).isEqualTo(expectedAlterColumnStatements[i]);
    }
    assertThat(captor.getAllValues().get(expectedAlterColumnStatements.length + 1))
        .isEqualTo(expectedUpdateTableMetadataStatement);
  }

  @Test
  public void dropIndex_VirtualTableWithColumnInLeftSourceTable_ShouldDropIndexOnLeftSourceTable()
      throws Exception {
    // Arrange
    String namespace = "ns";
    String table = "vtable";
    String leftSourceNamespace = "ns";
    String leftSourceTable = "left_table";
    String rightSourceNamespace = "ns";
    String rightSourceTable = "right_table";
    String columnName = "col1";

    when(dataSource.getConnection()).thenReturn(connection);

    VirtualTableInfo virtualTableInfo = mock(VirtualTableInfo.class);
    when(virtualTableInfo.getLeftSourceNamespaceName()).thenReturn(leftSourceNamespace);
    when(virtualTableInfo.getLeftSourceTableName()).thenReturn(leftSourceTable);
    when(virtualTableInfo.getRightSourceNamespaceName()).thenReturn(rightSourceNamespace);
    when(virtualTableInfo.getRightSourceTableName()).thenReturn(rightSourceTable);

    when(virtualTableMetadataService.getVirtualTableInfo(connection, namespace, table))
        .thenReturn(virtualTableInfo);

    TableMetadata leftSourceTableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addColumn("pk1", DataType.TEXT)
            .addColumn("col1", DataType.INT)
            .build();

    TableMetadata rightSourceTableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addColumn("pk1", DataType.TEXT)
            .addColumn("col2", DataType.INT)
            .build();

    when(tableMetadataService.getTableMetadata(connection, leftSourceNamespace, leftSourceTable))
        .thenReturn(leftSourceTableMetadata);
    when(tableMetadataService.getTableMetadata(connection, rightSourceNamespace, rightSourceTable))
        .thenReturn(rightSourceTableMetadata);

    Statement statement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(statement);

    JdbcAdmin admin = createJdbcAdmin();

    // Act
    admin.dropIndex(namespace, table, columnName);

    // Assert
    verify(virtualTableMetadataService)
        .getVirtualTableInfo(eq(connection), eq(namespace), eq(table));
    verify(tableMetadataService)
        .getTableMetadata(eq(connection), eq(leftSourceNamespace), eq(leftSourceTable));

    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(statement).execute(captor.capture());
    assertThat(captor.getValue())
        .isEqualTo("DROP INDEX `index_ns_left_table_col1` ON `ns`.`left_table`");
    verify(tableMetadataService)
        .updateTableMetadata(
            eq(connection),
            eq(leftSourceNamespace),
            eq(leftSourceTable),
            eq(columnName),
            eq(false));
  }

  @Test
  public void dropIndex_VirtualTableWithColumnInRightSourceTable_ShouldDropIndexOnRightSourceTable()
      throws Exception {
    // Arrange
    String namespace = "ns";
    String table = "vtable";
    String leftSourceNamespace = "ns";
    String leftSourceTable = "left_table";
    String rightSourceNamespace = "ns";
    String rightSourceTable = "right_table";
    String columnName = "col2";

    when(dataSource.getConnection()).thenReturn(connection);

    VirtualTableInfo virtualTableInfo = mock(VirtualTableInfo.class);
    when(virtualTableInfo.getLeftSourceNamespaceName()).thenReturn(leftSourceNamespace);
    when(virtualTableInfo.getLeftSourceTableName()).thenReturn(leftSourceTable);
    when(virtualTableInfo.getRightSourceNamespaceName()).thenReturn(rightSourceNamespace);
    when(virtualTableInfo.getRightSourceTableName()).thenReturn(rightSourceTable);

    when(virtualTableMetadataService.getVirtualTableInfo(connection, namespace, table))
        .thenReturn(virtualTableInfo);

    TableMetadata leftSourceTableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addColumn("pk1", DataType.TEXT)
            .addColumn("col1", DataType.INT)
            .build();

    TableMetadata rightSourceTableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addColumn("pk1", DataType.TEXT)
            .addColumn("col2", DataType.INT)
            .build();

    when(tableMetadataService.getTableMetadata(connection, leftSourceNamespace, leftSourceTable))
        .thenReturn(leftSourceTableMetadata);
    when(tableMetadataService.getTableMetadata(connection, rightSourceNamespace, rightSourceTable))
        .thenReturn(rightSourceTableMetadata);

    Statement statement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(statement);

    JdbcAdmin admin = createJdbcAdmin();

    // Act
    admin.dropIndex(namespace, table, columnName);

    // Assert
    verify(virtualTableMetadataService)
        .getVirtualTableInfo(eq(connection), eq(namespace), eq(table));
    verify(tableMetadataService)
        .getTableMetadata(eq(connection), eq(leftSourceNamespace), eq(leftSourceTable));
    verify(tableMetadataService)
        .getTableMetadata(eq(connection), eq(rightSourceNamespace), eq(rightSourceTable));

    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(statement).execute(captor.capture());
    assertThat(captor.getValue())
        .isEqualTo("DROP INDEX `index_ns_right_table_col2` ON `ns`.`right_table`");
    verify(tableMetadataService)
        .updateTableMetadata(
            eq(connection),
            eq(rightSourceNamespace),
            eq(rightSourceTable),
            eq(columnName),
            eq(false));
  }

  @Test
  public void dropIndex_VirtualTableWithPrimaryKeyColumn_ShouldDropIndexOnBothSourceTables()
      throws Exception {
    // Arrange
    String namespace = "ns";
    String table = "vtable";
    String leftSourceNamespace = "ns";
    String leftSourceTable = "left_table";
    String rightSourceNamespace = "ns";
    String rightSourceTable = "right_table";
    String columnName = "pk1";

    when(dataSource.getConnection()).thenReturn(connection);

    VirtualTableInfo virtualTableInfo = mock(VirtualTableInfo.class);
    when(virtualTableInfo.getLeftSourceNamespaceName()).thenReturn(leftSourceNamespace);
    when(virtualTableInfo.getLeftSourceTableName()).thenReturn(leftSourceTable);
    when(virtualTableInfo.getRightSourceNamespaceName()).thenReturn(rightSourceNamespace);
    when(virtualTableInfo.getRightSourceTableName()).thenReturn(rightSourceTable);

    when(virtualTableMetadataService.getVirtualTableInfo(connection, namespace, table))
        .thenReturn(virtualTableInfo);

    TableMetadata leftSourceTableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addColumn("pk1", DataType.INT)
            .addColumn("col1", DataType.INT)
            .build();

    TableMetadata rightSourceTableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addColumn("pk1", DataType.INT)
            .addColumn("col2", DataType.INT)
            .build();

    when(tableMetadataService.getTableMetadata(connection, leftSourceNamespace, leftSourceTable))
        .thenReturn(leftSourceTableMetadata);
    when(tableMetadataService.getTableMetadata(connection, rightSourceNamespace, rightSourceTable))
        .thenReturn(rightSourceTableMetadata);

    Statement statement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(statement);

    JdbcAdmin admin = createJdbcAdmin();

    // Act
    admin.dropIndex(namespace, table, columnName);

    // Assert
    verify(virtualTableMetadataService)
        .getVirtualTableInfo(eq(connection), eq(namespace), eq(table));
    verify(tableMetadataService)
        .getTableMetadata(eq(connection), eq(leftSourceNamespace), eq(leftSourceTable));
    verify(tableMetadataService)
        .getTableMetadata(eq(connection), eq(rightSourceNamespace), eq(rightSourceTable));

    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(statement, times(2)).execute(captor.capture());
    assertThat(captor.getAllValues())
        .containsExactly(
            "DROP INDEX `index_ns_left_table_pk1` ON `ns`.`left_table`",
            "DROP INDEX `index_ns_right_table_pk1` ON `ns`.`right_table`");
    verify(tableMetadataService)
        .updateTableMetadata(
            eq(connection),
            eq(leftSourceNamespace),
            eq(leftSourceTable),
            eq(columnName),
            eq(false));
    verify(tableMetadataService)
        .updateTableMetadata(
            eq(connection),
            eq(rightSourceNamespace),
            eq(rightSourceTable),
            eq(columnName),
            eq(false));
  }

  @Test
  public void dropIndex_VirtualTableWithSQLException_ShouldThrowExecutionException()
      throws Exception {
    // Arrange
    String namespace = "ns";
    String table = "vtable";
    String columnName = "col1";

    when(dataSource.getConnection()).thenReturn(connection);

    SQLException sqlException = new SQLException("SQL error occurred");
    when(virtualTableMetadataService.getVirtualTableInfo(connection, namespace, table))
        .thenThrow(sqlException);

    JdbcAdmin admin = createJdbcAdmin();

    // Act Assert
    assertThatThrownBy(() -> admin.dropIndex(namespace, table, columnName))
        .isInstanceOf(ExecutionException.class)
        .hasMessageContaining("Dropping the secondary index")
        .hasMessageContaining(columnName)
        .hasMessageContaining(getFullTableName(namespace, table))
        .hasCause(sqlException);
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

  @Test
  public void addNewColumnToTable_ForDb2_ShouldWorkProperly()
      throws SQLException, ExecutionException {
    addNewColumnToTable_ForX_ShouldWorkProperly(
        RdbEngine.DB2,
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
  public void dropColumnFromTable_ForMysql_ShouldWorkProperly()
      throws SQLException, ExecutionException {
    dropColumnFromTable_ForX_ShouldWorkProperly(
        RdbEngine.MYSQL,
        "SELECT `column_name`,`data_type`,`key_type`,`clustering_order`,`indexed` FROM `"
            + METADATA_SCHEMA
            + "`.`metadata` WHERE `full_table_name`=? ORDER BY `ordinal_position` ASC",
        "ALTER TABLE `ns`.`table` DROP COLUMN `c2`",
        "DELETE FROM `" + METADATA_SCHEMA + "`.`metadata` WHERE `full_table_name` = 'ns.table'",
        "INSERT INTO `"
            + METADATA_SCHEMA
            + "`.`metadata` VALUES ('ns.table','c1','TEXT','PARTITION',NULL,false,1)");
  }

  @Test
  public void dropColumnFromTable_ForOracle_ShouldWorkProperly()
      throws SQLException, ExecutionException {
    dropColumnFromTable_ForX_ShouldWorkProperly(
        RdbEngine.ORACLE,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "ALTER TABLE \"ns\".\"table\" DROP COLUMN \"c2\"",
        "DELETE FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\" = 'ns.table'",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('ns.table','c1','TEXT','PARTITION',NULL,0,1)");
  }

  @Test
  public void dropColumnFromTable_ForPostgresql_ShouldWorkProperly()
      throws SQLException, ExecutionException {
    dropColumnFromTable_ForX_ShouldWorkProperly(
        RdbEngine.POSTGRESQL,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "ALTER TABLE \"ns\".\"table\" DROP COLUMN \"c2\"",
        "DELETE FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\" = 'ns.table'",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('ns.table','c1','TEXT','PARTITION',NULL,false,1)");
  }

  @Test
  public void dropColumnFromTable_ForSqlServer_ShouldWorkProperly()
      throws SQLException, ExecutionException {
    dropColumnFromTable_ForX_ShouldWorkProperly(
        RdbEngine.SQL_SERVER,
        "SELECT [column_name],[data_type],[key_type],[clustering_order],[indexed] FROM ["
            + METADATA_SCHEMA
            + "].[metadata] WHERE [full_table_name]=? ORDER BY [ordinal_position] ASC",
        "ALTER TABLE [ns].[table] DROP COLUMN [c2]",
        "DELETE FROM [" + METADATA_SCHEMA + "].[metadata] WHERE [full_table_name] = 'ns.table'",
        "INSERT INTO ["
            + METADATA_SCHEMA
            + "].[metadata] VALUES ('ns.table','c1','TEXT','PARTITION',NULL,0,1)");
  }

  @Test
  public void dropColumnFromTable_ForSqlite_ShouldWorkProperly()
      throws SQLException, ExecutionException {
    dropColumnFromTable_ForX_ShouldWorkProperly(
        RdbEngine.SQLITE,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + METADATA_SCHEMA
            + "$metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "ALTER TABLE \"ns$table\" DROP COLUMN \"c2\"",
        "DELETE FROM \"" + METADATA_SCHEMA + "$metadata\" WHERE \"full_table_name\" = 'ns.table'",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "$metadata\" VALUES ('ns.table','c1','TEXT','PARTITION',NULL,FALSE,1)");
  }

  @Test
  public void dropColumnFromTable_ForDb2_ShouldWorkProperly()
      throws SQLException, ExecutionException {
    dropColumnFromTable_ForX_ShouldWorkProperly(
        RdbEngine.DB2,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "ALTER TABLE \"ns\".\"table\" DROP COLUMN \"c2\"",
        "CALL SYSPROC.ADMIN_CMD('REORG TABLE \"ns\".\"table\"')",
        "DELETE FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\" = 'ns.table'",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('ns.table','c1','TEXT','PARTITION',NULL,false,1)");
  }

  private void dropColumnFromTable_ForX_ShouldWorkProperly(
      RdbEngine rdbEngine, String expectedGetMetadataStatement, String... expectedSqlStatements)
      throws SQLException, ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "table";
    String column1 = "c1";
    String column2 = "c2";

    PreparedStatement selectStatement = mock(PreparedStatement.class);
    ResultSet resultSet =
        mockResultSet(
            new SelectAllFromMetadataTableResultSetMocker.Row(
                column1, DataType.TEXT.toString(), "PARTITION", null, false),
            new SelectAllFromMetadataTableResultSetMocker.Row(
                column2, DataType.INT.toString(), null, null, false));
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
    admin.dropColumnFromTable(namespace, table, column2);

    // Assert
    verify(selectStatement).setString(1, getFullTableName(namespace, table));
    verify(connection).prepareStatement(expectedGetMetadataStatement);
    for (int i = 0; i < expectedSqlStatements.length; i++) {
      verify(expectedStatements.get(i)).execute(expectedSqlStatements[i]);
    }
  }

  @Test
  public void renameColumn_ForMysql_ShouldWorkProperly() throws SQLException, ExecutionException {
    renameColumn_ForX_ShouldWorkProperly(
        RdbEngine.MYSQL,
        "SELECT `column_name`,`data_type`,`key_type`,`clustering_order`,`indexed` FROM `"
            + METADATA_SCHEMA
            + "`.`metadata` WHERE `full_table_name`=? ORDER BY `ordinal_position` ASC",
        "ALTER TABLE `ns`.`table` CHANGE COLUMN `c2` `c3` INT",
        "DELETE FROM `" + METADATA_SCHEMA + "`.`metadata` WHERE `full_table_name` = 'ns.table'",
        "INSERT INTO `"
            + METADATA_SCHEMA
            + "`.`metadata` VALUES ('ns.table','c1','TEXT','PARTITION',NULL,false,1)",
        "INSERT INTO `"
            + METADATA_SCHEMA
            + "`.`metadata` VALUES ('ns.table','c3','INT',NULL,NULL,false,2)");
  }

  @Test
  public void renameColumn_ForOracle_ShouldWorkProperly() throws SQLException, ExecutionException {
    renameColumn_ForX_ShouldWorkProperly(
        RdbEngine.ORACLE,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "ALTER TABLE \"ns\".\"table\" RENAME COLUMN \"c2\" TO \"c3\"",
        "DELETE FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\" = 'ns.table'",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('ns.table','c1','TEXT','PARTITION',NULL,0,1)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('ns.table','c3','INT',NULL,NULL,0,2)");
  }

  @Test
  public void renameColumn_ForPostgresql_ShouldWorkProperly()
      throws SQLException, ExecutionException {
    renameColumn_ForX_ShouldWorkProperly(
        RdbEngine.POSTGRESQL,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "ALTER TABLE \"ns\".\"table\" RENAME COLUMN \"c2\" TO \"c3\"",
        "DELETE FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\" = 'ns.table'",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('ns.table','c1','TEXT','PARTITION',NULL,false,1)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('ns.table','c3','INT',NULL,NULL,false,2)");
  }

  @Test
  public void renameColumn_ForSqlServer_ShouldWorkProperly()
      throws SQLException, ExecutionException {
    renameColumn_ForX_ShouldWorkProperly(
        RdbEngine.SQL_SERVER,
        "SELECT [column_name],[data_type],[key_type],[clustering_order],[indexed] FROM ["
            + METADATA_SCHEMA
            + "].[metadata] WHERE [full_table_name]=? ORDER BY [ordinal_position] ASC",
        "EXEC sp_rename '[ns].[table].[c2]', 'c3', 'COLUMN'",
        "DELETE FROM [" + METADATA_SCHEMA + "].[metadata] WHERE [full_table_name] = 'ns.table'",
        "INSERT INTO ["
            + METADATA_SCHEMA
            + "].[metadata] VALUES ('ns.table','c1','TEXT','PARTITION',NULL,0,1)",
        "INSERT INTO ["
            + METADATA_SCHEMA
            + "].[metadata] VALUES ('ns.table','c3','INT',NULL,NULL,0,2)");
  }

  @Test
  public void renameColumn_ForSqlite_ShouldWorkProperly() throws SQLException, ExecutionException {
    renameColumn_ForX_ShouldWorkProperly(
        RdbEngine.SQLITE,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + METADATA_SCHEMA
            + "$metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "ALTER TABLE \"ns$table\" RENAME COLUMN \"c2\" TO \"c3\"",
        "DELETE FROM \"" + METADATA_SCHEMA + "$metadata\" WHERE \"full_table_name\" = 'ns.table'",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "$metadata\" VALUES ('ns.table','c1','TEXT','PARTITION',NULL,FALSE,1)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "$metadata\" VALUES ('ns.table','c3','INT',NULL,NULL,FALSE,2)");
  }

  @Test
  public void renameColumn_ForDb2_ShouldWorkProperly() throws SQLException, ExecutionException {
    renameColumn_ForX_ShouldWorkProperly(
        RdbEngine.DB2,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "ALTER TABLE \"ns\".\"table\" RENAME COLUMN \"c2\" TO \"c3\"",
        "DELETE FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\" = 'ns.table'",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('ns.table','c1','TEXT','PARTITION',NULL,false,1)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('ns.table','c3','INT',NULL,NULL,false,2)");
  }

  private void renameColumn_ForX_ShouldWorkProperly(
      RdbEngine rdbEngine, String expectedGetMetadataStatement, String... expectedSqlStatements)
      throws SQLException, ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "table";
    String columnName1 = "c1";
    String columnName2 = "c2";
    String columnName3 = "c3";

    PreparedStatement selectStatement = mock(PreparedStatement.class);
    ResultSet resultSet =
        mockResultSet(
            new SelectAllFromMetadataTableResultSetMocker.Row(
                columnName1, DataType.TEXT.toString(), "PARTITION", null, false),
            new SelectAllFromMetadataTableResultSetMocker.Row(
                columnName2, DataType.INT.toString(), null, null, false));
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
    admin.renameColumn(namespace, table, columnName2, columnName3);

    // Assert
    verify(selectStatement).setString(1, getFullTableName(namespace, table));
    verify(connection).prepareStatement(expectedGetMetadataStatement);
    for (int i = 0; i < expectedSqlStatements.length; i++) {
      verify(expectedStatements.get(i)).execute(expectedSqlStatements[i]);
    }
  }

  @Test
  public void alterColumnType_ForMysql_ShouldWorkProperly()
      throws SQLException, ExecutionException {
    alterColumnType_ForX_ShouldWorkProperly(
        RdbEngine.MYSQL,
        "SELECT `column_name`,`data_type`,`key_type`,`clustering_order`,`indexed` FROM `"
            + METADATA_SCHEMA
            + "`.`metadata` WHERE `full_table_name`=? ORDER BY `ordinal_position` ASC",
        "ALTER TABLE `ns`.`table` MODIFY `c2` BIGINT",
        "DELETE FROM `" + METADATA_SCHEMA + "`.`metadata` WHERE `full_table_name` = 'ns.table'",
        "INSERT INTO `"
            + METADATA_SCHEMA
            + "`.`metadata` VALUES ('ns.table','c1','TEXT','PARTITION',NULL,false,1)",
        "INSERT INTO `"
            + METADATA_SCHEMA
            + "`.`metadata` VALUES ('ns.table','c2','BIGINT',NULL,NULL,false,2)");
  }

  @Test
  public void alterColumnType_ForOracle_ShouldWorkProperly()
      throws SQLException, ExecutionException {
    alterColumnType_ForX_ShouldWorkProperly(
        RdbEngine.ORACLE,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "ALTER TABLE \"ns\".\"table\" MODIFY ( \"c2\" NUMBER(16) )",
        "DELETE FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\" = 'ns.table'",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('ns.table','c1','TEXT','PARTITION',NULL,0,1)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('ns.table','c2','BIGINT',NULL,NULL,0,2)");
  }

  @Test
  public void alterColumnType_ForPostgresql_ShouldWorkProperly()
      throws SQLException, ExecutionException {
    alterColumnType_ForX_ShouldWorkProperly(
        RdbEngine.POSTGRESQL,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "ALTER TABLE \"ns\".\"table\" ALTER COLUMN \"c2\" TYPE BIGINT",
        "DELETE FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\" = 'ns.table'",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('ns.table','c1','TEXT','PARTITION',NULL,false,1)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('ns.table','c2','BIGINT',NULL,NULL,false,2)");
  }

  @Test
  public void alterColumnType_ForSqlServer_ShouldWorkProperly()
      throws SQLException, ExecutionException {
    alterColumnType_ForX_ShouldWorkProperly(
        RdbEngine.SQL_SERVER,
        "SELECT [column_name],[data_type],[key_type],[clustering_order],[indexed] FROM ["
            + METADATA_SCHEMA
            + "].[metadata] WHERE [full_table_name]=? ORDER BY [ordinal_position] ASC",
        "ALTER TABLE [ns].[table] ALTER COLUMN [c2] BIGINT",
        "DELETE FROM [" + METADATA_SCHEMA + "].[metadata] WHERE [full_table_name] = 'ns.table'",
        "INSERT INTO ["
            + METADATA_SCHEMA
            + "].[metadata] VALUES ('ns.table','c1','TEXT','PARTITION',NULL,0,1)",
        "INSERT INTO ["
            + METADATA_SCHEMA
            + "].[metadata] VALUES ('ns.table','c2','BIGINT',NULL,NULL,0,2)");
  }

  @Test
  public void alterColumnType_ForSqlite_ShouldThrowUnsupportedOperationException()
      throws SQLException {
    // Arrange
    String namespace = "ns";
    String table = "table";
    String columnName1 = "c1";
    String columnName2 = "c2";

    PreparedStatement selectStatement = mock(PreparedStatement.class);
    ResultSet resultSet =
        mockResultSet(
            new SelectAllFromMetadataTableResultSetMocker.Row(
                columnName1, DataType.TEXT.toString(), "PARTITION", null, false),
            new SelectAllFromMetadataTableResultSetMocker.Row(
                columnName2, DataType.INT.toString(), null, null, false));
    when(selectStatement.executeQuery()).thenReturn(resultSet);
    when(connection.prepareStatement(any())).thenReturn(selectStatement);
    when(dataSource.getConnection()).thenReturn(connection);
    JdbcAdmin admin = createJdbcAdminFor(RdbEngine.SQLITE);

    // Act Assert
    assertThatThrownBy(() -> admin.alterColumnType(namespace, table, columnName1, DataType.BIGINT))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage(CoreError.JDBC_SQLITE_ALTER_COLUMN_TYPE_NOT_SUPPORTED.buildMessage());
  }

  @Test
  public void alterColumnType_ForDb2_ShouldWorkProperly() throws SQLException, ExecutionException {
    alterColumnType_ForX_ShouldWorkProperly(
        RdbEngine.DB2,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "ALTER TABLE \"ns\".\"table\" ALTER COLUMN \"c2\" SET DATA TYPE BIGINT",
        "CALL SYSPROC.ADMIN_CMD('REORG TABLE \"ns\".\"table\"')",
        "DELETE FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\" = 'ns.table'",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('ns.table','c1','TEXT','PARTITION',NULL,false,1)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('ns.table','c2','BIGINT',NULL,NULL,false,2)");
  }

  private void alterColumnType_ForX_ShouldWorkProperly(
      RdbEngine rdbEngine, String expectedGetMetadataStatement, String... expectedSqlStatements)
      throws SQLException, ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "table";
    String columnName1 = "c1";
    String columnName2 = "c2";

    PreparedStatement selectStatement = mock(PreparedStatement.class);
    ResultSet resultSet =
        mockResultSet(
            new SelectAllFromMetadataTableResultSetMocker.Row(
                columnName1, DataType.TEXT.toString(), "PARTITION", null, false),
            new SelectAllFromMetadataTableResultSetMocker.Row(
                columnName2, DataType.INT.toString(), null, null, false));
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
    admin.alterColumnType(namespace, table, columnName2, DataType.BIGINT);

    // Assert
    verify(selectStatement).setString(1, getFullTableName(namespace, table));
    verify(connection).prepareStatement(expectedGetMetadataStatement);
    for (int i = 0; i < expectedSqlStatements.length; i++) {
      verify(expectedStatements.get(i)).execute(expectedSqlStatements[i]);
    }
  }

  @Test
  public void renameTable_ForMysql_ShouldWorkProperly() throws SQLException, ExecutionException {
    renameTable_ForX_ShouldWorkProperly(
        RdbEngine.MYSQL,
        "SELECT `column_name`,`data_type`,`key_type`,`clustering_order`,`indexed` FROM `"
            + METADATA_SCHEMA
            + "`.`metadata` WHERE `full_table_name`=? ORDER BY `ordinal_position` ASC",
        "ALTER TABLE `ns`.`table` RENAME TO `ns`.`table_new`",
        "DELETE FROM `" + METADATA_SCHEMA + "`.`metadata` WHERE `full_table_name` = 'ns.table'",
        "INSERT INTO `"
            + METADATA_SCHEMA
            + "`.`metadata` VALUES ('ns.table_new','c1','TEXT','PARTITION',NULL,false,1)",
        "INSERT INTO `"
            + METADATA_SCHEMA
            + "`.`metadata` VALUES ('ns.table_new','c2','INT',NULL,NULL,false,2)");
  }

  @Test
  public void renameTable_ForOracle_ShouldWorkProperly() throws SQLException, ExecutionException {
    renameTable_ForX_ShouldWorkProperly(
        RdbEngine.ORACLE,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "ALTER TABLE \"ns\".\"table\" RENAME TO \"table_new\"",
        "DELETE FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\" = 'ns.table'",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('ns.table_new','c1','TEXT','PARTITION',NULL,0,1)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('ns.table_new','c2','INT',NULL,NULL,0,2)");
  }

  @Test
  public void renameTable_ForPostgresql_ShouldWorkProperly()
      throws SQLException, ExecutionException {
    renameTable_ForX_ShouldWorkProperly(
        RdbEngine.POSTGRESQL,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "ALTER TABLE \"ns\".\"table\" RENAME TO \"table_new\"",
        "DELETE FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\" = 'ns.table'",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('ns.table_new','c1','TEXT','PARTITION',NULL,false,1)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('ns.table_new','c2','INT',NULL,NULL,false,2)");
  }

  @Test
  public void renameTable_ForSqlServer_ShouldWorkProperly()
      throws SQLException, ExecutionException {
    renameTable_ForX_ShouldWorkProperly(
        RdbEngine.SQL_SERVER,
        "SELECT [column_name],[data_type],[key_type],[clustering_order],[indexed] FROM ["
            + METADATA_SCHEMA
            + "].[metadata] WHERE [full_table_name]=? ORDER BY [ordinal_position] ASC",
        "EXEC sp_rename '[ns].[table]', 'table_new'",
        "DELETE FROM [" + METADATA_SCHEMA + "].[metadata] WHERE [full_table_name] = 'ns.table'",
        "INSERT INTO ["
            + METADATA_SCHEMA
            + "].[metadata] VALUES ('ns.table_new','c1','TEXT','PARTITION',NULL,0,1)",
        "INSERT INTO ["
            + METADATA_SCHEMA
            + "].[metadata] VALUES ('ns.table_new','c2','INT',NULL,NULL,0,2)");
  }

  @Test
  public void renameTable_ForSqlite_ShouldWorkProperly() throws SQLException, ExecutionException {
    renameTable_ForX_ShouldWorkProperly(
        RdbEngine.SQLITE,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + METADATA_SCHEMA
            + "$metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "ALTER TABLE \"ns$table\" RENAME TO \"ns$table_new\"",
        "DELETE FROM \"" + METADATA_SCHEMA + "$metadata\" WHERE \"full_table_name\" = 'ns.table'",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "$metadata\" VALUES ('ns.table_new','c1','TEXT','PARTITION',NULL,FALSE,1)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "$metadata\" VALUES ('ns.table_new','c2','INT',NULL,NULL,FALSE,2)");
  }

  @Test
  public void renameTable_ForDb2_ShouldWorkProperly() throws SQLException, ExecutionException {
    renameTable_ForX_ShouldWorkProperly(
        RdbEngine.DB2,
        "SELECT \"column_name\",\"data_type\",\"key_type\",\"clustering_order\",\"indexed\" FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\"=? ORDER BY \"ordinal_position\" ASC",
        "RENAME \"ns\".\"table\" TO \"table_new\"",
        "DELETE FROM \""
            + METADATA_SCHEMA
            + "\".\"metadata\" WHERE \"full_table_name\" = 'ns.table'",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('ns.table_new','c1','TEXT','PARTITION',NULL,false,1)",
        "INSERT INTO \""
            + METADATA_SCHEMA
            + "\".\"metadata\" VALUES ('ns.table_new','c2','INT',NULL,NULL,false,2)");
  }

  private void renameTable_ForX_ShouldWorkProperly(
      RdbEngine rdbEngine, String expectedGetMetadataStatement, String... expectedSqlStatements)
      throws SQLException, ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "table";
    String columnName1 = "c1";
    String columnName2 = "c2";

    PreparedStatement selectStatement = mock(PreparedStatement.class);
    ResultSet resultSet1 =
        mockResultSet(
            new SelectAllFromMetadataTableResultSetMocker.Row(
                columnName1, DataType.TEXT.toString(), "PARTITION", null, false),
            new SelectAllFromMetadataTableResultSetMocker.Row(
                columnName2, DataType.INT.toString(), null, null, false));
    when(selectStatement.executeQuery()).thenReturn(resultSet1);
    when(connection.prepareStatement(any())).thenReturn(selectStatement);
    List<Statement> expectedStatements = new ArrayList<>();
    for (String expectedSqlStatement : expectedSqlStatements) {
      Statement mock = mock(Statement.class);
      expectedStatements.add(mock);
      if (expectedSqlStatement.startsWith("SELECT DISTINCT ")) {
        ResultSet resultSet2 = mock(ResultSet.class);
        when(resultSet2.next()).thenReturn(true);
        when(mock.executeQuery(any())).thenReturn(resultSet2);
      }
    }
    when(connection.createStatement())
        .thenReturn(
            expectedStatements.get(0),
            expectedStatements.subList(1, expectedStatements.size()).toArray(new Statement[0]));

    when(dataSource.getConnection()).thenReturn(connection);
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    // Act
    admin.renameTable(namespace, table, "table_new");

    // Assert
    verify(selectStatement).setString(1, getFullTableName(namespace, table));
    verify(connection).prepareStatement(expectedGetMetadataStatement);
    for (int i = 0; i < expectedSqlStatements.length; i++) {
      if (expectedSqlStatements[i].startsWith("SELECT DISTINCT ")) {
        verify(expectedStatements.get(i)).executeQuery(expectedSqlStatements[i]);
      } else {
        verify(expectedStatements.get(i)).execute(expectedSqlStatements[i]);
      }
    }
  }

  @Test
  public void getNamespaceNames_ForMysql_WithExistingTables_ShouldWorkProperly()
      throws SQLException, ExecutionException {
    getNamespaceNames_ForX_WithExistingTables_ShouldWorkProperly(
        RdbEngine.MYSQL,
        "SELECT DISTINCT `full_table_name` FROM `" + METADATA_SCHEMA + "`.`metadata`");
  }

  @Test
  public void getNamespaceNames_Posgresql_WithExistingTables_ShouldWorkProperly()
      throws SQLException, ExecutionException {
    getNamespaceNames_ForX_WithExistingTables_ShouldWorkProperly(
        RdbEngine.POSTGRESQL,
        "SELECT DISTINCT \"full_table_name\" FROM \"" + METADATA_SCHEMA + "\".\"metadata\"");
  }

  @Test
  public void getNamespaceNames_Oracle_WithExistingTables_ShouldWorkProperly()
      throws SQLException, ExecutionException {
    getNamespaceNames_ForX_WithExistingTables_ShouldWorkProperly(
        RdbEngine.ORACLE,
        "SELECT DISTINCT \"full_table_name\" FROM \"" + METADATA_SCHEMA + "\".\"metadata\"");
  }

  @Test
  public void getNamespaceNames_SqlServer_WithExistingTables_ShouldWorkProperly()
      throws SQLException, ExecutionException {
    getNamespaceNames_ForX_WithExistingTables_ShouldWorkProperly(
        RdbEngine.SQL_SERVER,
        "SELECT DISTINCT [full_table_name] FROM [" + METADATA_SCHEMA + "].[metadata]");
  }

  @Test
  public void getNamespaceNames_Sqlite_WithExistingTables_ShouldWorkProperly()
      throws SQLException, ExecutionException {
    getNamespaceNames_ForX_WithExistingTables_ShouldWorkProperly(
        RdbEngine.SQLITE,
        "SELECT DISTINCT \"full_table_name\" FROM \"" + METADATA_SCHEMA + "$metadata\"");
  }

  @Test
  public void getNamespaceNames_Db2_WithExistingTables_ShouldWorkProperly()
      throws SQLException, ExecutionException {
    getNamespaceNames_ForX_WithExistingTables_ShouldWorkProperly(
        RdbEngine.DB2,
        "SELECT DISTINCT \"full_table_name\" FROM \"" + METADATA_SCHEMA + "\".\"metadata\"");
  }

  private void getNamespaceNames_ForX_WithExistingTables_ShouldWorkProperly(
      RdbEngine rdbEngine, String getTableMetadataNamespacesStatement)
      throws SQLException, ExecutionException {
    // Arrange
    Statement getTableMetadataNamespacesStatementMock = mock(Statement.class);

    when(connection.createStatement()).thenReturn(getTableMetadataNamespacesStatementMock);
    when(dataSource.getConnection()).thenReturn(connection);

    ResultSet resultSet =
        mockResultSet(
            new SelectFullTableNameFromMetadataTableResultSetMocker.Row("ns1.tbl1"),
            new SelectFullTableNameFromMetadataTableResultSetMocker.Row("ns1.tbl2"),
            new SelectFullTableNameFromMetadataTableResultSetMocker.Row("ns2.tbl3"));
    when(getTableMetadataNamespacesStatementMock.executeQuery(anyString())).thenReturn(resultSet);
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    // Act
    Set<String> actual = admin.getNamespaceNames();

    // Assert
    if (rdbEngine == RdbEngine.MYSQL || rdbEngine == RdbEngine.SQLITE) {
      verify(connection, never()).setReadOnly(anyBoolean());
    } else {
      verify(connection).setReadOnly(true);
    }
    verify(connection).createStatement();
    verify(getTableMetadataNamespacesStatementMock)
        .executeQuery(getTableMetadataNamespacesStatement);
    assertThat(actual).containsOnly("ns1", "ns2", METADATA_SCHEMA);
  }

  @Test
  public void getNamespaceNames_ForMysql_WithoutExistingTables_ShouldReturnMetadataSchemaOnly()
      throws SQLException, ExecutionException {
    getNamespaceNames_ForX_WithoutExistingTables_ShouldReturnMetadataSchemaOnly(
        RdbEngine.MYSQL,
        "SELECT DISTINCT `full_table_name` FROM `" + METADATA_SCHEMA + "`.`metadata`");
  }

  @Test
  public void getNamespaceNames_Posgresql_WithoutExistingTables_ShouldReturnMetadataSchemaOnly()
      throws SQLException, ExecutionException {
    getNamespaceNames_ForX_WithoutExistingTables_ShouldReturnMetadataSchemaOnly(
        RdbEngine.POSTGRESQL,
        "SELECT DISTINCT \"full_table_name\" FROM \"" + METADATA_SCHEMA + "\".\"metadata\"");
  }

  @Test
  public void getNamespaceNames_Oracle_WithoutExistingTables_ShouldReturnMetadataSchemaOnly()
      throws SQLException, ExecutionException {
    getNamespaceNames_ForX_WithoutExistingTables_ShouldReturnMetadataSchemaOnly(
        RdbEngine.ORACLE,
        "SELECT DISTINCT \"full_table_name\" FROM \"" + METADATA_SCHEMA + "\".\"metadata\"");
  }

  @Test
  public void getNamespaceNames_SqlServer_WithoutExistingTables_ShouldReturnMetadataSchemaOnly()
      throws SQLException, ExecutionException {
    getNamespaceNames_ForX_WithoutExistingTables_ShouldReturnMetadataSchemaOnly(
        RdbEngine.SQL_SERVER,
        "SELECT DISTINCT [full_table_name] FROM [" + METADATA_SCHEMA + "].[metadata]");
  }

  @Test
  public void getNamespaceNames_Sqlite_WithoutExistingTables_ShouldReturnMetadataSchemaOnly()
      throws SQLException, ExecutionException {
    getNamespaceNames_ForX_WithoutExistingTables_ShouldReturnMetadataSchemaOnly(
        RdbEngine.SQLITE,
        "SELECT DISTINCT \"full_table_name\" FROM \"" + METADATA_SCHEMA + "$metadata\"");
  }

  @Test
  public void getNamespaceNames_Db2_WithoutExistingTables_ShouldReturnMetadataSchemaOnly()
      throws SQLException, ExecutionException {
    getNamespaceNames_ForX_WithoutExistingTables_ShouldReturnMetadataSchemaOnly(
        RdbEngine.DB2,
        "SELECT DISTINCT \"full_table_name\" FROM \"" + METADATA_SCHEMA + "\".\"metadata\"");
  }

  private void getNamespaceNames_ForX_WithoutExistingTables_ShouldReturnMetadataSchemaOnly(
      RdbEngine rdbEngine, String getTableMetadataNamespacesStatement)
      throws SQLException, ExecutionException {
    // Arrange
    Statement getTableMetadataNamespacesStatementMock = mock(Statement.class);

    when(connection.createStatement()).thenReturn(getTableMetadataNamespacesStatementMock);
    when(dataSource.getConnection()).thenReturn(connection);

    SQLException sqlException = mock(SQLException.class);
    mockUndefinedTableError(rdbEngine, sqlException);
    when(getTableMetadataNamespacesStatementMock.executeQuery(anyString())).thenThrow(sqlException);
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);

    // Act
    Set<String> actual = admin.getNamespaceNames();

    // Assert
    verify(connection).createStatement();
    verify(getTableMetadataNamespacesStatementMock)
        .executeQuery(getTableMetadataNamespacesStatement);
    assertThat(actual).containsOnly(METADATA_SCHEMA);
  }

  @ParameterizedTest
  @EnumSource(
      value = RdbEngine.class,
      mode = Mode.EXCLUDE,
      names = {
        "SQLITE",
      })
  public void getImportTableMetadata_ForXBesidesSqlite_ShouldWorkProperly(RdbEngine rdbEngine)
      throws SQLException, ExecutionException {
    String expectedCheckTableExistStatement = prepareSqlForTableCheck(rdbEngine, NAMESPACE, TABLE);

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
    RdbEngineStrategy rdbEngineStrategy = spy(getRdbEngineStrategy(rdbEngine));
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
    JdbcAdmin admin = createJdbcAdminFor(rdbEngineStrategy);
    String description = "database engine specific test failed: " + rdbEngine;
    Map<String, DataType> overrideColumnsType = ImmutableMap.of("col", DataType.FLOAT);

    // Act
    TableMetadata actual = admin.getImportTableMetadata(NAMESPACE, TABLE, overrideColumnsType);

    // Assert
    verify(checkTableExistStatement, description(description))
        .execute(expectedCheckTableExistStatement);
    assertThat(actual.getPartitionKeyNames()).hasSameElementsAs(ImmutableSet.of("pk1", "pk2"));
    assertThat(actual.getColumnDataTypes()).containsExactlyEntriesOf(expectedColumns);
    if (rdbEngine == RdbEngine.MYSQL) {
      verify(connection, never()).setReadOnly(anyBoolean());
    } else {
      verify(connection).setReadOnly(true);
    }
    verify(rdbEngineStrategy)
        .getDataTypeForScalarDb(
            any(JDBCType.class),
            anyString(),
            anyInt(),
            anyInt(),
            eq(getFullTableName(NAMESPACE, TABLE) + " pk1"),
            eq(null));
    verify(rdbEngineStrategy)
        .getDataTypeForScalarDb(
            any(JDBCType.class),
            anyString(),
            anyInt(),
            anyInt(),
            eq(getFullTableName(NAMESPACE, TABLE) + " pk2"),
            eq(null));
    verify(rdbEngineStrategy)
        .getDataTypeForScalarDb(
            any(JDBCType.class),
            anyString(),
            anyInt(),
            anyInt(),
            eq(getFullTableName(NAMESPACE, TABLE) + " col"),
            eq(DataType.FLOAT));
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
    assertThatThrownBy(() -> admin.getImportTableMetadata(NAMESPACE, TABLE, Collections.emptyMap()))
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
    Throwable thrown =
        catchThrowable(
            () -> admin.getImportTableMetadata(NAMESPACE, TABLE, Collections.emptyMap()));

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
    when(columnResults.getInt(JDBC_COL_DATA_TYPE)).thenReturn(Types.OTHER);
    when(columnResults.getString(JDBC_COL_TYPE_NAME)).thenReturn("any_unsupported_type");
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
    Throwable thrown =
        catchThrowable(
            () -> admin.getImportTableMetadata(NAMESPACE, TABLE, Collections.emptyMap()));

    // Assert
    verify(checkTableExistStatement, description(description))
        .execute(expectedCheckTableExistStatement);
    assertThat(thrown).as(description).isInstanceOf(IllegalArgumentException.class);
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

    Statement statement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(statement);
    when(dataSource.getConnection()).thenReturn(connection);
    TableMetadata importedTableMetadata = mock(TableMetadata.class);
    doReturn(importedTableMetadata)
        .when(adminSpy)
        .getImportTableMetadata(anyString(), anyString(), anyMap());
    doNothing()
        .when(adminSpy)
        .addTableMetadata(any(), anyString(), anyString(), any(), anyBoolean(), anyBoolean());

    // Act
    adminSpy.importTable(NAMESPACE, TABLE, Collections.emptyMap(), Collections.emptyMap());

    // Assert
    verify(adminSpy).getImportTableMetadata(NAMESPACE, TABLE, Collections.emptyMap());
    verify(adminSpy).createMetadataSchemaIfNotExists(connection);
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

  private RdbEngineStrategy getRdbEngineStrategy(RdbEngine rdbEngine) {
    RdbEngineStrategy engine = RDB_ENGINES.get(rdbEngine);
    if (engine == null) {
      throw new AssertionError("RdbEngineStrategy not found for " + rdbEngine);
    }
    return engine;
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

  @Test
  void
      createTableInternal_Db2_WithBlobColumnAsKeyOrIndex_ShouldThrowUnsupportedOperationException() {
    // Arrange
    TableMetadata metadata1 =
        TableMetadata.newBuilder().addPartitionKey("pk").addColumn("pk", DataType.BLOB).build();
    TableMetadata metadata2 =
        TableMetadata.newBuilder()
            .addPartitionKey("pk")
            .addClusteringKey("ck")
            .addColumn("pk", DataType.INT)
            .addColumn("ck", DataType.BLOB)
            .build();
    TableMetadata metadata3 =
        TableMetadata.newBuilder()
            .addPartitionKey("pk")
            .addColumn("pk", DataType.INT)
            .addColumn("col", DataType.BLOB)
            .addSecondaryIndex("col")
            .build();
    JdbcAdmin admin = createJdbcAdminFor(RdbEngine.DB2);

    // Act Assert
    assertThatThrownBy(() -> admin.createTableInternal(connection, "ns", "tbl", metadata1, false))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContainingAll("BLOB", "key");
    assertThatThrownBy(() -> admin.createTableInternal(connection, "ns", "tbl", metadata2, false))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContainingAll("BLOB", "key");
    assertThatThrownBy(() -> admin.createTableInternal(connection, "ns", "tbl", metadata3, false))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContainingAll("BLOB", "index");
  }

  @Test
  void createIndex_Db2_WithBlobColumnAsKeyOrIndex_ShouldThrowUnsupportedOperationException()
      throws SQLException {
    // Arrange
    String namespace = "my_ns";
    String table = "my_tbl";
    String indexColumn = "index_col";
    JdbcAdmin admin = createJdbcAdminFor(RdbEngine.DB2);

    PreparedStatement selectStatement = mock(PreparedStatement.class);
    ResultSet resultSet =
        mockResultSet(
            new SelectAllFromMetadataTableResultSetMocker.Row(
                "pk", DataType.BOOLEAN.toString(), "PARTITION", null, false),
            new SelectAllFromMetadataTableResultSetMocker.Row(
                indexColumn, DataType.BLOB.toString(), null, null, false));
    when(selectStatement.executeQuery()).thenReturn(resultSet);
    when(connection.prepareStatement(any())).thenReturn(selectStatement);
    Statement statement = mock(Statement.class);

    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.createStatement()).thenReturn(statement);

    // Act Assert
    assertThatThrownBy(
            () -> admin.createIndex(namespace, table, indexColumn, Collections.emptyMap()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContainingAll("BLOB", "index");
  }

  @Test
  void
      createTableInternal_Oracle_WithBlobColumnAsKeyOrIndex_ShouldThrowUnsupportedOperationException() {
    // Arrange
    TableMetadata metadata1 =
        TableMetadata.newBuilder().addPartitionKey("pk").addColumn("pk", DataType.BLOB).build();
    TableMetadata metadata2 =
        TableMetadata.newBuilder()
            .addPartitionKey("pk")
            .addClusteringKey("ck")
            .addColumn("pk", DataType.INT)
            .addColumn("ck", DataType.BLOB)
            .build();
    TableMetadata metadata3 =
        TableMetadata.newBuilder()
            .addPartitionKey("pk")
            .addColumn("pk", DataType.INT)
            .addColumn("col", DataType.BLOB)
            .addSecondaryIndex("col")
            .build();
    JdbcAdmin admin = createJdbcAdminFor(RdbEngine.ORACLE);

    // Act Assert
    assertThatThrownBy(() -> admin.createTableInternal(connection, "ns", "tbl", metadata1, false))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContainingAll("BLOB", "key");
    assertThatThrownBy(() -> admin.createTableInternal(connection, "ns", "tbl", metadata2, false))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContainingAll("BLOB", "key");
    assertThatThrownBy(() -> admin.createTableInternal(connection, "ns", "tbl", metadata3, false))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContainingAll("BLOB", "index");
  }

  @Test
  void createIndex_Oracle_WithBlobColumnAsKeyOrIndex_ShouldThrowUnsupportedOperationException()
      throws SQLException {
    // Arrange
    String namespace = "my_ns";
    String table = "my_tbl";
    String indexColumn = "index_col";
    JdbcAdmin admin = createJdbcAdminFor(RdbEngine.ORACLE);

    PreparedStatement selectStatement = mock(PreparedStatement.class);
    ResultSet resultSet =
        mockResultSet(
            new SelectAllFromMetadataTableResultSetMocker.Row(
                "pk", DataType.BOOLEAN.toString(), "PARTITION", null, false),
            new SelectAllFromMetadataTableResultSetMocker.Row(
                indexColumn, DataType.BLOB.toString(), null, null, false));
    when(selectStatement.executeQuery()).thenReturn(resultSet);
    when(connection.prepareStatement(any())).thenReturn(selectStatement);
    Statement statement = mock(Statement.class);

    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.createStatement()).thenReturn(statement);

    // Act Assert
    assertThatThrownBy(
            () -> admin.createIndex(namespace, table, indexColumn, Collections.emptyMap()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContainingAll("BLOB", "index");
  }

  @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
  @Test
  public void createVirtualTable_WithInnerJoin_ShouldCreateViewWithInnerJoin() throws Exception {
    // Arrange
    String namespace = "ns";
    String table = "vtable";
    String leftSourceNamespace = "ns";
    String leftSourceTable = "left_table";
    String rightSourceNamespace = "ns";
    String rightSourceTable = "right_table";
    VirtualTableJoinType joinType = VirtualTableJoinType.INNER;

    TableMetadata leftSourceTableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addClusteringKey("ck1", Order.ASC)
            .addColumn("pk1", DataType.INT)
            .addColumn("ck1", DataType.TEXT)
            .addColumn("col1", DataType.INT)
            .build();

    TableMetadata rightSourceTableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addClusteringKey("ck1", Order.ASC)
            .addColumn("pk1", DataType.INT)
            .addColumn("ck1", DataType.TEXT)
            .addColumn("col2", DataType.TEXT)
            .build();

    when(tableMetadataService.getTableMetadata(
            any(Connection.class), eq(leftSourceNamespace), eq(leftSourceTable)))
        .thenReturn(leftSourceTableMetadata);
    when(tableMetadataService.getTableMetadata(
            any(Connection.class), eq(rightSourceNamespace), eq(rightSourceTable)))
        .thenReturn(rightSourceTableMetadata);

    Statement statement = mock(Statement.class);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.createStatement()).thenReturn(statement);
    doNothing()
        .when(virtualTableMetadataService)
        .createVirtualTablesTableIfNotExists(any(Connection.class));
    doNothing()
        .when(virtualTableMetadataService)
        .insertIntoVirtualTablesTable(
            any(Connection.class),
            anyString(),
            anyString(),
            anyString(),
            anyString(),
            anyString(),
            anyString(),
            any(VirtualTableJoinType.class),
            anyString());

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);

    // Build expected SQL (MySQL)
    RdbEngineStrategy rdbEngineStrategy = getRdbEngineStrategy(RdbEngine.MYSQL);
    String expectedSql =
        "CREATE VIEW "
            + rdbEngineStrategy.encloseFullTableName(namespace, table)
            + " AS SELECT t1."
            + rdbEngineStrategy.enclose("pk1")
            + " AS "
            + rdbEngineStrategy.enclose("pk1")
            + ", t1."
            + rdbEngineStrategy.enclose("ck1")
            + " AS "
            + rdbEngineStrategy.enclose("ck1")
            + ", t1."
            + rdbEngineStrategy.enclose("col1")
            + " AS "
            + rdbEngineStrategy.enclose("col1")
            + ", t2."
            + rdbEngineStrategy.enclose("col2")
            + " AS "
            + rdbEngineStrategy.enclose("col2")
            + " FROM "
            + rdbEngineStrategy.encloseFullTableName(leftSourceNamespace, leftSourceTable)
            + " t1 INNER JOIN "
            + rdbEngineStrategy.encloseFullTableName(rightSourceNamespace, rightSourceTable)
            + " t2 ON t1."
            + rdbEngineStrategy.enclose("pk1")
            + " = t2."
            + rdbEngineStrategy.enclose("pk1")
            + " AND t1."
            + rdbEngineStrategy.enclose("ck1")
            + " = t2."
            + rdbEngineStrategy.enclose("ck1");

    JdbcAdmin admin = createJdbcAdmin();
    JdbcAdmin spyAdmin = spy(admin);
    doNothing().when(spyAdmin).createMetadataSchemaIfNotExists(any(Connection.class));

    // Act
    spyAdmin.createVirtualTable(
        namespace,
        table,
        leftSourceNamespace,
        leftSourceTable,
        rightSourceNamespace,
        rightSourceTable,
        joinType,
        Collections.emptyMap());

    // Assert
    verify(statement).execute(sqlCaptor.capture());
    String executedSql = sqlCaptor.getValue();
    assertThat(executedSql).isEqualTo(expectedSql);
    verify(spyAdmin).createMetadataSchemaIfNotExists(eq(connection));
    verify(virtualTableMetadataService).createVirtualTablesTableIfNotExists(eq(connection));
    verify(virtualTableMetadataService)
        .insertIntoVirtualTablesTable(
            any(Connection.class),
            eq(namespace),
            eq(table),
            eq(leftSourceNamespace),
            eq(leftSourceTable),
            eq(rightSourceNamespace),
            eq(rightSourceTable),
            eq(joinType),
            eq(""));
  }

  @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
  @Test
  public void createVirtualTable_WithLeftOuterJoin_ShouldCreateViewWithLeftOuterJoin()
      throws Exception {
    // Arrange
    String namespace = "ns";
    String table = "vtable";
    String leftSourceNamespace = "ns";
    String leftSourceTable = "left_table";
    String rightSourceNamespace = "ns";
    String rightSourceTable = "right_table";
    VirtualTableJoinType joinType = VirtualTableJoinType.LEFT_OUTER;

    TableMetadata leftSourceTableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addClusteringKey("ck1", Order.ASC)
            .addColumn("pk1", DataType.INT)
            .addColumn("ck1", DataType.TEXT)
            .addColumn("col1", DataType.INT)
            .build();

    TableMetadata rightSourceTableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addClusteringKey("ck1", Order.ASC)
            .addColumn("pk1", DataType.INT)
            .addColumn("ck1", DataType.TEXT)
            .addColumn("col2", DataType.TEXT)
            .build();

    when(tableMetadataService.getTableMetadata(
            any(Connection.class), eq(leftSourceNamespace), eq(leftSourceTable)))
        .thenReturn(leftSourceTableMetadata);
    when(tableMetadataService.getTableMetadata(
            any(Connection.class), eq(rightSourceNamespace), eq(rightSourceTable)))
        .thenReturn(rightSourceTableMetadata);

    Statement statement = mock(Statement.class);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.createStatement()).thenReturn(statement);
    doNothing()
        .when(virtualTableMetadataService)
        .createVirtualTablesTableIfNotExists(any(Connection.class));
    doNothing()
        .when(virtualTableMetadataService)
        .insertIntoVirtualTablesTable(
            any(Connection.class),
            anyString(),
            anyString(),
            anyString(),
            anyString(),
            anyString(),
            anyString(),
            any(VirtualTableJoinType.class),
            anyString());

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);

    // Build expected SQL (MySQL)
    RdbEngineStrategy rdbEngineStrategy = getRdbEngineStrategy(RdbEngine.MYSQL);
    String expectedSql =
        "CREATE VIEW "
            + rdbEngineStrategy.encloseFullTableName(namespace, table)
            + " AS SELECT t1."
            + rdbEngineStrategy.enclose("pk1")
            + " AS "
            + rdbEngineStrategy.enclose("pk1")
            + ", t1."
            + rdbEngineStrategy.enclose("ck1")
            + " AS "
            + rdbEngineStrategy.enclose("ck1")
            + ", t1."
            + rdbEngineStrategy.enclose("col1")
            + " AS "
            + rdbEngineStrategy.enclose("col1")
            + ", t2."
            + rdbEngineStrategy.enclose("col2")
            + " AS "
            + rdbEngineStrategy.enclose("col2")
            + " FROM "
            + rdbEngineStrategy.encloseFullTableName(leftSourceNamespace, leftSourceTable)
            + " t1 LEFT OUTER JOIN "
            + rdbEngineStrategy.encloseFullTableName(rightSourceNamespace, rightSourceTable)
            + " t2 ON t1."
            + rdbEngineStrategy.enclose("pk1")
            + " = t2."
            + rdbEngineStrategy.enclose("pk1")
            + " AND t1."
            + rdbEngineStrategy.enclose("ck1")
            + " = t2."
            + rdbEngineStrategy.enclose("ck1");

    JdbcAdmin admin = createJdbcAdmin();
    JdbcAdmin spyAdmin = spy(admin);
    doNothing().when(spyAdmin).createMetadataSchemaIfNotExists(any(Connection.class));

    // Act
    spyAdmin.createVirtualTable(
        namespace,
        table,
        leftSourceNamespace,
        leftSourceTable,
        rightSourceNamespace,
        rightSourceTable,
        joinType,
        Collections.emptyMap());

    // Assert
    verify(statement).execute(sqlCaptor.capture());
    String executedSql = sqlCaptor.getValue();
    assertThat(executedSql).isEqualTo(expectedSql);
    verify(spyAdmin).createMetadataSchemaIfNotExists(eq(connection));
    verify(virtualTableMetadataService).createVirtualTablesTableIfNotExists(eq(connection));
    verify(virtualTableMetadataService)
        .insertIntoVirtualTablesTable(
            eq(connection),
            eq(namespace),
            eq(table),
            eq(leftSourceNamespace),
            eq(leftSourceTable),
            eq(rightSourceNamespace),
            eq(rightSourceTable),
            eq(joinType),
            eq(""));
  }

  @Test
  public void createVirtualTable_SQLExceptionThrown_ShouldThrowExecutionException()
      throws Exception {
    // Arrange
    String namespace = "ns";
    String table = "vtable";
    String leftSourceNamespace = "ns";
    String leftSourceTable = "left_table";
    String rightSourceNamespace = "ns";
    String rightSourceTable = "right_table";
    VirtualTableJoinType joinType = VirtualTableJoinType.INNER;

    Statement statement = mock(Statement.class);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.createStatement()).thenReturn(statement);

    TableMetadata leftSourceTableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addClusteringKey("ck1")
            .addColumn("pk1", DataType.TEXT)
            .addColumn("ck1", DataType.TEXT)
            .addColumn("col1", DataType.TEXT)
            .build();

    TableMetadata rightSourceTableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addClusteringKey("ck1")
            .addColumn("pk1", DataType.TEXT)
            .addColumn("ck1", DataType.TEXT)
            .addColumn("col2", DataType.TEXT)
            .build();

    when(tableMetadataService.getTableMetadata(
            any(Connection.class), eq(leftSourceNamespace), eq(leftSourceTable)))
        .thenReturn(leftSourceTableMetadata);
    when(tableMetadataService.getTableMetadata(
            any(Connection.class), eq(rightSourceNamespace), eq(rightSourceTable)))
        .thenReturn(rightSourceTableMetadata);

    SQLException sqlException = new SQLException("SQL error occurred");
    when(statement.execute(anyString())).thenThrow(sqlException);

    JdbcAdmin admin = createJdbcAdmin();

    // Act Assert
    assertThatThrownBy(
            () ->
                admin.createVirtualTable(
                    namespace,
                    table,
                    leftSourceNamespace,
                    leftSourceTable,
                    rightSourceNamespace,
                    rightSourceTable,
                    joinType,
                    Collections.emptyMap()))
        .isInstanceOf(ExecutionException.class)
        .hasCause(sqlException);
  }

  @Test
  public void getVirtualTableInfo_VirtualTableExists_ShouldReturnVirtualTableInfo()
      throws Exception {
    // Arrange
    String namespace = "ns";
    String table = "vtable";

    when(dataSource.getConnection()).thenReturn(connection);

    VirtualTableInfo expectedVirtualTableInfo = mock(VirtualTableInfo.class);

    when(virtualTableMetadataService.getVirtualTableInfo(connection, namespace, table))
        .thenReturn(expectedVirtualTableInfo);

    JdbcAdmin admin = createJdbcAdmin();

    // Act
    Optional<VirtualTableInfo> result = admin.getVirtualTableInfo(namespace, table);

    // Assert
    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(expectedVirtualTableInfo);
    verify(virtualTableMetadataService)
        .getVirtualTableInfo(eq(connection), eq(namespace), eq(table));
  }

  @Test
  public void getVirtualTableInfo_VirtualTableDoesNotExist_ShouldReturnEmptyOptional()
      throws Exception {
    // Arrange
    String namespace = "ns";
    String table = "vtable";

    when(dataSource.getConnection()).thenReturn(connection);

    when(virtualTableMetadataService.getVirtualTableInfo(connection, namespace, table))
        .thenReturn(null);

    JdbcAdmin admin = createJdbcAdmin();

    // Act
    Optional<VirtualTableInfo> result = admin.getVirtualTableInfo(namespace, table);

    // Assert
    assertThat(result).isEmpty();
    verify(virtualTableMetadataService)
        .getVirtualTableInfo(eq(connection), eq(namespace), eq(table));
  }

  @Test
  public void getVirtualTableInfo_SQLExceptionThrown_ShouldThrowExecutionException()
      throws Exception {
    // Arrange
    String namespace = "ns";
    String table = "vtable";

    when(dataSource.getConnection()).thenReturn(connection);

    SQLException sqlException = new SQLException("SQL error occurred");

    when(virtualTableMetadataService.getVirtualTableInfo(connection, namespace, table))
        .thenThrow(sqlException);

    JdbcAdmin admin = createJdbcAdmin();

    // Act Assert
    assertThatThrownBy(() -> admin.getVirtualTableInfo(namespace, table))
        .isInstanceOf(ExecutionException.class)
        .hasCause(sqlException);
  }

  @ParameterizedTest
  @EnumSource(RdbEngine.class)
  void getStorageInfo_WithRepeatableReadIsolationLevel_ShouldReturnCorrectInfo(RdbEngine rdbEngine)
      throws Exception {
    // Arrange
    int isolationLevel = Connection.TRANSACTION_REPEATABLE_READ;
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.getTransactionIsolation()).thenReturn(isolationLevel);

    // Act
    StorageInfo storageInfo = admin.getStorageInfo("namespace");

    // Assert
    assertThat(storageInfo.getStorageName()).isEqualTo("jdbc");
    assertThat(storageInfo.getMutationAtomicityUnit())
        .isEqualTo(StorageInfo.MutationAtomicityUnit.STORAGE);
    assertThat(storageInfo.getMaxAtomicMutationsCount()).isEqualTo(Integer.MAX_VALUE);

    // Check consistent virtual table read guarantee based on RDB engine
    RdbEngineStrategy strategy = getRdbEngineStrategy(rdbEngine);
    boolean expectedConsistentVirtualTableReadGuaranteed =
        isolationLevel >= strategy.getMinimumIsolationLevelForConsistentVirtualTableRead();
    assertThat(storageInfo.isConsistentVirtualTableReadGuaranteed())
        .isEqualTo(expectedConsistentVirtualTableReadGuaranteed);
  }

  @ParameterizedTest
  @EnumSource(RdbEngine.class)
  void getStorageInfo_WithReadCommittedIsolationLevel_ShouldReturnCorrectInfo(RdbEngine rdbEngine)
      throws Exception {
    // Arrange
    int isolationLevel = Connection.TRANSACTION_READ_COMMITTED;
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.getTransactionIsolation()).thenReturn(isolationLevel);

    // Act
    StorageInfo storageInfo = admin.getStorageInfo("namespace");

    // Assert
    assertThat(storageInfo.getStorageName()).isEqualTo("jdbc");
    assertThat(storageInfo.getMutationAtomicityUnit())
        .isEqualTo(StorageInfo.MutationAtomicityUnit.STORAGE);
    assertThat(storageInfo.getMaxAtomicMutationsCount()).isEqualTo(Integer.MAX_VALUE);

    // Check consistent virtual table read guarantee based on RDB engine
    RdbEngineStrategy strategy = getRdbEngineStrategy(rdbEngine);
    boolean expectedConsistentVirtualTableReadGuaranteed =
        isolationLevel >= strategy.getMinimumIsolationLevelForConsistentVirtualTableRead();
    assertThat(storageInfo.isConsistentVirtualTableReadGuaranteed())
        .isEqualTo(expectedConsistentVirtualTableReadGuaranteed);
  }

  @ParameterizedTest
  @EnumSource(RdbEngine.class)
  void getStorageInfo_WithSerializableIsolationLevel_ShouldReturnCorrectInfo(RdbEngine rdbEngine)
      throws Exception {
    // Arrange
    int isolationLevel = Connection.TRANSACTION_SERIALIZABLE;
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.getTransactionIsolation()).thenReturn(isolationLevel);

    // Act
    StorageInfo storageInfo = admin.getStorageInfo("namespace");

    // Assert
    assertThat(storageInfo.getStorageName()).isEqualTo("jdbc");
    assertThat(storageInfo.getMutationAtomicityUnit())
        .isEqualTo(StorageInfo.MutationAtomicityUnit.STORAGE);
    assertThat(storageInfo.getMaxAtomicMutationsCount()).isEqualTo(Integer.MAX_VALUE);

    // Check consistent virtual table read guarantee based on RDB engine
    RdbEngineStrategy strategy = getRdbEngineStrategy(rdbEngine);
    boolean expectedConsistentVirtualTableReadGuaranteed =
        isolationLevel >= strategy.getMinimumIsolationLevelForConsistentVirtualTableRead();
    assertThat(storageInfo.isConsistentVirtualTableReadGuaranteed())
        .isEqualTo(expectedConsistentVirtualTableReadGuaranteed);
  }

  @ParameterizedTest
  @EnumSource(RdbEngine.class)
  void getStorageInfo_SQLExceptionThrown_ShouldThrowExecutionException(RdbEngine rdbEngine)
      throws Exception {
    // Arrange
    JdbcAdmin admin = createJdbcAdminFor(rdbEngine);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.getTransactionIsolation()).thenThrow(new SQLException("Connection error"));

    // Act Assert
    assertThatThrownBy(() -> admin.getStorageInfo("namespace"))
        .isInstanceOf(ExecutionException.class)
        .hasMessageContaining("Getting the transaction isolation level failed")
        .hasCauseInstanceOf(SQLException.class);
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
      when(mock.getString(TableMetadataService.COL_COLUMN_NAME)).thenReturn(currentRow.columnName);
      when(mock.getString(TableMetadataService.COL_DATA_TYPE)).thenReturn(currentRow.dataType);
      when(mock.getString(TableMetadataService.COL_KEY_TYPE)).thenReturn(currentRow.keyType);
      when(mock.getString(TableMetadataService.COL_CLUSTERING_ORDER))
          .thenReturn(currentRow.clusteringOrder);
      when(mock.getBoolean(TableMetadataService.COL_INDEXED)).thenReturn(currentRow.indexed);
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
      when(mock.getString(TableMetadataService.COL_FULL_TABLE_NAME))
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
