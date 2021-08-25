package com.scalar.db.storage.jdbc;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
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
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@SuppressFBWarnings({"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"})
public class JdbcDatabaseAdminTest {

  @Mock JdbcTableMetadataManager metadataManager;
  @Mock BasicDataSource dataSource;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void
      getTableMetadata_ConstructedWithoutNamespacePrefix_ShouldBeCalledWithoutNamespacePrefix()
          throws ExecutionException {
    // Arrange
    Optional<String> namespacePrefix = Optional.empty();
    String namespace = "ns";
    String table = "table";

    JdbcDatabaseAdmin admin =
        new JdbcDatabaseAdmin(dataSource, metadataManager, namespacePrefix, RdbEngine.MYSQL);

    // Act
    admin.getTableMetadata(namespace, table);

    // Assert
    verify(metadataManager).getTableMetadata(namespace, table);
  }

  @Test
  public void getTableMetadata_ConstructedWithNamespacePrefix_ShouldBeCalledWithoutNamespacePrefix()
      throws ExecutionException {
    // Arrange
    Optional<String> namespacePrefix = Optional.of("prefix_");
    String namespace = "ns";
    String table = "table";

    JdbcDatabaseAdmin admin =
        new JdbcDatabaseAdmin(dataSource, metadataManager, namespacePrefix, RdbEngine.MYSQL);

    // Act
    admin.getTableMetadata(namespace, table);

    // Assert
    verify(metadataManager).getTableMetadata(namespace, table);
  }

  @Test
  public void createTable_forMysql_shouldExecuteCreateTableStatement()
      throws ExecutionException, SQLException {
    createTable_forX_shouldExecuteCreateTableStatement(
        RdbEngine.MYSQL,
        "CREATE TABLE `ns_prefixmy_ns`.`foo_table`(`c3` BOOLEAN,`c1` VARCHAR(64),`c4` VARBINARY(64),`c2` BIGINT,`c5` INT,`c6` DOUBLE,`c7` FLOAT, PRIMARY KEY (`c3`,`c1`,`c4`))",
        "CREATE INDEX index_ns_prefixmy_ns_foo_table_c4 ON `ns_prefixmy_ns`.`foo_table` (`c4`)",
        "CREATE INDEX index_ns_prefixmy_ns_foo_table_c1 ON `ns_prefixmy_ns`.`foo_table` (`c1`)");
  }

  @Test
  public void createTable_forPostgresql_shouldExecuteCreateTableStatement()
      throws ExecutionException, SQLException {
    createTable_forX_shouldExecuteCreateTableStatement(
        RdbEngine.POSTGRESQL,
        "CREATE TABLE \"ns_prefixmy_ns\".\"foo_table\"(\"c3\" BOOLEAN,\"c1\" VARCHAR(10485760),\"c4\" BYTEA,\"c2\" BIGINT,\"c5\" INT,\"c6\" DOUBLE PRECISION,\"c7\" FLOAT, PRIMARY KEY (\"c3\",\"c1\",\"c4\"))",
        "CREATE INDEX index_ns_prefixmy_ns_foo_table_c4 ON \"ns_prefixmy_ns\".\"foo_table\" (\"c4\")",
        "CREATE INDEX index_ns_prefixmy_ns_foo_table_c1 ON \"ns_prefixmy_ns\".\"foo_table\" (\"c1\")");
  }

  @Test
  public void createTable_forSqlServer_shouldExecuteCreateTableStatement()
      throws ExecutionException, SQLException {
    createTable_forX_shouldExecuteCreateTableStatement(
        RdbEngine.SQL_SERVER,
        "CREATE TABLE [ns_prefixmy_ns].[foo_table]([c3] BIT,[c1] VARCHAR(8000),[c4] VARBINARY(8000),[c2] BIGINT,[c5] INT,[c6] FLOAT,[c7] FLOAT(24), PRIMARY KEY ([c3],[c1],[c4]))",
        "CREATE INDEX index_ns_prefixmy_ns_foo_table_c4 ON [ns_prefixmy_ns].[foo_table] ([c4])",
        "CREATE INDEX index_ns_prefixmy_ns_foo_table_c1 ON [ns_prefixmy_ns].[foo_table] ([c1])");
  }

  @Test
  public void createTable_forOracle_shouldExecuteCreateTableStatement()
      throws ExecutionException, SQLException {
    createTable_forX_shouldExecuteCreateTableStatement(
        RdbEngine.ORACLE,
        "CREATE TABLE \"ns_prefixmy_ns\".\"foo_table\"(\"c3\" NUMBER(1),\"c1\" VARCHAR2(64),\"c4\" RAW(64),\"c2\" NUMBER(19),\"c5\" INT,\"c6\" BINARY_DOUBLE,\"c7\" BINARY_FLOAT, PRIMARY KEY (\"c3\",\"c1\",\"c4\"))",
        "CREATE INDEX index_ns_prefixmy_ns_foo_table_c4 ON \"ns_prefixmy_ns\".\"foo_table\" (\"c4\")",
        "CREATE INDEX index_ns_prefixmy_ns_foo_table_c1 ON \"ns_prefixmy_ns\".\"foo_table\" (\"c1\")");
  }

  @Test
  public void createNamespace_forMysql_shouldExecuteCreateNamespaceStatement()
      throws ExecutionException, SQLException {
    createNamespace_forX_shouldExecuteCreateNamespaceStatement(
        RdbEngine.MYSQL, "CREATE SCHEMA `ns_prefixmy_ns`");
  }

  @Test
  public void createNamespace_forPostgresql_shouldExecuteCreateNamespaceStatement()
      throws ExecutionException, SQLException {
    createNamespace_forX_shouldExecuteCreateNamespaceStatement(
        RdbEngine.POSTGRESQL, "CREATE SCHEMA \"ns_prefixmy_ns\"");
  }

  @Test
  public void createNamespace_forSqlServer_shouldExecuteCreateNamespaceStatement()
      throws ExecutionException, SQLException {
    createNamespace_forX_shouldExecuteCreateNamespaceStatement(
        RdbEngine.SQL_SERVER, "CREATE SCHEMA [ns_prefixmy_ns]");
  }

  @Test
  public void createNamespace_forOracle_shouldExecuteCreateNamespaceStatement()
      throws ExecutionException, SQLException {
    createNamespace_forX_shouldExecuteCreateNamespaceStatement(
        RdbEngine.ORACLE,
        "CREATE USER \"ns_prefixmy_ns\" IDENTIFIED BY \"oracle\"",
        "ALTER USER \"ns_prefixmy_ns\" quota unlimited on USERS");
  }

  private void createTable_forX_shouldExecuteCreateTableStatement(
      RdbEngine rdbEngine, String... expectedSqlStatements)
      throws SQLException, ExecutionException {
    // Arrange
    String namespacePrefix = "ns_prefix";
    String namespace = "my_ns";
    String table = "foo_table";
    JdbcDatabaseAdmin admin =
        new JdbcDatabaseAdmin(dataSource, metadataManager, Optional.of(namespacePrefix), rdbEngine);
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
    Connection connection = mock(Connection.class);
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
    admin.createTable(namespace, table, metadata, new HashMap<>());

    // Assert
    for (int i = 0; i < expectedSqlStatements.length; i++) {
      verify(mockedStatements.get(i)).execute(expectedSqlStatements[i]);
    }
    verify(metadataManager).addTableMetadata(namespace, table, metadata);
  }

  private void createNamespace_forX_shouldExecuteCreateNamespaceStatement(
      RdbEngine rdbEngine, String... expectedSqlStatements)
      throws SQLException, ExecutionException {
    // Arrange
    String namespacePrefix = "ns_prefix";
    String namespace = "my_ns";
    JdbcDatabaseAdmin admin =
        new JdbcDatabaseAdmin(dataSource, metadataManager, Optional.of(namespacePrefix), rdbEngine);
    Connection connection = mock(Connection.class);
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
  public void truncateTable_forMysql_shouldExecuteTruncateTableStatement()
      throws SQLException, ExecutionException {
    truncateTable_forX_shouldExecuteTruncateTableStatement(
        RdbEngine.MYSQL, "TRUNCATE TABLE `ns_prefixmy_ns`.`foo_table`");
  }

  @Test
  public void truncateTable_forPostgresql_shouldExecuteTruncateTableStatement()
      throws SQLException, ExecutionException {
    truncateTable_forX_shouldExecuteTruncateTableStatement(
        RdbEngine.POSTGRESQL, "TRUNCATE TABLE \"ns_prefixmy_ns\".\"foo_table\"");
  }

  @Test
  public void truncateTable_forSqlServer_shouldExecuteTruncateTableStatement()
      throws SQLException, ExecutionException {
    truncateTable_forX_shouldExecuteTruncateTableStatement(
        RdbEngine.SQL_SERVER, "TRUNCATE TABLE [ns_prefixmy_ns].[foo_table]");
  }

  @Test
  public void truncateTable_forOracle_shouldExecuteTruncateTableStatement()
      throws SQLException, ExecutionException {
    truncateTable_forX_shouldExecuteTruncateTableStatement(
        RdbEngine.ORACLE, "TRUNCATE TABLE \"ns_prefixmy_ns\".\"foo_table\"");
  }

  private void truncateTable_forX_shouldExecuteTruncateTableStatement(
      RdbEngine rdbEngine, String expectedTruncateTableStatement)
      throws SQLException, ExecutionException {
    // Arrange
    String namespacePrefix = "ns_prefix";
    String namespace = "my_ns";
    String table = "foo_table";
    JdbcDatabaseAdmin admin =
        new JdbcDatabaseAdmin(dataSource, metadataManager, Optional.of(namespacePrefix), rdbEngine);

    Connection connection = mock(Connection.class);
    Statement truncateTableStatement = mock(Statement.class);

    when(connection.createStatement()).thenReturn(truncateTableStatement);
    when(dataSource.getConnection()).thenReturn(connection);

    // Act
    admin.truncateTable(namespace, table);

    // Assert
    verify(truncateTableStatement).execute(expectedTruncateTableStatement);
  }

  @Test
  public void dropTable_forMysql_shouldDropTable() throws Exception {
    dropTable_forX_shouldDropTable(RdbEngine.MYSQL, "DROP TABLE `ns_prefixmy_ns`.`foo_table`");
  }

  @Test
  public void dropTable_forPostgresql_shouldDropTable() throws Exception {
    dropTable_forX_shouldDropTable(
        RdbEngine.POSTGRESQL, "DROP TABLE \"ns_prefixmy_ns\".\"foo_table\"");
  }

  @Test
  public void dropTable_forSqlServer_shouldDropTable() throws Exception {
    dropTable_forX_shouldDropTable(RdbEngine.SQL_SERVER, "DROP TABLE [ns_prefixmy_ns].[foo_table]");
  }

  @Test
  public void dropTable_forOracle_shouldDropTable() throws Exception {
    dropTable_forX_shouldDropTable(RdbEngine.ORACLE, "DROP TABLE \"ns_prefixmy_ns\".\"foo_table\"");
  }

  @Test
  public void dropNamespace_forMysql_shouldDropNamespace() throws Exception {
    dropSchema_forX_shouldDropSchema(RdbEngine.MYSQL, "DROP SCHEMA `ns_prefixmy_ns`");
  }

  @Test
  public void dropNamespace_forPostgresql_shouldDropNamespace() throws Exception {
    dropSchema_forX_shouldDropSchema(RdbEngine.POSTGRESQL, "DROP SCHEMA \"ns_prefixmy_ns\"");
  }

  @Test
  public void dropNamespace_forSqlServer_shouldDropNamespace() throws Exception {
    dropSchema_forX_shouldDropSchema(RdbEngine.SQL_SERVER, "DROP SCHEMA [ns_prefixmy_ns]");
  }

  @Test
  public void dropNamespace_forOracle_shouldDropNamespace() throws Exception {
    dropSchema_forX_shouldDropSchema(RdbEngine.ORACLE, "DROP USER \"ns_prefixmy_ns\"");
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

  private void dropTable_forX_shouldDropTable(
      RdbEngine rdbEngine, String expectedDropTableStatement) throws Exception {
    // Arrange
    String namespacePrefix = "ns_prefix";
    String namespace = "my_ns";
    String table = "foo_table";
    JdbcDatabaseAdmin admin =
        new JdbcDatabaseAdmin(dataSource, metadataManager, Optional.of(namespacePrefix), rdbEngine);

    Connection connection = mock(Connection.class);
    Statement dropTableStatement = mock(Statement.class);

    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.createStatement()).thenReturn(dropTableStatement);

    // Act
    admin.dropTable(namespace, table);

    // Assert
    verify(dropTableStatement).execute(expectedDropTableStatement);
    verify(metadataManager).deleteTableMetadata(namespace, table);
  }

  private void dropSchema_forX_shouldDropSchema(
      RdbEngine rdbEngine, String expectedDropSchemaStatement) throws Exception {
    // Arrange
    String namespacePrefix = "ns_prefix";
    String namespace = "my_ns";
    JdbcDatabaseAdmin admin =
        new JdbcDatabaseAdmin(dataSource, metadataManager, Optional.of(namespacePrefix), rdbEngine);

    Connection connection = mock(Connection.class);
    Statement dropSchemaStatement = mock(Statement.class);

    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.createStatement()).thenReturn(dropSchemaStatement);

    // Act
    admin.dropNamespace(namespace);

    // Assert
    verify(dropSchemaStatement).execute(expectedDropSchemaStatement);
  }

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED")
  private void namespaceExists_forXWithExistingNamespace_ShouldReturnTrue(
      RdbEngine rdbEngine, String expectedSelectStatement) throws SQLException, ExecutionException {
    // Arrange
    String namespacePrefix = "ns_prefix";
    String namespace = "my_ns";
    JdbcDatabaseAdmin admin =
        new JdbcDatabaseAdmin(dataSource, metadataManager, Optional.of(namespacePrefix), rdbEngine);

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
    verify(selectStatement).setString(1, namespacePrefix + namespace);
  }
}
