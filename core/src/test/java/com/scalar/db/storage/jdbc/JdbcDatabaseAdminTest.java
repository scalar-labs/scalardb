package com.scalar.db.storage.jdbc;

import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
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
        "CREATE SCHEMA IF NOT EXISTS `ns_prefixmy_ns`",
        "CREATE TABLE `ns_prefixmy_ns`.`foo_table`(`c3` BOOLEAN,`c1` VARCHAR(64),`c4` VARBINARY(64),`c2` BIGINT,`c5` INT,`c6` DOUBLE,`c7` FLOAT, PRIMARY KEY (`c3`,`c1`,`c4`))",
        "CREATE INDEX index_ns_prefixmy_ns_foo_table_c4 ON `ns_prefixmy_ns`.`foo_table` (`c4`)",
        "CREATE INDEX index_ns_prefixmy_ns_foo_table_c1 ON `ns_prefixmy_ns`.`foo_table` (`c1`)");
  }

  @Test
  public void createTable_forPostgresql_shouldExecuteCreateTableStatement()
      throws ExecutionException, SQLException {
    createTable_forX_shouldExecuteCreateTableStatement(
        RdbEngine.POSTGRESQL,
        "CREATE SCHEMA IF NOT EXISTS \"ns_prefixmy_ns\"",
        "CREATE TABLE \"ns_prefixmy_ns\".\"foo_table\"(\"c3\" BOOLEAN,\"c1\" VARCHAR(10485760),\"c4\" BYTEA,\"c2\" BIGINT,\"c5\" INT,\"c6\" DOUBLE PRECISION,\"c7\" FLOAT, PRIMARY KEY (\"c3\",\"c1\",\"c4\"))",
        "CREATE INDEX index_ns_prefixmy_ns_foo_table_c4 ON \"ns_prefixmy_ns\".\"foo_table\" (\"c4\")",
        "CREATE INDEX index_ns_prefixmy_ns_foo_table_c1 ON \"ns_prefixmy_ns\".\"foo_table\" (\"c1\")");
  }

  @Test
  public void createTable_forSqlServer_shouldExecuteCreateTableStatement()
      throws ExecutionException, SQLException {
    createTable_forX_shouldExecuteCreateTableStatement(
        RdbEngine.SQL_SERVER,
        "CREATE SCHEMA [ns_prefixmy_ns]",
        "CREATE TABLE [ns_prefixmy_ns].[foo_table]([c3] BIT,[c1] VARCHAR(8000),[c4] VARBINARY(8000),[c2] BIGINT,[c5] INT,[c6] FLOAT,[c7] FLOAT(24), PRIMARY KEY ([c3],[c1],[c4]))",
        "CREATE INDEX index_ns_prefixmy_ns_foo_table_c4 ON [ns_prefixmy_ns].[foo_table] ([c4])",
        "CREATE INDEX index_ns_prefixmy_ns_foo_table_c1 ON [ns_prefixmy_ns].[foo_table] ([c1])");
  }

  @Test
  public void createTable_forOracle_shouldExecuteCreateTableStatement()
      throws ExecutionException, SQLException {
    createTable_forX_shouldExecuteCreateTableStatement(
        RdbEngine.ORACLE,
        "CREATE USER \"ns_prefixmy_ns\" IDENTIFIED BY \"oracle\"",
        "ALTER USER \"ns_prefixmy_ns\" quota unlimited on USERS",
        "CREATE TABLE \"ns_prefixmy_ns\".\"foo_table\"(\"c3\" NUMBER(1),\"c1\" VARCHAR2(64),\"c4\" RAW(64),\"c2\" NUMBER(19),\"c5\" INT,\"c6\" BINARY_DOUBLE,\"c7\" BINARY_FLOAT, PRIMARY KEY (\"c3\",\"c1\",\"c4\")) ROWDEPENDENCIES",
        "ALTER TABLE \"ns_prefixmy_ns\".\"foo_table\" INITRANS 3 MAXTRANS 255",
        "CREATE INDEX index_ns_prefixmy_ns_foo_table_c4 ON \"ns_prefixmy_ns\".\"foo_table\" (\"c4\")",
        "CREATE INDEX index_ns_prefixmy_ns_foo_table_c1 ON \"ns_prefixmy_ns\".\"foo_table\" (\"c1\")");
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
  public void dropTable_forMysqlWithOnlyOneTableLeftInNamespace_shouldDropTableAndSchema()
      throws Exception {
    dropTable_forXWithOnlyOneTableLeftInNamespace_shouldDropTableAndSchema(
        RdbEngine.MYSQL, "DROP TABLE `ns_prefixmy_ns`.`foo_table`", "DROP SCHEMA `ns_prefixmy_ns`");
  }

  @Test
  public void dropTable_forPostgresqlWithOnlyOneTableLeftInNamespace_shouldDropTableAndSchema()
      throws Exception {
    dropTable_forXWithOnlyOneTableLeftInNamespace_shouldDropTableAndSchema(
        RdbEngine.POSTGRESQL,
        "DROP TABLE \"ns_prefixmy_ns\".\"foo_table\"",
        "DROP SCHEMA \"ns_prefixmy_ns\"");
  }

  @Test
  public void dropTable_forSqlServerWithOnlyOneTableLeftInNamespace_shouldDropTableAndSchema()
      throws Exception {
    dropTable_forXWithOnlyOneTableLeftInNamespace_shouldDropTableAndSchema(
        RdbEngine.SQL_SERVER,
        "DROP TABLE [ns_prefixmy_ns].[foo_table]",
        "DROP SCHEMA [ns_prefixmy_ns]");
  }

  @Test
  public void dropTable_forOracleWithOnlyOneTableLeftInNamespace_shouldDropTableAndSchema()
      throws Exception {
    dropTable_forXWithOnlyOneTableLeftInNamespace_shouldDropTableAndSchema(
        RdbEngine.ORACLE,
        "DROP TABLE \"ns_prefixmy_ns\".\"foo_table\"",
        "DROP USER \"ns_prefixmy_ns\"");
  }

  private void dropTable_forXWithOnlyOneTableLeftInNamespace_shouldDropTableAndSchema(
      RdbEngine rdbEngine, String expectedDropTableStatement, String expectedDropSchemaStatement)
      throws Exception {
    // Arrange
    String namespacePrefix = "ns_prefix";
    String namespace = "my_ns";
    String table = "foo_table";
    JdbcDatabaseAdmin admin =
        new JdbcDatabaseAdmin(dataSource, metadataManager, Optional.of(namespacePrefix), rdbEngine);

    Connection connection = mock(Connection.class);
    Statement dropTableStatement = mock(Statement.class);
    Statement dropSchemaStatement = mock(Statement.class);

    when(connection.createStatement()).thenReturn(dropTableStatement, dropSchemaStatement);
    when(dataSource.getConnection()).thenReturn(connection);
    when(metadataManager.getTableNames(namespace)).thenReturn(Collections.emptySet());

    // Act
    admin.dropTable(namespace, table);

    // Assert
    verify(dropTableStatement).execute(expectedDropTableStatement);
    verify(metadataManager).deleteTableMetadata(namespace, table);
    verify(dropSchemaStatement).execute(expectedDropSchemaStatement);
  }

  @Test
  public void dropTable_forMysqlWithSeveralTableLeftInNamespace_shouldOnlyDropTable()
      throws Exception {
    dropTable_forXWithSeveralTableLeftInNamespace_shouldOnlyDropTable(RdbEngine.MYSQL);
  }

  @Test
  public void dropTable_forPostgresqlWithSeveralTableLeftInNamespace_shouldOnlyDropTable()
      throws Exception {
    dropTable_forXWithSeveralTableLeftInNamespace_shouldOnlyDropTable(RdbEngine.POSTGRESQL);
  }

  @Test
  public void dropTable_forSqlServerWithSeveralTableLeftInNamespace_shouldOnlyDropTable()
      throws Exception {
    dropTable_forXWithSeveralTableLeftInNamespace_shouldOnlyDropTable(RdbEngine.SQL_SERVER);
  }

  @Test
  public void dropTable_forOracleWithSeveralTableLeftInNamespace_shouldOnlyDropTable()
      throws Exception {
    dropTable_forXWithSeveralTableLeftInNamespace_shouldOnlyDropTable(RdbEngine.ORACLE);
  }

  private void dropTable_forXWithSeveralTableLeftInNamespace_shouldOnlyDropTable(
      RdbEngine rdbEngine) throws Exception {
    // Arrange
    String namespacePrefix = "ns_prefix";
    String namespace = "my_ns";
    String table = "foo_table";
    JdbcDatabaseAdmin admin =
        new JdbcDatabaseAdmin(dataSource, metadataManager, Optional.of(namespacePrefix), rdbEngine);

    Connection connection = mock(Connection.class);
    Statement dropTableStatement = mock(Statement.class);

    when(connection.createStatement()).thenReturn(dropTableStatement);
    when(dataSource.getConnection()).thenReturn(connection);
    when(metadataManager.getTableNames(namespace)).thenReturn(Collections.singleton("bar_table"));

    // Act
    admin.dropTable(namespace, table);

    // Assert
    verify(dropTableStatement).execute(startsWith("DROP TABLE"));
    verify(metadataManager).deleteTableMetadata(namespace, table);
    verify(connection).createStatement();
  }
}
