package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.JdbcTableMetadataManagerTest.ResultSetMocker.Row;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;

public class JdbcTableMetadataManagerTest {
  @Mock DataSource dataSource;
  @Mock Connection connection1;
  @Mock Connection connection2;

  @Before
  public void before() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void addTableMetadata_forMysql_ShouldAddMetadata() throws Exception {
    // Arrange
    String namespace = "ns1";
    String table = "t1";
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
            .addSecondaryIndex("c4")
            .build();
    JdbcTableMetadataManager manager =
        new JdbcTableMetadataManager(dataSource, Optional.empty(), RdbEngine.MYSQL);
    Statement createSchemaStatement = mock(Statement.class);
    Statement createTableStatement = mock(Statement.class);
    Statement insertC1Statement = mock(Statement.class);
    Statement insertC2Statement = mock(Statement.class);
    Statement insertC3Statement = mock(Statement.class);
    Statement insertC4Statement = mock(Statement.class);
    Statement insertC5Statement = mock(Statement.class);
    Statement insertC6Statement = mock(Statement.class);
    Statement insertC7Statement = mock(Statement.class);
    when(connection1.createStatement())
        .thenReturn(createSchemaStatement)
        .thenReturn(createTableStatement);

    when(connection2.createStatement())
        .thenReturn(insertC3Statement)
        .thenReturn(insertC1Statement)
        .thenReturn(insertC4Statement)
        .thenReturn(insertC2Statement)
        .thenReturn(insertC5Statement)
        .thenReturn(insertC6Statement)
        .thenReturn(insertC7Statement);
    when(dataSource.getConnection()).thenReturn(connection1).thenReturn(connection2);

    // Act
    manager.addTableMetadata(namespace, table, metadata);

    // Assert
    verify(createSchemaStatement).execute("CREATE SCHEMA `scalardb`");
    String expectedCreateTableStatement =
        "CREATE TABLE `scalardb`.`metadata`("
            + "`full_table_name` VARCHAR(128),"
            + "`column_name` VARCHAR(128),"
            + "`data_type` VARCHAR(20) NOT NULL,"
            + "`key_type` VARCHAR(20),"
            + "`clustering_order` VARCHAR(10),"
            + "`indexed` BOOLEAN NOT NULL,"
            + "`ordinal_position` INTEGER NOT NULL,"
            + "PRIMARY KEY (`full_table_name`, `column_name`))";
    verify(createTableStatement).execute(expectedCreateTableStatement);
    verify(insertC3Statement)
        .execute(
            "INSERT INTO `scalardb`.`metadata` VALUES ('ns1.t1','c3','BOOLEAN','PARTITION',NULL,false,1)");
    verify(insertC1Statement)
        .execute(
            "INSERT INTO `scalardb`.`metadata` VALUES ('ns1.t1','c1','TEXT','CLUSTERING','DESC',false,2)");
    verify(insertC4Statement)
        .execute(
            "INSERT INTO `scalardb`.`metadata` VALUES ('ns1.t1','c4','BLOB','CLUSTERING','ASC',true,3)");
    verify(insertC2Statement)
        .execute(
            "INSERT INTO `scalardb`.`metadata` VALUES ('ns1.t1','c2','BIGINT',NULL,NULL,false,4)");
    verify(insertC5Statement)
        .execute(
            "INSERT INTO `scalardb`.`metadata` VALUES ('ns1.t1','c5','INT',NULL,NULL,false,5)");
    verify(insertC6Statement)
        .execute(
            "INSERT INTO `scalardb`.`metadata` VALUES ('ns1.t1','c6','DOUBLE',NULL,NULL,false,6)");
    verify(insertC7Statement)
        .execute(
            "INSERT INTO `scalardb`.`metadata` VALUES ('ns1.t1','c7','FLOAT',NULL,NULL,false,7)");
  }

  @Test
  public void addTableMetadata_forPostgresql_ShouldAddMetadata() throws Exception {
    // Arrange
    String namespace = "ns1";
    String table = "t1";
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
            .addSecondaryIndex("c4")
            .build();
    JdbcTableMetadataManager manager =
        new JdbcTableMetadataManager(dataSource, Optional.empty(), RdbEngine.POSTGRESQL);
    Statement createSchemaStatement = mock(Statement.class);
    Statement createTableStatement = mock(Statement.class);
    Statement insertC1Statement = mock(Statement.class);
    Statement insertC2Statement = mock(Statement.class);
    Statement insertC3Statement = mock(Statement.class);
    Statement insertC4Statement = mock(Statement.class);
    Statement insertC5Statement = mock(Statement.class);
    Statement insertC6Statement = mock(Statement.class);
    Statement insertC7Statement = mock(Statement.class);
    when(connection1.createStatement())
        .thenReturn(createSchemaStatement)
        .thenReturn(createTableStatement);

    when(connection2.createStatement())
        .thenReturn(insertC3Statement)
        .thenReturn(insertC1Statement)
        .thenReturn(insertC4Statement)
        .thenReturn(insertC2Statement)
        .thenReturn(insertC5Statement)
        .thenReturn(insertC6Statement)
        .thenReturn(insertC7Statement);
    when(dataSource.getConnection()).thenReturn(connection1).thenReturn(connection2);

    // Act
    manager.addTableMetadata(namespace, table, metadata);

    // Assert
    verify(createSchemaStatement).execute("CREATE SCHEMA \"scalardb\"");
    String expectedCreateTableStatement =
        "CREATE TABLE \"scalardb\".\"metadata\"("
            + "\"full_table_name\" VARCHAR(128),"
            + "\"column_name\" VARCHAR(128),"
            + "\"data_type\" VARCHAR(20) NOT NULL,"
            + "\"key_type\" VARCHAR(20),"
            + "\"clustering_order\" VARCHAR(10),"
            + "\"indexed\" BOOLEAN NOT NULL,"
            + "\"ordinal_position\" INTEGER NOT NULL,"
            + "PRIMARY KEY (\"full_table_name\", \"column_name\"))";
    verify(createTableStatement).execute(expectedCreateTableStatement);
    verify(insertC3Statement)
        .execute(
            "INSERT INTO \"scalardb\".\"metadata\" VALUES ('ns1.t1','c3','BOOLEAN','PARTITION',NULL,false,1)");
    verify(insertC1Statement)
        .execute(
            "INSERT INTO \"scalardb\".\"metadata\" VALUES ('ns1.t1','c1','TEXT','CLUSTERING','DESC',false,2)");
    verify(insertC4Statement)
        .execute(
            "INSERT INTO \"scalardb\".\"metadata\" VALUES ('ns1.t1','c4','BLOB','CLUSTERING','ASC',true,3)");
    verify(insertC2Statement)
        .execute(
            "INSERT INTO \"scalardb\".\"metadata\" VALUES ('ns1.t1','c2','BIGINT',NULL,NULL,false,4)");
    verify(insertC5Statement)
        .execute(
            "INSERT INTO \"scalardb\".\"metadata\" VALUES ('ns1.t1','c5','INT',NULL,NULL,false,5)");
    verify(insertC6Statement)
        .execute(
            "INSERT INTO \"scalardb\".\"metadata\" VALUES ('ns1.t1','c6','DOUBLE',NULL,NULL,false,6)");
    verify(insertC7Statement)
        .execute(
            "INSERT INTO \"scalardb\".\"metadata\" VALUES ('ns1.t1','c7','FLOAT',NULL,NULL,false,7)");
  }

  @Test
  public void addTableMetadata_forSqlServer_ShouldAddMetadata() throws Exception {
    // Arrange
    String namespace = "ns1";
    String table = "t1";
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
            .addSecondaryIndex("c4")
            .build();
    JdbcTableMetadataManager manager =
        new JdbcTableMetadataManager(dataSource, Optional.empty(), RdbEngine.SQL_SERVER);
    Statement createSchemaStatement = mock(Statement.class);
    Statement createTableStatement = mock(Statement.class);
    Statement insertC1Statement = mock(Statement.class);
    Statement insertC2Statement = mock(Statement.class);
    Statement insertC3Statement = mock(Statement.class);
    Statement insertC4Statement = mock(Statement.class);
    Statement insertC5Statement = mock(Statement.class);
    Statement insertC6Statement = mock(Statement.class);
    Statement insertC7Statement = mock(Statement.class);
    when(connection1.createStatement())
        .thenReturn(createSchemaStatement)
        .thenReturn(createTableStatement);

    when(connection2.createStatement())
        .thenReturn(insertC3Statement)
        .thenReturn(insertC1Statement)
        .thenReturn(insertC4Statement)
        .thenReturn(insertC2Statement)
        .thenReturn(insertC5Statement)
        .thenReturn(insertC6Statement)
        .thenReturn(insertC7Statement);
    when(dataSource.getConnection()).thenReturn(connection1).thenReturn(connection2);

    // Act
    manager.addTableMetadata(namespace, table, metadata);

    // Assert
    verify(createSchemaStatement).execute("CREATE SCHEMA [scalardb]");
    String expectedCreateTableStatement =
        "CREATE TABLE [scalardb].[metadata]("
            + "[full_table_name] VARCHAR(128),"
            + "[column_name] VARCHAR(128),"
            + "[data_type] VARCHAR(20) NOT NULL,"
            + "[key_type] VARCHAR(20),"
            + "[clustering_order] VARCHAR(10),"
            + "[indexed] BIT NOT NULL,"
            + "[ordinal_position] INTEGER NOT NULL,"
            + "PRIMARY KEY ([full_table_name], [column_name]))";
    verify(createTableStatement).execute(expectedCreateTableStatement);
    verify(insertC3Statement)
        .execute(
            "INSERT INTO [scalardb].[metadata] VALUES ('ns1.t1','c3','BOOLEAN','PARTITION',NULL,0,1)");
    verify(insertC1Statement)
        .execute(
            "INSERT INTO [scalardb].[metadata] VALUES ('ns1.t1','c1','TEXT','CLUSTERING','DESC',0,2)");
    verify(insertC4Statement)
        .execute(
            "INSERT INTO [scalardb].[metadata] VALUES ('ns1.t1','c4','BLOB','CLUSTERING','ASC',1,3)");
    verify(insertC2Statement)
        .execute("INSERT INTO [scalardb].[metadata] VALUES ('ns1.t1','c2','BIGINT',NULL,NULL,0,4)");
    verify(insertC5Statement)
        .execute("INSERT INTO [scalardb].[metadata] VALUES ('ns1.t1','c5','INT',NULL,NULL,0,5)");
    verify(insertC6Statement)
        .execute("INSERT INTO [scalardb].[metadata] VALUES ('ns1.t1','c6','DOUBLE',NULL,NULL,0,6)");
    verify(insertC7Statement)
        .execute("INSERT INTO [scalardb].[metadata] VALUES ('ns1.t1','c7','FLOAT',NULL,NULL,0,7)");
  }

  @Test
  public void addTableMetadata_forOracle_ShouldAddMetadata() throws Exception {
    // Arrange
    String namespace = "ns1";
    String table = "t1";
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
            .addSecondaryIndex("c4")
            .build();
    JdbcTableMetadataManager manager =
        new JdbcTableMetadataManager(dataSource, Optional.empty(), RdbEngine.ORACLE);
    Statement createSchemaStatement = mock(Statement.class);
    Statement alterSchemaStatement = mock(Statement.class);
    Statement createTableStatement = mock(Statement.class);
    Statement insertC1Statement = mock(Statement.class);
    Statement insertC2Statement = mock(Statement.class);
    Statement insertC3Statement = mock(Statement.class);
    Statement insertC4Statement = mock(Statement.class);
    Statement insertC5Statement = mock(Statement.class);
    Statement insertC6Statement = mock(Statement.class);
    Statement insertC7Statement = mock(Statement.class);
    when(connection1.createStatement())
        .thenReturn(createSchemaStatement)
        .thenReturn(alterSchemaStatement)
        .thenReturn(createTableStatement);

    when(connection2.createStatement())
        .thenReturn(insertC3Statement)
        .thenReturn(insertC1Statement)
        .thenReturn(insertC4Statement)
        .thenReturn(insertC2Statement)
        .thenReturn(insertC5Statement)
        .thenReturn(insertC6Statement)
        .thenReturn(insertC7Statement);
    when(dataSource.getConnection()).thenReturn(connection1).thenReturn(connection2);
    // Act
    manager.addTableMetadata(namespace, table, metadata);

    // Assert
    verify(createSchemaStatement).execute("CREATE USER \"scalardb\" IDENTIFIED BY \"oracle\"");
    verify(alterSchemaStatement).execute("ALTER USER \"scalardb\" quota unlimited on USERS");
    String expectedCreateTableStatement =
        "CREATE TABLE \"scalardb\".\"metadata\"("
            + "\"full_table_name\" VARCHAR2(128),"
            + "\"column_name\" VARCHAR2(128),"
            + "\"data_type\" VARCHAR2(20) NOT NULL,"
            + "\"key_type\" VARCHAR2(20),"
            + "\"clustering_order\" VARCHAR2(10),"
            + "\"indexed\" NUMBER(1) NOT NULL,"
            + "\"ordinal_position\" INTEGER NOT NULL,"
            + "PRIMARY KEY (\"full_table_name\", \"column_name\"))";
    verify(createTableStatement).execute(expectedCreateTableStatement);
    verify(insertC3Statement)
        .execute(
            "INSERT INTO \"scalardb\".\"metadata\" VALUES ('ns1.t1','c3','BOOLEAN','PARTITION',NULL,0,1)");
    verify(insertC1Statement)
        .execute(
            "INSERT INTO \"scalardb\".\"metadata\" VALUES ('ns1.t1','c1','TEXT','CLUSTERING','DESC',0,2)");
    verify(insertC4Statement)
        .execute(
            "INSERT INTO \"scalardb\".\"metadata\" VALUES ('ns1.t1','c4','BLOB','CLUSTERING','ASC',1,3)");
    verify(insertC2Statement)
        .execute(
            "INSERT INTO \"scalardb\".\"metadata\" VALUES ('ns1.t1','c2','BIGINT',NULL,NULL,0,4)");
    verify(insertC5Statement)
        .execute(
            "INSERT INTO \"scalardb\".\"metadata\" VALUES ('ns1.t1','c5','INT',NULL,NULL,0,5)");
    verify(insertC6Statement)
        .execute(
            "INSERT INTO \"scalardb\".\"metadata\" VALUES ('ns1.t1','c6','DOUBLE',NULL,NULL,0,6)");
    verify(insertC7Statement)
        .execute(
            "INSERT INTO \"scalardb\".\"metadata\" VALUES ('ns1.t1','c7','FLOAT',NULL,NULL,0,7)");
  }

  @Test
  public void getTableMetadata_withExistingTable_ShouldReturnTableMetadata() throws SQLException {
    // Arrange
    String namespace = "ns1";
    String table = "t1";

    JdbcTableMetadataManager manager =
        new JdbcTableMetadataManager(dataSource, Optional.empty(), RdbEngine.MYSQL);
    PreparedStatement selectStatement = mock(PreparedStatement.class);

    ResultSet resultSet =
        mockResultSet(
            Arrays.asList(
                new ResultSetMocker.Row(
                    "c3", DataType.BOOLEAN.toString(), "PARTITION", null, false),
                new ResultSetMocker.Row(
                    "c1", DataType.TEXT.toString(), "CLUSTERING", Order.DESC.toString(), false),
                new ResultSetMocker.Row(
                    "c4", DataType.BLOB.toString(), "CLUSTERING", Order.ASC.toString(), true),
                new ResultSetMocker.Row("c2", DataType.BIGINT.toString(), null, null, false),
                new ResultSetMocker.Row("c5", DataType.INT.toString(), null, null, false),
                new ResultSetMocker.Row("c6", DataType.DOUBLE.toString(), null, null, false),
                new ResultSetMocker.Row("c7", DataType.FLOAT.toString(), null, null, false)));
    when(selectStatement.executeQuery()).thenReturn(resultSet);
    when(connection1.prepareStatement(any())).thenReturn(selectStatement);

    when(dataSource.getConnection()).thenReturn(connection1).thenReturn(connection2);

    // Act
    TableMetadata actualMetadata = manager.getTableMetadata(namespace, table);

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
  }

  @Test
  public void deleteMetadata_forMysqlWithExistingTable_ShouldDeleteMetadata() throws SQLException {
    deleteMetadata_ForXWithExistingTable_ShouldDeleteMetadata(
        RdbEngine.MYSQL, "DELETE FROM `scalardb`.`metadata` WHERE `full_table_name` = 'ns1.t2'");
  }

  @Test
  public void deleteMetadata_forPostgresqlWithExistingTable_ShouldDeleteMetadata()
      throws SQLException {
    deleteMetadata_ForXWithExistingTable_ShouldDeleteMetadata(
        RdbEngine.POSTGRESQL,
        "DELETE FROM `scalardb`.`metadata` WHERE `full_table_name` = 'ns1.t2'");
  }

  @Test
  public void deleteMetadata_forSqlServerWithExistingTable_ShouldDeleteMetadata()
      throws SQLException {
    deleteMetadata_ForXWithExistingTable_ShouldDeleteMetadata(
        RdbEngine.SQL_SERVER,
        "DELETE FROM `scalardb`.`metadata` WHERE `full_table_name` = 'ns1.t2'");
  }

  @Test
  public void deleteMetadata_forOracleWithExistingTable_ShouldDeleteMetadata() throws SQLException {
    deleteMetadata_ForXWithExistingTable_ShouldDeleteMetadata(
        RdbEngine.ORACLE, "DELETE FROM `scalardb`.`metadata` WHERE `full_table_name` = 'ns1.t2'");
  }

  private void deleteMetadata_ForXWithExistingTable_ShouldDeleteMetadata(
      RdbEngine rdbEngine, String deleteQuery) throws SQLException {
    // Arrange
    String namespace = "ns1";
    String table = "t2";

    JdbcTableMetadataManager manager =
        new JdbcTableMetadataManager(dataSource, Optional.empty(), RdbEngine.MYSQL);
    PreparedStatement selectStatement = mock(PreparedStatement.class);

    ResultSet resultSet =
        mockResultSet(
            Collections.singletonList(
                new Row("c1", DataType.BOOLEAN.toString(), "PARTITION", null, false)));
    when(selectStatement.executeQuery()).thenReturn(resultSet);
    when(connection1.prepareStatement(any())).thenReturn(selectStatement);

    when(dataSource.getConnection()).thenReturn(connection1).thenReturn(connection2);
    Statement deleteStatement = mock(Statement.class);
    when(connection2.createStatement()).thenReturn(deleteStatement);
    when(dataSource.getConnection()).thenReturn(connection1, connection2);

    // Act
    TableMetadata beforeDeletionMedatata = manager.getTableMetadata(namespace, table);
    manager.deleteTableMetadata(namespace, table);
    TableMetadata afterDeletionMetadata = manager.getTableMetadata(namespace, table);

    // Assert
    assertThat(beforeDeletionMedatata)
        .isEqualTo(
            TableMetadata.newBuilder()
                .addPartitionKey("c1")
                .addColumn("c1", DataType.BOOLEAN)
                .build());
    verify(deleteStatement).execute(deleteQuery);
    assertThat(afterDeletionMetadata).isNull();
  }

  public ResultSet mockResultSet(List<ResultSetMocker.Row> rows) throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);
    // Everytime the ResultSet.next() method will be called, the ResultSet.getXXX methods call be
    // mocked to return the current row data
    doAnswer(new ResultSetMocker(rows)).when(resultSet).next();
    return resultSet;
  }
  // Utility class used to mock ResultSet
  static class ResultSetMocker implements org.mockito.stubbing.Answer<Object> {
    List<Row> rows;
    int row = -1;

    public ResultSetMocker(List<Row> rows) {
      this.rows = rows;
    }

    @Override
    public Object answer(InvocationOnMock invocation) throws Throwable {
      row++;
      if (row >= rows.size()) {
        return false;
      }
      Row currentRow = rows.get(row);
      ResultSet mock = (ResultSet) invocation.getMock();
      when(mock.getString(JdbcTableMetadataManager.COLUMN_NAME)).thenReturn(currentRow.columnName);
      when(mock.getString(JdbcTableMetadataManager.DATA_TYPE)).thenReturn(currentRow.dataType);
      when(mock.getString(JdbcTableMetadataManager.KEY_TYPE)).thenReturn(currentRow.keyType);
      when(mock.getString(JdbcTableMetadataManager.CLUSTERING_ORDER))
          .thenReturn(currentRow.clusteringOrder);
      when(mock.getBoolean(JdbcTableMetadataManager.INDEXED)).thenReturn(currentRow.indexed);
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
}
