package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.VirtualTableInfo;
import com.scalar.db.api.VirtualTableJoinType;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class VirtualTableMetadataServiceTest {

  private static final String METADATA_SCHEMA = "scalardb";

  @Mock private Connection connection;

  private RdbEngineStrategy rdbEngine;
  private VirtualTableMetadataService service;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    rdbEngine = new RdbEngineMysql();
    service = new VirtualTableMetadataService(METADATA_SCHEMA, rdbEngine);
  }

  @Test
  public void createVirtualTablesTableIfNotExists_TableDoesNotExist_ShouldCreateTable()
      throws Exception {
    // Arrange
    Statement createStatement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(createStatement);

    // Act
    service.createVirtualTablesTableIfNotExists(connection);

    // Assert
    ArgumentCaptor<String> createCaptor = ArgumentCaptor.forClass(String.class);
    verify(createStatement).execute(createCaptor.capture());
    assertThat(createCaptor.getValue())
        .isEqualTo(
            "CREATE TABLE IF NOT EXISTS `scalardb`.`virtual_tables`("
                + "`full_table_name` VARCHAR(128), "
                + "`left_source_table_full_table_name` VARCHAR(128), "
                + "`right_source_table_full_table_name` VARCHAR(128), "
                + "`join_type` VARCHAR(20), "
                + "`attributes` LONGTEXT, "
                + "PRIMARY KEY (`full_table_name`))");
  }

  @Test
  public void createVirtualTablesTableIfNotExists_TableExists_ShouldSuppressDuplicateTableError()
      throws Exception {
    // Arrange
    Statement createStatement = mock(Statement.class);
    SQLException duplicateTableException = new SQLException("Table already exists", "42S01", 1050);
    when(createStatement.execute(anyString())).thenThrow(duplicateTableException);
    when(connection.createStatement()).thenReturn(createStatement);

    // Act
    service.createVirtualTablesTableIfNotExists(connection);

    // Assert
    ArgumentCaptor<String> createCaptor = ArgumentCaptor.forClass(String.class);
    verify(createStatement).execute(createCaptor.capture());
    assertThat(createCaptor.getValue())
        .isEqualTo(
            "CREATE TABLE IF NOT EXISTS `scalardb`.`virtual_tables`("
                + "`full_table_name` VARCHAR(128), "
                + "`left_source_table_full_table_name` VARCHAR(128), "
                + "`right_source_table_full_table_name` VARCHAR(128), "
                + "`join_type` VARCHAR(20), "
                + "`attributes` LONGTEXT, "
                + "PRIMARY KEY (`full_table_name`))");
  }

  @Test
  public void deleteVirtualTablesTableIfEmpty_TableIsEmpty_ShouldDeleteTable() throws Exception {
    // Arrange
    Statement selectStatement = mock(Statement.class);
    Statement dropStatement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(selectStatement, dropStatement);
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(false);
    when(selectStatement.executeQuery(anyString())).thenReturn(resultSet);

    // Act
    service.deleteVirtualTablesTableIfEmpty(connection);

    // Assert
    ArgumentCaptor<String> selectCaptor = ArgumentCaptor.forClass(String.class);
    verify(selectStatement).executeQuery(selectCaptor.capture());
    assertThat(selectCaptor.getValue()).isEqualTo("SELECT * FROM `scalardb`.`virtual_tables`");

    ArgumentCaptor<String> dropCaptor = ArgumentCaptor.forClass(String.class);
    verify(dropStatement).execute(dropCaptor.capture());
    assertThat(dropCaptor.getValue()).isEqualTo("DROP TABLE `scalardb`.`virtual_tables`");
  }

  @Test
  public void deleteVirtualTablesTableIfEmpty_TableIsNotEmpty_ShouldNotDeleteTable()
      throws Exception {
    // Arrange
    Statement selectStatement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(selectStatement);
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(true);
    when(selectStatement.executeQuery(anyString())).thenReturn(resultSet);

    // Act
    service.deleteVirtualTablesTableIfEmpty(connection);

    // Assert
    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(selectStatement).executeQuery(captor.capture());
    assertThat(captor.getValue()).isEqualTo("SELECT * FROM `scalardb`.`virtual_tables`");
    verify(selectStatement, never()).execute(anyString());
  }

  @Test
  public void insertIntoVirtualTablesTable_ShouldInsertVirtualTableMetadata() throws Exception {
    // Arrange
    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);

    String namespace = "ns";
    String table = "vtable";
    String leftSourceNamespace = "ns";
    String leftSourceTable = "left_table";
    String rightSourceNamespace = "ns";
    String rightSourceTable = "right_table";
    VirtualTableJoinType joinType = VirtualTableJoinType.INNER;
    String attributes = "";

    // Act
    service.insertIntoVirtualTablesTable(
        connection,
        namespace,
        table,
        leftSourceNamespace,
        leftSourceTable,
        rightSourceNamespace,
        rightSourceTable,
        joinType,
        attributes);

    // Assert
    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(connection).prepareStatement(captor.capture());
    assertThat(captor.getValue())
        .isEqualTo("INSERT INTO `scalardb`.`virtual_tables` VALUES (?,?,?,?,?)");
    verify(preparedStatement).setString(1, "ns.vtable");
    verify(preparedStatement).setString(2, "ns.left_table");
    verify(preparedStatement).setString(3, "ns.right_table");
    verify(preparedStatement).setString(4, "INNER");
    verify(preparedStatement).setString(5, "");
    verify(preparedStatement).execute();
  }

  @Test
  public void deleteFromVirtualTablesTable_ShouldDeleteVirtualTableMetadata() throws Exception {
    // Arrange
    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);

    String namespace = "ns";
    String table = "vtable";

    // Act
    service.deleteFromVirtualTablesTable(connection, namespace, table);

    // Assert
    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(connection).prepareStatement(captor.capture());
    assertThat(captor.getValue())
        .isEqualTo("DELETE FROM `scalardb`.`virtual_tables` WHERE `full_table_name` = ?");
    verify(preparedStatement).setString(1, "ns.vtable");
    verify(preparedStatement).execute();
  }

  @Test
  public void getVirtualTableInfo_VirtualTableExists_ShouldReturnVirtualTableInfo()
      throws Exception {
    // Arrange
    Statement checkStatement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(checkStatement);

    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getString("full_table_name")).thenReturn("ns.vtable");
    when(resultSet.getString("left_source_table_full_table_name")).thenReturn("ns.left_table");
    when(resultSet.getString("right_source_table_full_table_name")).thenReturn("ns.right_table");
    when(resultSet.getString("join_type")).thenReturn("INNER");
    when(preparedStatement.executeQuery()).thenReturn(resultSet);

    String namespace = "ns";
    String table = "vtable";

    // Act
    VirtualTableInfo virtualTableInfo = service.getVirtualTableInfo(connection, namespace, table);

    // Assert
    assertThat(virtualTableInfo).isNotNull();
    assertThat(virtualTableInfo.getNamespaceName()).isEqualTo("ns");
    assertThat(virtualTableInfo.getTableName()).isEqualTo("vtable");
    assertThat(virtualTableInfo.getLeftSourceNamespaceName()).isEqualTo("ns");
    assertThat(virtualTableInfo.getLeftSourceTableName()).isEqualTo("left_table");
    assertThat(virtualTableInfo.getRightSourceNamespaceName()).isEqualTo("ns");
    assertThat(virtualTableInfo.getRightSourceTableName()).isEqualTo("right_table");
    assertThat(virtualTableInfo.getJoinType()).isEqualTo(VirtualTableJoinType.INNER);

    ArgumentCaptor<String> checkCaptor = ArgumentCaptor.forClass(String.class);
    verify(checkStatement).execute(checkCaptor.capture());
    assertThat(checkCaptor.getValue())
        .isEqualTo("SELECT 1 FROM `scalardb`.`virtual_tables` LIMIT 1");

    ArgumentCaptor<String> selectCaptor = ArgumentCaptor.forClass(String.class);
    verify(connection).prepareStatement(selectCaptor.capture());
    assertThat(selectCaptor.getValue())
        .isEqualTo("SELECT * FROM `scalardb`.`virtual_tables` WHERE `full_table_name` = ?");
    verify(preparedStatement).setString(1, "ns.vtable");
  }

  @Test
  public void getVirtualTableInfo_VirtualTableDoesNotExist_ShouldReturnNull() throws Exception {
    // Arrange
    Statement checkStatement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(checkStatement);

    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(false);
    when(preparedStatement.executeQuery()).thenReturn(resultSet);

    String namespace = "ns";
    String table = "vtable";

    // Act
    VirtualTableInfo virtualTableInfo = service.getVirtualTableInfo(connection, namespace, table);

    // Assert
    assertThat(virtualTableInfo).isNull();

    ArgumentCaptor<String> checkCaptor = ArgumentCaptor.forClass(String.class);
    verify(checkStatement).execute(checkCaptor.capture());
    assertThat(checkCaptor.getValue())
        .isEqualTo("SELECT 1 FROM `scalardb`.`virtual_tables` LIMIT 1");

    ArgumentCaptor<String> selectCaptor = ArgumentCaptor.forClass(String.class);
    verify(connection).prepareStatement(selectCaptor.capture());
    assertThat(selectCaptor.getValue())
        .isEqualTo("SELECT * FROM `scalardb`.`virtual_tables` WHERE `full_table_name` = ?");
    verify(preparedStatement).setString(1, "ns.vtable");
  }

  @Test
  public void getVirtualTableInfo_MetadataTableDoesNotExist_ShouldReturnNull() throws Exception {
    // Arrange
    Statement statement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(statement);
    SQLException sqlException = new SQLException("Table doesn't exist", "42S02", 1146);
    when(statement.execute(anyString())).thenThrow(sqlException);

    String namespace = "ns";
    String table = "vtable";

    // Act
    VirtualTableInfo virtualTableInfo = service.getVirtualTableInfo(connection, namespace, table);

    // Assert
    assertThat(virtualTableInfo).isNull();

    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(statement).execute(captor.capture());
    assertThat(captor.getValue()).isEqualTo("SELECT 1 FROM `scalardb`.`virtual_tables` LIMIT 1");
  }

  @Test
  public void getVirtualTableInfosBySourceTable_ShouldReturnVirtualTableInfos() throws Exception {
    // Arrange
    Statement checkStatement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(checkStatement);

    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(true, true, false);
    when(resultSet.getString("full_table_name")).thenReturn("ns.vtable1", "ns.vtable2");
    when(resultSet.getString("left_source_table_full_table_name"))
        .thenReturn("ns.source_table", "ns.left_table");
    when(resultSet.getString("right_source_table_full_table_name"))
        .thenReturn("ns.right_table", "ns.source_table");
    when(resultSet.getString("join_type")).thenReturn("INNER", "LEFT_OUTER");
    when(preparedStatement.executeQuery()).thenReturn(resultSet);

    String sourceNamespace = "ns";
    String sourceTable = "source_table";

    // Act
    List<VirtualTableInfo> virtualTableInfos =
        service.getVirtualTableInfosBySourceTable(connection, sourceNamespace, sourceTable);

    // Assert
    assertThat(virtualTableInfos).hasSize(2);
    assertThat(virtualTableInfos.get(0).getNamespaceName()).isEqualTo("ns");
    assertThat(virtualTableInfos.get(0).getTableName()).isEqualTo("vtable1");
    assertThat(virtualTableInfos.get(0).getLeftSourceTableName()).isEqualTo("source_table");
    assertThat(virtualTableInfos.get(0).getJoinType()).isEqualTo(VirtualTableJoinType.INNER);
    assertThat(virtualTableInfos.get(1).getTableName()).isEqualTo("vtable2");
    assertThat(virtualTableInfos.get(1).getRightSourceTableName()).isEqualTo("source_table");
    assertThat(virtualTableInfos.get(1).getJoinType()).isEqualTo(VirtualTableJoinType.LEFT_OUTER);

    ArgumentCaptor<String> checkCaptor = ArgumentCaptor.forClass(String.class);
    verify(checkStatement).execute(checkCaptor.capture());
    assertThat(checkCaptor.getValue())
        .isEqualTo("SELECT 1 FROM `scalardb`.`virtual_tables` LIMIT 1");

    ArgumentCaptor<String> selectCaptor = ArgumentCaptor.forClass(String.class);
    verify(connection).prepareStatement(selectCaptor.capture());
    assertThat(selectCaptor.getValue())
        .isEqualTo(
            "SELECT * FROM `scalardb`.`virtual_tables` WHERE `left_source_table_full_table_name` = ? OR `right_source_table_full_table_name` = ?");
    verify(preparedStatement).setString(1, "ns.source_table");
    verify(preparedStatement).setString(2, "ns.source_table");
  }

  @Test
  public void getVirtualTableInfosBySourceTable_MetadataTableDoesNotExist_ShouldReturnEmptyList()
      throws Exception {
    // Arrange
    Statement checkStatement = mock(Statement.class);
    SQLException sqlException = new SQLException("Table doesn't exist", "42S02", 1146);
    when(checkStatement.execute(anyString())).thenThrow(sqlException);
    when(connection.createStatement()).thenReturn(checkStatement);

    String sourceNamespace = "ns";
    String sourceTable = "source_table";

    // Act
    List<VirtualTableInfo> virtualTableInfos =
        service.getVirtualTableInfosBySourceTable(connection, sourceNamespace, sourceTable);

    // Assert
    assertThat(virtualTableInfos).isEmpty();

    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(checkStatement).execute(captor.capture());
    assertThat(captor.getValue()).isEqualTo("SELECT 1 FROM `scalardb`.`virtual_tables` LIMIT 1");
  }

  @Test
  public void getNamespaceTableNames_ShouldReturnTableNames() throws Exception {
    // Arrange
    Statement checkStatement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(checkStatement);

    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(true, true, false);
    when(resultSet.getString("full_table_name")).thenReturn("ns.table1", "ns.table2");
    when(preparedStatement.executeQuery()).thenReturn(resultSet);

    String namespace = "ns";

    // Act
    Set<String> tableNames = service.getNamespaceTableNames(connection, namespace);

    // Assert
    assertThat(tableNames).containsExactlyInAnyOrder("table1", "table2");

    ArgumentCaptor<String> checkCaptor = ArgumentCaptor.forClass(String.class);
    verify(checkStatement).execute(checkCaptor.capture());
    assertThat(checkCaptor.getValue())
        .isEqualTo("SELECT 1 FROM `scalardb`.`virtual_tables` LIMIT 1");

    ArgumentCaptor<String> selectCaptor = ArgumentCaptor.forClass(String.class);
    verify(connection).prepareStatement(selectCaptor.capture());
    assertThat(selectCaptor.getValue())
        .isEqualTo(
            "SELECT `full_table_name` FROM `scalardb`.`virtual_tables` WHERE `full_table_name` LIKE ?");
    verify(preparedStatement).setString(1, "ns.%");
  }

  @Test
  public void getNamespaceTableNames_MetadataTableDoesNotExist_ShouldReturnEmptySet()
      throws Exception {
    // Arrange
    Statement checkStatement = mock(Statement.class);
    SQLException sqlException = new SQLException("Table doesn't exist", "42S02", 1146);
    when(checkStatement.execute(anyString())).thenThrow(sqlException);
    when(connection.createStatement()).thenReturn(checkStatement);

    String namespace = "ns";

    // Act
    Set<String> tableNames = service.getNamespaceTableNames(connection, namespace);

    // Assert
    assertThat(tableNames).isEmpty();

    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(checkStatement).execute(captor.capture());
    assertThat(captor.getValue()).isEqualTo("SELECT 1 FROM `scalardb`.`virtual_tables` LIMIT 1");
  }

  @Test
  public void getNamespaceNamesOfExistingTables_ShouldReturnNamespaceNames() throws Exception {
    // Arrange
    Statement checkStatement = mock(Statement.class);
    Statement selectStatement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(checkStatement, selectStatement);

    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(true, true, true, false);
    when(resultSet.getString("full_table_name"))
        .thenReturn("ns1.table1", "ns1.table2", "ns2.table1");
    when(selectStatement.executeQuery(anyString())).thenReturn(resultSet);

    // Act
    Set<String> namespaceNames = service.getNamespaceNamesOfExistingTables(connection);

    // Assert
    assertThat(namespaceNames).containsExactlyInAnyOrder("ns1", "ns2");

    ArgumentCaptor<String> checkCaptor = ArgumentCaptor.forClass(String.class);
    verify(checkStatement).execute(checkCaptor.capture());
    assertThat(checkCaptor.getValue())
        .isEqualTo("SELECT 1 FROM `scalardb`.`virtual_tables` LIMIT 1");

    ArgumentCaptor<String> selectCaptor = ArgumentCaptor.forClass(String.class);
    verify(selectStatement).executeQuery(selectCaptor.capture());
    assertThat(selectCaptor.getValue())
        .isEqualTo("SELECT `full_table_name` FROM `scalardb`.`virtual_tables`");
  }

  @Test
  public void getNamespaceNamesOfExistingTables_MetadataTableDoesNotExist_ShouldReturnEmptySet()
      throws Exception {
    // Arrange
    Statement checkStatement = mock(Statement.class);
    SQLException sqlException = new SQLException("Table doesn't exist", "42S02", 1146);
    when(checkStatement.execute(anyString())).thenThrow(sqlException);
    when(connection.createStatement()).thenReturn(checkStatement);

    // Act
    Set<String> namespaceNames = service.getNamespaceNamesOfExistingTables(connection);

    // Assert
    assertThat(namespaceNames).isEmpty();

    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(checkStatement).execute(captor.capture());
    assertThat(captor.getValue()).isEqualTo("SELECT 1 FROM `scalardb`.`virtual_tables` LIMIT 1");
  }
}
