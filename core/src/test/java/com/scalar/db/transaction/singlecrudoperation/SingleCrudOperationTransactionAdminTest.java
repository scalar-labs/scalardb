package com.scalar.db.transaction.singlecrudoperation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class SingleCrudOperationTransactionAdminTest {

  @Mock private DistributedStorageAdmin distributedStorageAdmin;
  private SingleCrudOperationTransactionAdmin admin;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    admin = new SingleCrudOperationTransactionAdmin(distributedStorageAdmin);
  }

  @Test
  public void createNamespace_ShouldCallDistributedStorageAdminProperly()
      throws ExecutionException {
    // Arrange

    // Act
    admin.createNamespace("ns", Collections.emptyMap());

    // Assert
    verify(distributedStorageAdmin).createNamespace("ns", Collections.emptyMap());
  }

  @Test
  public void createTable_ShouldCallDistributedStorageAdminProperly() throws ExecutionException {
    // Arrange
    TableMetadata metadata =
        TableMetadata.newBuilder().addColumn("c1", DataType.INT).addPartitionKey("c1").build();

    // Act
    admin.createTable("ns", "tbl", metadata, Collections.emptyMap());

    // Assert
    verify(distributedStorageAdmin).createTable("ns", "tbl", metadata, Collections.emptyMap());
  }

  @Test
  public void dropTable_ShouldCallDistributedStorageAdminProperly() throws ExecutionException {
    // Arrange

    // Act
    admin.dropTable("ns", "tbl");

    // Assert
    verify(distributedStorageAdmin).dropTable("ns", "tbl");
  }

  @Test
  public void dropNamespace_ShouldCallDistributedStorageAdminProperly() throws ExecutionException {
    // Arrange

    // Act
    admin.dropNamespace("ns");

    // Assert
    verify(distributedStorageAdmin).dropNamespace("ns");
  }

  @Test
  public void truncateTable_ShouldCallDistributedStorageAdminProperly() throws ExecutionException {
    // Arrange

    // Act
    admin.truncateTable("ns", "tbl");

    // Assert
    verify(distributedStorageAdmin).truncateTable("ns", "tbl");
  }

  @Test
  public void createIndex_ShouldCallDistributedStorageAdminProperly() throws ExecutionException {
    // Arrange

    // Act
    admin.createIndex("ns", "tbl", "col", Collections.emptyMap());

    // Assert
    verify(distributedStorageAdmin).createIndex("ns", "tbl", "col", Collections.emptyMap());
  }

  @Test
  public void dropIndex_ShouldCallDistributedStorageAdminProperly() throws ExecutionException {
    // Arrange

    // Act
    admin.dropIndex("ns", "tbl", "col");

    // Assert
    verify(distributedStorageAdmin).dropIndex("ns", "tbl", "col");
  }

  @Test
  public void getTableMetadata_ShouldCallDistributedStorageAdminProperly()
      throws ExecutionException {
    // Arrange
    TableMetadata metadata =
        TableMetadata.newBuilder().addColumn("c1", DataType.INT).addPartitionKey("c1").build();
    when(distributedStorageAdmin.getTableMetadata(any(), any())).thenReturn(metadata);

    // Act
    TableMetadata actual = admin.getTableMetadata("ns", "tbl");

    // Assert
    verify(distributedStorageAdmin).getTableMetadata("ns", "tbl");
    assertThat(actual).isEqualTo(metadata);
  }

  @Test
  public void getNamespaceTableNames_ShouldCallDistributedStorageAdminProperly()
      throws ExecutionException {
    // Arrange
    Set<String> tableNames = ImmutableSet.of("tbl1", "tbl2", "tbl3");
    when(distributedStorageAdmin.getNamespaceTableNames(any())).thenReturn(tableNames);

    // Act
    Set<String> actual = admin.getNamespaceTableNames("ns");

    // Assert
    verify(distributedStorageAdmin).getNamespaceTableNames("ns");
    assertThat(actual).isEqualTo(tableNames);
  }

  @Test
  public void namespaceExists_ShouldCallDistributedStorageAdminProperly()
      throws ExecutionException {
    // Arrange
    when(distributedStorageAdmin.namespaceExists(any())).thenReturn(true);

    // Act
    boolean actual = admin.namespaceExists("ns");

    // Assert
    verify(distributedStorageAdmin).namespaceExists("ns");
    assertThat(actual).isTrue();
  }

  @Test
  public void createCoordinatorTables_ShouldDoNothing() {
    // Arrange

    // Act Assert
    assertThatCode(() -> admin.createCoordinatorTables(Collections.emptyMap()))
        .doesNotThrowAnyException();
  }

  @Test
  public void dropCoordinatorTables_ShouldDoNothing() {
    // Arrange

    // Act Assert
    assertThatCode(() -> admin.dropCoordinatorTables()).doesNotThrowAnyException();
  }

  @Test
  public void truncateCoordinatorTables_ShouldDoNothing() {
    // Arrange

    // Act Assert
    assertThatCode(() -> admin.truncateCoordinatorTables()).doesNotThrowAnyException();
  }

  @Test
  public void coordinatorTablesExist_ShouldReturnTrue() {
    // Arrange

    // Act
    boolean actual = admin.coordinatorTablesExist();

    // Assert
    assertThat(actual).isTrue();
  }

  @Test
  public void repairTable_ShouldCallDistributedStorageAdminProperly() throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "tbl";
    TableMetadata metadata =
        TableMetadata.newBuilder().addColumn("c1", DataType.INT).addPartitionKey("c1").build();
    Map<String, String> options = ImmutableMap.of("foo", "bar");

    // Act
    admin.repairTable(namespace, table, metadata, options);

    // Assert
    verify(distributedStorageAdmin).repairTable(namespace, table, metadata, options);
  }

  @Test
  public void repairCoordinatorTables_ShouldDoNothing() {
    // Arrange

    // Act
    assertThatCode(() -> admin.repairCoordinatorTables(Collections.emptyMap()))
        .doesNotThrowAnyException();
  }

  @Test
  public void addNewColumnToTable_ShouldCallDistributedStorageAdminProperly()
      throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "tbl";
    String column = "c1";
    DataType dataType = DataType.TEXT;

    // Act
    admin.addNewColumnToTable(namespace, table, column, dataType);

    // Assert
    verify(distributedStorageAdmin).addNewColumnToTable(namespace, table, column, dataType);
  }

  @Test
  public void importTable_ShouldCallDistributedStorageAdminProperly() throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "tbl";
    Map<String, String> options = ImmutableMap.of("a", "b");
    Map<String, DataType> overrideColumnsType = ImmutableMap.of("c", DataType.TIMESTAMPTZ);

    // Act
    admin.importTable(namespace, table, options, overrideColumnsType);

    // Assert
    verify(distributedStorageAdmin).importTable(namespace, table, options, overrideColumnsType);
  }

  @Test
  public void getNamespaceNames_ShouldCallDistributedStorageAdminProperly()
      throws ExecutionException {
    // Arrange
    Set<String> namespaceNames = ImmutableSet.of("n1", "n2");
    when(distributedStorageAdmin.getNamespaceNames()).thenReturn(namespaceNames);

    // Act
    Set<String> actualNamespaces = admin.getNamespaceNames();

    // Assert
    verify(distributedStorageAdmin).getNamespaceNames();
    assertThat(actualNamespaces).containsOnly("n1", "n2");
  }

  @Test
  public void repairNamespace_ShouldCallDistributedStorageAdminProperly()
      throws ExecutionException {
    // Arrange
    String namespace = "ns";
    Map<String, String> options = ImmutableMap.of("foo", "bar");

    // Act
    admin.repairNamespace(namespace, options);

    // Assert
    verify(distributedStorageAdmin).repairNamespace(namespace, options);
  }

  @Test
  public void upgrade_ShouldCallDistributedStorageAdminProperly() throws ExecutionException {
    // Arrange
    Map<String, String> options = ImmutableMap.of("foo", "bar");

    // Act
    admin.upgrade(options);

    // Assert
    verify(distributedStorageAdmin).upgrade(options);
  }
}
