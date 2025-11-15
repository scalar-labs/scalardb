package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.StorageInfo;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.VirtualTableInfo;
import com.scalar.db.api.VirtualTableJoinType;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CommonDistributedStorageAdminTest {

  private static final String SYSTEM_NAMESPACE = "scalardb";

  @Mock private DistributedStorageAdmin admin;
  @Mock private DatabaseConfig databaseConfig;

  private CommonDistributedStorageAdmin commonDistributedStorageAdmin;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    when(databaseConfig.getSystemNamespaceName()).thenReturn(SYSTEM_NAMESPACE);
    commonDistributedStorageAdmin = new CommonDistributedStorageAdmin(admin, databaseConfig);
  }

  @Test
  public void createNamespace_SystemNamespaceNameGiven_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> commonDistributedStorageAdmin.createNamespace(SYSTEM_NAMESPACE))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void dropNamespace_SystemNamespaceNameGiven_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> commonDistributedStorageAdmin.dropNamespace(SYSTEM_NAMESPACE))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void createTable_ShouldCallAdminProperly() throws ExecutionException {
    // Arrange
    String namespaceName = "ns";
    String tableName = "tbl";
    TableMetadata tableMetadata = mock(TableMetadata.class);
    Map<String, String> options = ImmutableMap.of("name", "value");

    when(admin.namespaceExists(namespaceName)).thenReturn(true);
    when(admin.tableExists(namespaceName, tableName)).thenReturn(true);

    // Act
    commonDistributedStorageAdmin.createTable(namespaceName, tableName, tableMetadata, options);

    // Assert
    verify(admin).createTable(namespaceName, tableName, tableMetadata, options);
  }

  @Test
  public void
      createTable_TableMetadataWithEncryptedColumns_ShouldThrowUnsupportedOperationException()
          throws ExecutionException {
    // Arrange
    String namespaceName = "ns";
    String tableName = "tbl";

    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(tableMetadata.getEncryptedColumnNames()).thenReturn(Collections.singleton("col"));

    Map<String, String> options = ImmutableMap.of("name", "value");

    when(admin.namespaceExists(namespaceName)).thenReturn(true);
    when(admin.tableExists(namespaceName, tableName)).thenReturn(true);

    // Act Assert
    assertThatThrownBy(
            () ->
                commonDistributedStorageAdmin.createTable(
                    namespaceName, tableName, tableMetadata, options))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void namespaceExists_SystemNamespaceNameGiven_ShouldReturnTrue()
      throws ExecutionException {
    // Arrange

    // Act
    boolean actual = commonDistributedStorageAdmin.namespaceExists(SYSTEM_NAMESPACE);

    // Assert
    assertThat(actual).isTrue();
  }

  @Test
  public void getNamespaceNames_ShouldReturnListWithSystemNamespaceName()
      throws ExecutionException {
    // Arrange
    when(admin.getNamespaceNames()).thenReturn(Collections.emptySet());

    // Act
    Set<String> actual = commonDistributedStorageAdmin.getNamespaceNames();

    // Assert
    assertThat(actual).containsExactly(SYSTEM_NAMESPACE);
  }

  @Test
  public void repairTable_ShouldCallAdminProperly() throws ExecutionException {
    // Arrange
    String namespaceName = "ns";
    String tableName = "tbl";

    TableMetadata tableMetadata = mock(TableMetadata.class);
    Map<String, String> options = ImmutableMap.of("name", "value");

    // Act
    commonDistributedStorageAdmin.repairTable(namespaceName, tableName, tableMetadata, options);

    // Assert
    verify(admin).repairTable(namespaceName, tableName, tableMetadata, options);
  }

  @Test
  public void
      repairTable_TableMetadataWithEncryptedColumns_ShouldThrowUnsupportedOperationException() {
    // Arrange
    String namespaceName = "ns";
    String tableName = "tbl";

    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(tableMetadata.getEncryptedColumnNames()).thenReturn(Collections.singleton("col"));

    Map<String, String> options = ImmutableMap.of("name", "value");

    // Act Assert
    assertThatThrownBy(
            () ->
                commonDistributedStorageAdmin.repairTable(
                    namespaceName, tableName, tableMetadata, options))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void createVirtualTable_ProperArgumentsGiven_ShouldCallAdminProperly()
      throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "vtable";
    String leftSourceNamespace = "ns";
    String leftSourceTable = "left_table";
    String rightSourceNamespace = "ns";
    String rightSourceTable = "right_table";
    VirtualTableJoinType joinType = VirtualTableJoinType.INNER;
    Map<String, String> options = Collections.emptyMap();

    StorageInfo storageInfo = mock(StorageInfo.class);
    when(storageInfo.getStorageName()).thenReturn("test-storage");
    when(storageInfo.getAtomicityUnit()).thenReturn(StorageInfo.AtomicityUnit.NAMESPACE);
    when(admin.getStorageInfo(leftSourceNamespace)).thenReturn(storageInfo);
    when(admin.namespaceExists(namespace)).thenReturn(true);

    // Mock getNamespaceTableNames for tableExists() to work
    // All tables (namespace, leftSourceNamespace, rightSourceNamespace) are in "ns"
    // Return source tables that exist, but not the target table
    when(admin.getNamespaceTableNames("ns"))
        .thenReturn(new HashSet<>(Arrays.asList(leftSourceTable, rightSourceTable)));

    TableMetadata leftTableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addClusteringKey("ck1", Order.ASC)
            .addColumn("pk1", DataType.INT)
            .addColumn("ck1", DataType.TEXT)
            .addColumn("col1", DataType.INT)
            .build();
    when(admin.getTableMetadata(leftSourceNamespace, leftSourceTable))
        .thenReturn(leftTableMetadata);

    TableMetadata rightTableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addClusteringKey("ck1", Order.ASC)
            .addColumn("pk1", DataType.INT)
            .addColumn("ck1", DataType.TEXT)
            .addColumn("col2", DataType.TEXT)
            .build();
    when(admin.getTableMetadata(rightSourceNamespace, rightSourceTable))
        .thenReturn(rightTableMetadata);

    when(admin.getVirtualTableInfo(leftSourceNamespace, leftSourceTable))
        .thenReturn(Optional.empty());
    when(admin.getVirtualTableInfo(rightSourceNamespace, rightSourceTable))
        .thenReturn(Optional.empty());

    // Act
    commonDistributedStorageAdmin.createVirtualTable(
        namespace,
        table,
        leftSourceNamespace,
        leftSourceTable,
        rightSourceNamespace,
        rightSourceTable,
        joinType,
        options);

    // Assert
    verify(admin)
        .createVirtualTable(
            namespace,
            table,
            leftSourceNamespace,
            leftSourceTable,
            rightSourceNamespace,
            rightSourceTable,
            joinType,
            options);
  }

  @Test
  public void
      createVirtualTable_SourceTablesWithDifferentPartitionKeys_ShouldThrowIllegalArgumentException()
          throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "vtable";
    String leftSourceNamespace = "ns";
    String leftSourceTable = "left_table";
    String rightSourceNamespace = "ns";
    String rightSourceTable = "right_table";
    VirtualTableJoinType joinType = VirtualTableJoinType.INNER;
    Map<String, String> options = Collections.emptyMap();

    StorageInfo storageInfo = mock(StorageInfo.class);
    when(storageInfo.getAtomicityUnit()).thenReturn(StorageInfo.AtomicityUnit.NAMESPACE);
    when(admin.getStorageInfo(leftSourceNamespace)).thenReturn(storageInfo);
    when(admin.namespaceExists(namespace)).thenReturn(true);
    when(admin.tableExists(namespace, table)).thenReturn(false);

    TableMetadata leftTableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addColumn("pk1", DataType.INT)
            .addColumn("col1", DataType.INT)
            .build();
    when(admin.getTableMetadata(leftSourceNamespace, leftSourceTable))
        .thenReturn(leftTableMetadata);

    TableMetadata rightTableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk2")
            .addColumn("pk2", DataType.INT)
            .addColumn("col2", DataType.TEXT)
            .build();
    when(admin.getTableMetadata(rightSourceNamespace, rightSourceTable))
        .thenReturn(rightTableMetadata);

    // Act Assert
    assertThatThrownBy(
            () ->
                commonDistributedStorageAdmin.createVirtualTable(
                    namespace,
                    table,
                    leftSourceNamespace,
                    leftSourceTable,
                    rightSourceNamespace,
                    rightSourceTable,
                    joinType,
                    options))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must have the same primary key");
  }

  @Test
  public void
      createVirtualTable_SourceTablesWithDifferentPrimaryKeyTypes_ShouldThrowIllegalArgumentException()
          throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "vtable";
    String leftSourceNamespace = "ns";
    String leftSourceTable = "left_table";
    String rightSourceNamespace = "ns";
    String rightSourceTable = "right_table";
    VirtualTableJoinType joinType = VirtualTableJoinType.INNER;
    Map<String, String> options = Collections.emptyMap();

    StorageInfo storageInfo = mock(StorageInfo.class);
    when(storageInfo.getAtomicityUnit()).thenReturn(StorageInfo.AtomicityUnit.NAMESPACE);
    when(admin.getStorageInfo(leftSourceNamespace)).thenReturn(storageInfo);
    when(admin.namespaceExists(namespace)).thenReturn(true);
    when(admin.tableExists(namespace, table)).thenReturn(false);

    TableMetadata leftTableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addColumn("pk1", DataType.INT)
            .addColumn("col1", DataType.INT)
            .build();
    when(admin.getTableMetadata(leftSourceNamespace, leftSourceTable))
        .thenReturn(leftTableMetadata);

    TableMetadata rightTableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addColumn("pk1", DataType.TEXT)
            .addColumn("col2", DataType.TEXT)
            .build();
    when(admin.getTableMetadata(rightSourceNamespace, rightSourceTable))
        .thenReturn(rightTableMetadata);

    // Act Assert
    assertThatThrownBy(
            () ->
                commonDistributedStorageAdmin.createVirtualTable(
                    namespace,
                    table,
                    leftSourceNamespace,
                    leftSourceTable,
                    rightSourceNamespace,
                    rightSourceTable,
                    joinType,
                    options))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must have the same data types for primary key column");
  }

  @Test
  public void
      createVirtualTable_SourceTablesWithConflictingColumnNames_ShouldThrowIllegalArgumentException()
          throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "vtable";
    String leftSourceNamespace = "ns";
    String leftSourceTable = "left_table";
    String rightSourceNamespace = "ns";
    String rightSourceTable = "right_table";
    VirtualTableJoinType joinType = VirtualTableJoinType.INNER;
    Map<String, String> options = Collections.emptyMap();

    StorageInfo storageInfo = mock(StorageInfo.class);
    when(storageInfo.getAtomicityUnit()).thenReturn(StorageInfo.AtomicityUnit.NAMESPACE);
    when(admin.getStorageInfo(leftSourceNamespace)).thenReturn(storageInfo);
    when(admin.namespaceExists(namespace)).thenReturn(true);

    // Mock getNamespaceTableNames for tableExists() to work
    // All tables are in "ns" namespace
    when(admin.getNamespaceTableNames("ns"))
        .thenReturn(new HashSet<>(Arrays.asList(leftSourceTable, rightSourceTable)));

    TableMetadata leftTableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addColumn("pk1", DataType.INT)
            .addColumn("col1", DataType.INT)
            .build();
    when(admin.getTableMetadata(leftSourceNamespace, leftSourceTable))
        .thenReturn(leftTableMetadata);

    TableMetadata rightTableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addColumn("pk1", DataType.INT)
            .addColumn("col1", DataType.TEXT)
            .build();
    when(admin.getTableMetadata(rightSourceNamespace, rightSourceTable))
        .thenReturn(rightTableMetadata);

    // Act Assert
    assertThatThrownBy(
            () ->
                commonDistributedStorageAdmin.createVirtualTable(
                    namespace,
                    table,
                    leftSourceNamespace,
                    leftSourceTable,
                    rightSourceNamespace,
                    rightSourceTable,
                    joinType,
                    options))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("have conflicting non-key column names");
  }

  @Test
  public void createVirtualTable_VirtualTableUsedAsSource_ShouldThrowIllegalArgumentException()
      throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "vtable";
    String leftSourceNamespace = "ns";
    String leftSourceTable = "left_vtable";
    String rightSourceNamespace = "ns";
    String rightSourceTable = "right_table";
    VirtualTableJoinType joinType = VirtualTableJoinType.INNER;
    Map<String, String> options = Collections.emptyMap();

    StorageInfo storageInfo = mock(StorageInfo.class);
    when(storageInfo.getAtomicityUnit()).thenReturn(StorageInfo.AtomicityUnit.NAMESPACE);
    when(admin.getStorageInfo(leftSourceNamespace)).thenReturn(storageInfo);
    when(admin.namespaceExists(namespace)).thenReturn(true);

    // Mock getNamespaceTableNames for tableExists() to work
    // All tables are in "ns" namespace
    when(admin.getNamespaceTableNames("ns"))
        .thenReturn(new HashSet<>(Arrays.asList(leftSourceTable, rightSourceTable)));

    TableMetadata leftTableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addColumn("pk1", DataType.INT)
            .addColumn("col1", DataType.INT)
            .build();
    when(admin.getTableMetadata(leftSourceNamespace, leftSourceTable))
        .thenReturn(leftTableMetadata);

    TableMetadata rightTableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addColumn("pk1", DataType.INT)
            .addColumn("col2", DataType.TEXT)
            .build();
    when(admin.getTableMetadata(rightSourceNamespace, rightSourceTable))
        .thenReturn(rightTableMetadata);

    VirtualTableInfo virtualTableInfo = mock(VirtualTableInfo.class);
    when(admin.getVirtualTableInfo(leftSourceNamespace, leftSourceTable))
        .thenReturn(Optional.of(virtualTableInfo));

    // Act Assert
    assertThatThrownBy(
            () ->
                commonDistributedStorageAdmin.createVirtualTable(
                    namespace,
                    table,
                    leftSourceNamespace,
                    leftSourceTable,
                    rightSourceNamespace,
                    rightSourceTable,
                    joinType,
                    options))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Virtual tables cannot be used as source tables");
  }

  @Test
  public void getVirtualTableInfo_ProperArgumentsGiven_ShouldCallAdminProperly()
      throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "vtable";

    // Mock getNamespaceTableNames for tableExists() to work
    when(admin.getNamespaceTableNames(namespace))
        .thenReturn(new HashSet<>(Collections.singletonList(table)));

    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(admin.getTableMetadata(namespace, table)).thenReturn(tableMetadata);

    VirtualTableInfo virtualTableInfo = mock(VirtualTableInfo.class);
    when(admin.getVirtualTableInfo(namespace, table)).thenReturn(Optional.of(virtualTableInfo));

    // Act
    Optional<VirtualTableInfo> result =
        commonDistributedStorageAdmin.getVirtualTableInfo(namespace, table);

    // Assert
    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(virtualTableInfo);
    verify(admin).getVirtualTableInfo(namespace, table);
  }

  @Test
  public void getVirtualTableInfo_TableDoesNotExist_ShouldThrowIllegalArgumentException()
      throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "vtable";

    // Mock getNamespaceTableNames for tableExists() to return false
    when(admin.getNamespaceTableNames(namespace)).thenReturn(Collections.emptySet());

    when(admin.getTableMetadata(namespace, table)).thenReturn(null);

    // Act Assert
    assertThatThrownBy(() -> commonDistributedStorageAdmin.getVirtualTableInfo(namespace, table))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not exist");
  }
}
