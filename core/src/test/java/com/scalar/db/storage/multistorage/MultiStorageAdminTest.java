package com.scalar.db.storage.multistorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class MultiStorageAdminTest {

  protected static final String NAMESPACE1 = "test_ns1";
  protected static final String NAMESPACE2 = "test_ns2";
  protected static final String NAMESPACE3 = "test_ns3";
  protected static final String TABLE1 = "test_table1";
  protected static final String TABLE2 = "test_table2";
  protected static final String TABLE3 = "test_table3";

  @Mock private DistributedStorageAdmin admin1;
  @Mock private DistributedStorageAdmin admin2;
  @Mock private DistributedStorageAdmin admin3;
  @Mock private TableMetadata tableMetadata;

  private MultiStorageAdmin multiStorageAdmin;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    Map<String, DistributedStorageAdmin> tableAdminMap = new HashMap<>();
    tableAdminMap.put(NAMESPACE1 + "." + TABLE1, admin1);
    tableAdminMap.put(NAMESPACE1 + "." + TABLE2, admin2);
    Map<String, DistributedStorageAdmin> namespaceAdminMap = new HashMap<>();
    namespaceAdminMap.put(NAMESPACE2, admin2);
    DistributedStorageAdmin defaultAdmin = admin3;
    multiStorageAdmin = new MultiStorageAdmin(tableAdminMap, namespaceAdminMap, defaultAdmin);
  }

  @Test
  public void createNamespace_ForNamespace2_ShouldCallAdmin2() throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE2;

    // Act
    multiStorageAdmin.createNamespace(namespace);

    // Assert
    verify(admin2).createNamespace(namespace);
  }

  @Test
  public void createNamespace_ForNamespace3_ShouldCallDefaultAdmin() throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE3;

    // Act
    multiStorageAdmin.createNamespace(namespace);

    // Assert
    verify(admin3).createNamespace(namespace);
  }

  @Test
  public void createTable_ForTable1InNamespace1_ShouldReturnMetadataFromAdmin1()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE1;

    // Act
    multiStorageAdmin.createTable(namespace, table, tableMetadata);

    // Assert
    verify(admin1).createTable(namespace, table, tableMetadata);
  }

  @Test
  public void createTable_ForTable2InNamespace1_ShouldReturnMetadataFromAdmin2()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE2;

    // Act
    multiStorageAdmin.createTable(namespace, table, tableMetadata);

    // Assert
    verify(admin2).createTable(namespace, table, tableMetadata);
  }

  @Test
  public void createTable_ForTable3InNamespace1_ShouldReturnMetadataFromDefaultAdmin()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE3;

    // Act
    multiStorageAdmin.createTable(namespace, table, tableMetadata);

    // Assert
    verify(admin3).createTable(namespace, table, tableMetadata);
  }

  @Test
  public void createTable_ForTable1InNamespace2_ShouldReturnMetadataFromAdmin2()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE2;
    String table = TABLE1;

    // Act
    multiStorageAdmin.createTable(namespace, table, tableMetadata);

    // Assert
    verify(admin2).createTable(namespace, table, tableMetadata);
  }

  @Test
  public void dropTable_ForTable1InNamespace1_ShouldReturnMetadataFromAdmin1()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE1;

    // Act
    multiStorageAdmin.dropTable(namespace, table);

    // Assert
    verify(admin1).dropTable(namespace, table);
  }

  @Test
  public void dropTable_ForTable2InNamespace1_ShouldReturnMetadataFromAdmin2()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE2;

    // Act
    multiStorageAdmin.dropTable(namespace, table);

    // Assert
    verify(admin2).dropTable(namespace, table);
  }

  @Test
  public void dropTable_ForTable3InNamespace1_ShouldReturnMetadataFromDefaultAdmin()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE3;

    // Act
    multiStorageAdmin.dropTable(namespace, table);

    // Assert
    verify(admin3).dropTable(namespace, table);
  }

  @Test
  public void dropTable_ForTable1InNamespace2_ShouldReturnMetadataFromAdmin2()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE2;
    String table = TABLE1;

    // Act
    multiStorageAdmin.dropTable(namespace, table);

    // Assert
    verify(admin2).dropTable(namespace, table);
  }

  @Test
  public void dropNamespace_ForNamespace2_ShouldCallAdmin2() throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE2;

    // Act
    multiStorageAdmin.dropNamespace(namespace);

    // Assert
    verify(admin2).dropNamespace(namespace);
  }

  @Test
  public void dropNamespace_ForNamespace3_ShouldCallDefaultAdmin() throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE3;

    // Act
    multiStorageAdmin.dropNamespace(namespace);

    // Assert
    verify(admin3).dropNamespace(namespace);
  }

  @Test
  public void truncateTable_ForTable1InNamespace1_ShouldReturnMetadataFromAdmin1()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE1;

    // Act
    multiStorageAdmin.truncateTable(namespace, table);

    // Assert
    verify(admin1).truncateTable(namespace, table);
  }

  @Test
  public void truncateTable_ForTable2InNamespace1_ShouldReturnMetadataFromAdmin2()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE2;

    // Act
    multiStorageAdmin.truncateTable(namespace, table);

    // Assert
    verify(admin2).truncateTable(namespace, table);
  }

  @Test
  public void truncateTable_ForTable3InNamespace1_ShouldReturnMetadataFromDefaultAdmin()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE3;

    // Act
    multiStorageAdmin.truncateTable(namespace, table);

    // Assert
    verify(admin3).truncateTable(namespace, table);
  }

  @Test
  public void truncateTable_ForTable1InNamespace2_ShouldReturnMetadataFromAdmin2()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE2;
    String table = TABLE1;

    // Act
    multiStorageAdmin.truncateTable(namespace, table);

    // Assert
    verify(admin2).truncateTable(namespace, table);
  }

  @Test
  public void getTableMetadata_ForTable1InNamespace1_ShouldReturnMetadataFromAdmin1()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE1;

    // Act
    multiStorageAdmin.getTableMetadata(namespace, table);

    // Assert
    verify(admin1).getTableMetadata(namespace, table);
  }

  @Test
  public void getTableMetadata_ForTable2InNamespace1_ShouldReturnMetadataFromAdmin2()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE2;

    // Act
    multiStorageAdmin.getTableMetadata(namespace, table);

    // Assert
    verify(admin2).getTableMetadata(namespace, table);
  }

  @Test
  public void getTableMetadata_ForTable3InNamespace1_ShouldReturnMetadataFromDefaultAdmin()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE3;

    // Act
    multiStorageAdmin.getTableMetadata(namespace, table);

    // Assert
    verify(admin3).getTableMetadata(namespace, table);
  }

  @Test
  public void getTableMetadata_ForTable1InNamespace2_ShouldReturnMetadataFromAdmin2()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE2;
    String table = TABLE1;

    // Act
    multiStorageAdmin.getTableMetadata(namespace, table);

    // Assert
    verify(admin2).getTableMetadata(namespace, table);
  }

  @Test
  public void getNamespaceTableNames_ForNamespace2_ShouldCallAdmin2() throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE2;

    // Act
    multiStorageAdmin.getNamespaceTableNames(namespace);

    // Assert
    verify(admin2).getNamespaceTableNames(namespace);
  }

  @Test
  public void getNamespaceTableNames_ForNamespace3_ShouldCallDefaultAdmin()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE3;

    // Act
    multiStorageAdmin.getNamespaceTableNames(namespace);

    // Assert
    verify(admin3).getNamespaceTableNames(namespace);
  }

  @Test
  public void namespaceExists_ForNamespace2_ShouldCallAdmin2() throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE2;

    // Act
    multiStorageAdmin.namespaceExists(namespace);

    // Assert
    verify(admin2).namespaceExists(namespace);
  }

  @Test
  public void namespaceExists_ForNamespace3_ShouldCallDefaultAdmin() throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE3;

    // Act
    multiStorageAdmin.namespaceExists(namespace);

    // Assert
    verify(admin3).namespaceExists(namespace);
  }

  @Test
  public void repairTable_ForTable1InNamespace1_ShouldRepairTableInAdmin1()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE1;
    Map<String, String> options = ImmutableMap.of("foo", "bar");

    // Act
    multiStorageAdmin.repairTable(namespace, table, tableMetadata, options);

    // Assert
    verify(admin1).repairTable(namespace, table, tableMetadata, options);
  }

  @Test
  public void repairTable_ForTable2InNamespace1_ShouldRepairTableInAdmin2()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE2;
    Map<String, String> options = ImmutableMap.of("foo", "bar");

    // Act
    multiStorageAdmin.repairTable(namespace, table, tableMetadata, options);

    // Assert
    verify(admin2).repairTable(namespace, table, tableMetadata, options);
  }

  @Test
  public void repairTable_ForTable3InNamespace1_ShouldRepairTableInDefaultAdmin()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE3;
    Map<String, String> options = ImmutableMap.of("foo", "bar");

    // Act
    multiStorageAdmin.repairTable(namespace, table, tableMetadata, options);

    // Assert
    verify(admin3).repairTable(namespace, table, tableMetadata, options);
  }

  @Test
  public void repairTable_ForTable1InNamespace2_ShouldRepairTableInAdmin2()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE2;
    String table = TABLE1;
    Map<String, String> options = ImmutableMap.of("foo", "bar");

    // Act
    multiStorageAdmin.repairTable(namespace, table, tableMetadata, options);

    // Assert
    verify(admin2).repairTable(namespace, table, tableMetadata, options);
  }

  @Test
  public void addNewColumnToTable_ForTable1InNamespace1_ShouldCallAddNewColumnOfAdmin1()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE1;
    String column = "c1";
    DataType dataType = DataType.TEXT;

    // Act
    multiStorageAdmin.addNewColumnToTable(namespace, table, column, dataType);

    // Assert
    verify(admin1).addNewColumnToTable(namespace, table, column, dataType);
  }

  @Test
  public void addNewColumnToTable_ForTable2InNamespace1_ShouldShouldCallAddNewColumnOfAdmin2()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE2;
    String column = "c1";
    DataType dataType = DataType.TEXT;

    // Act
    multiStorageAdmin.addNewColumnToTable(namespace, table, column, dataType);

    // Assert
    verify(admin2).addNewColumnToTable(namespace, table, column, dataType);
  }

  @Test
  public void addNewColumnToTable_ForTable3InNamespace1_ShouldCallAddNewColumnOfDefaultAdmin()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE3;
    String column = "c1";
    DataType dataType = DataType.TEXT;

    // Act
    multiStorageAdmin.addNewColumnToTable(namespace, table, column, dataType);

    // Assert
    verify(admin3).addNewColumnToTable(namespace, table, column, dataType);
  }

  @Test
  public void addNewColumnToTable_ForTable1InNamespace2_ShouldCallAddNewColumnOfAdmin2()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE2;
    String table = TABLE1;
    String column = "c1";
    DataType dataType = DataType.TEXT;

    // Act
    multiStorageAdmin.addNewColumnToTable(namespace, table, column, dataType);

    // Assert
    verify(admin2).addNewColumnToTable(namespace, table, column, dataType);
  }

  @Test
  public void
      getNamespaceNames_WithExistingNamespacesNotInMapping_ShouldReturnExistingNamespacesInMappingAndFromDefaultAdmin()
          throws ExecutionException {
    // Arrange
    Map<String, DistributedStorageAdmin> namespaceAdminMap = new HashMap<>();
    namespaceAdminMap.put("ns1", admin1);
    namespaceAdminMap.put("ns2", admin2);
    namespaceAdminMap.put("ns3", admin2);
    DistributedStorageAdmin defaultAdmin = admin3;
    multiStorageAdmin =
        new MultiStorageAdmin(Collections.emptyMap(), namespaceAdminMap, defaultAdmin);

    when(admin1.getNamespaceNames()).thenReturn(ImmutableSet.of("ns1", "ns2"));
    when(admin2.getNamespaceNames()).thenReturn(ImmutableSet.of("ns3"));
    when(admin3.getNamespaceNames()).thenReturn(ImmutableSet.of("ns4", "ns5"));

    // Act
    Set<String> actualNamespaces = multiStorageAdmin.getNamespaceNames();

    // Assert
    verify(admin1).getNamespaceNames();
    verify(admin2).getNamespaceNames();
    verify(admin3).getNamespaceNames();
    assertThat(actualNamespaces).containsOnly("ns1", "ns3", "ns4", "ns5");
  }

  @Test
  public void getNamespaceNames_WithNamespaceInMappingButNotExisting_ShouldReturnEmptySet()
      throws ExecutionException {
    // Arrange
    Map<String, DistributedStorageAdmin> namespaceAdminMap = new HashMap<>();
    namespaceAdminMap.put("ns1", admin1);
    namespaceAdminMap.put("ns2", admin2);
    DistributedStorageAdmin defaultAdmin = admin3;
    multiStorageAdmin =
        new MultiStorageAdmin(Collections.emptyMap(), namespaceAdminMap, defaultAdmin);

    when(admin1.getNamespaceNames()).thenReturn(Collections.emptySet());
    when(admin2.getNamespaceNames()).thenReturn(Collections.emptySet());
    when(admin3.getNamespaceNames()).thenReturn(Collections.emptySet());

    // Act
    Set<String> actualNamespaces = multiStorageAdmin.getNamespaceNames();

    // Assert
    verify(admin1).getNamespaceNames();
    verify(admin2).getNamespaceNames();
    verify(admin3).getNamespaceNames();
    assertThat(actualNamespaces).isEmpty();
  }

  @Test
  public void getNamespaceNames_WithExistingNamespaceButNotInMapping_ShouldReturnEmptySet()
      throws ExecutionException {
    // Arrange
    Map<String, DistributedStorageAdmin> namespaceAdminMap = new HashMap<>();
    namespaceAdminMap.put("ns1", admin1);
    namespaceAdminMap.put("ns2", admin2);
    DistributedStorageAdmin defaultAdmin = admin3;
    multiStorageAdmin =
        new MultiStorageAdmin(Collections.emptyMap(), namespaceAdminMap, defaultAdmin);

    when(admin1.getNamespaceNames()).thenReturn(ImmutableSet.of("ns2"));
    when(admin2.getNamespaceNames()).thenReturn(Collections.emptySet());
    when(admin3.getNamespaceNames()).thenReturn(Collections.emptySet());

    // Act
    Set<String> actualNamespaces = multiStorageAdmin.getNamespaceNames();

    // Assert
    verify(admin1).getNamespaceNames();
    verify(admin2).getNamespaceNames();
    verify(admin3).getNamespaceNames();
    assertThat(actualNamespaces).isEmpty();
  }

  @Test
  public void repairNamespace_ForNamespace2_ShouldCallAdmin2() throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE2;
    @SuppressWarnings("unchecked")
    Map<String, String> options = mock(Map.class);

    // Act
    multiStorageAdmin.repairNamespace(namespace, options);

    // Assert
    verify(admin2).repairNamespace(namespace, options);
  }

  @Test
  public void repairNamespace_ForNamespace3_ShouldCallDefaultAdmin() throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE3;
    @SuppressWarnings("unchecked")
    Map<String, String> options = mock(Map.class);

    // Act
    multiStorageAdmin.repairNamespace(namespace, options);

    // Assert
    verify(admin3).repairNamespace(namespace, options);
  }

  @Test
  public void upgrade_ShouldCallNamespaceAndDefaultAdmins() throws ExecutionException {
    // Arrange
    Map<String, String> options = ImmutableMap.of("foo", "bar");
    Map<String, DistributedStorageAdmin> namespaceAdminMap =
        ImmutableMap.of("ns1", admin1, "ns2", admin2);
    DistributedStorageAdmin defaultAdmin = admin2;
    multiStorageAdmin =
        new MultiStorageAdmin(Collections.emptyMap(), namespaceAdminMap, defaultAdmin);

    // Act
    multiStorageAdmin.upgrade(options);

    // Assert
    verify(admin1).upgrade(options);
    verify(admin2).upgrade(options);
  }

  @Test
  public void importTable_ForTable1InNamespace1_ShouldCallAdmin1() throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE1;
    Map<String, String> options = ImmutableMap.of("a", "b");
    Map<String, DataType> overrideColumnsType = ImmutableMap.of("c", DataType.TIMESTAMPTZ);

    // Act
    multiStorageAdmin.importTable(namespace, table, options, overrideColumnsType);

    // Assert
    verify(admin1).importTable(namespace, table, options, overrideColumnsType);
  }

  @Test
  public void getImportTableMetadata_ForTable1InNamespace1_ShouldCallAdmin1()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE1;
    Map<String, DataType> overrideColumnsType = ImmutableMap.of("c", DataType.TIMESTAMPTZ);

    // Act
    multiStorageAdmin.getImportTableMetadata(namespace, table, overrideColumnsType);

    // Assert
    verify(admin1).getImportTableMetadata(namespace, table, overrideColumnsType);
  }
}
