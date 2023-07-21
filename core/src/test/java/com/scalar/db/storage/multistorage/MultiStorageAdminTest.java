package com.scalar.db.storage.multistorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedStorageAtomicityLevel;
import com.scalar.db.api.DistributedStorageMetadata;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class MultiStorageAdminTest {

  private static final String NAMESPACE1 = "test_ns1";
  private static final String NAMESPACE2 = "test_ns2";
  private static final String NAMESPACE3 = "test_ns3";
  private static final String TABLE1 = "test_table1";
  private static final String TABLE2 = "test_table2";
  private static final String TABLE3 = "test_table3";
  private static final String STORAGE_NAME2 = "straoge_name2";
  private static final String STORAGE_NAME3 = "straoge_name3";

  @Mock private DistributedStorageAdmin admin1;
  @Mock private DistributedStorageAdmin admin2;
  @Mock private DistributedStorageAdmin admin3;
  @Mock private TableMetadata tableMetadata;

  private MultiStorageAdmin multiStorageAdmin;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    when(admin2.getDistributedStorageMetadata(NAMESPACE2))
        .thenReturn(
            DistributedStorageMetadata.newBuilder()
                .type("cassandra")
                .name("cassandra")
                .atomicityLevel(DistributedStorageAtomicityLevel.PARTITION)
                .build());

    when(admin3.getDistributedStorageMetadata(NAMESPACE3))
        .thenReturn(
            DistributedStorageMetadata.newBuilder()
                .type("jdbc")
                .name("jdbc")
                .linearizableScanAllSupported()
                .atomicityLevel(DistributedStorageAtomicityLevel.STORAGE)
                .build());

    Map<String, DistributedStorageAdmin> tableAdminMap = new HashMap<>();
    tableAdminMap.put(NAMESPACE1 + "." + TABLE1, admin1);
    tableAdminMap.put(NAMESPACE1 + "." + TABLE2, admin2);
    Map<String, DistributedStorageAdmin> namespaceAdminMap = new HashMap<>();
    namespaceAdminMap.put(NAMESPACE2, admin2);
    DistributedStorageAdmin defaultAdmin = admin3;
    Map<String, String> namespaceStorageNameMap = new HashMap<>();
    namespaceStorageNameMap.put(NAMESPACE2, STORAGE_NAME2);
    multiStorageAdmin =
        new MultiStorageAdmin(
            tableAdminMap, namespaceAdminMap, defaultAdmin, namespaceStorageNameMap, STORAGE_NAME3);
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
  public void getDistributedStorageMetadata_ShouldReturnAppropriateMetadata()
      throws ExecutionException {
    // Arrange

    // Act
    DistributedStorageMetadata metadata1 =
        multiStorageAdmin.getDistributedStorageMetadata(NAMESPACE2);
    DistributedStorageMetadata metadata2 =
        multiStorageAdmin.getDistributedStorageMetadata(NAMESPACE3);

    // Assert
    assertThat(metadata1).isNotNull();
    assertThat(metadata1.getType()).isEqualTo("cassandra");
    assertThat(metadata1.getName()).isEqualTo(STORAGE_NAME2);
    assertThat(metadata1.isLinearizableScanAllSupported()).isFalse();
    assertThat(metadata1.getAtomicityLevel()).isEqualTo(DistributedStorageAtomicityLevel.PARTITION);

    assertThat(metadata2).isNotNull();
    assertThat(metadata2.getType()).isEqualTo("jdbc");
    assertThat(metadata2.getName()).isEqualTo(STORAGE_NAME3);
    assertThat(metadata2.isLinearizableScanAllSupported()).isTrue();
    assertThat(metadata2.getAtomicityLevel()).isEqualTo(DistributedStorageAtomicityLevel.STORAGE);
  }
}
