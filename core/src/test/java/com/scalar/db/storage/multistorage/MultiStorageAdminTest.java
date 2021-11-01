package com.scalar.db.storage.multistorage;

import static org.mockito.Mockito.verify;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
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

  @Before
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
}
