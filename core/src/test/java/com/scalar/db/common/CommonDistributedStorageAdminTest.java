package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.Collections;
import java.util.Map;
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
}
