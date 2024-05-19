package com.scalar.db.dataloader.core.tablemetadata;

import static com.scalar.db.dataloader.core.constant.ErrorMessages.ERROR_MISSING_NAMESPACE_OR_TABLE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class TableMetadataServiceTest {

  @Mock private DistributedStorageAdmin storageAdmin;

  private TableMetadataService tableMetadataService;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    tableMetadataService = new TableMetadataService(storageAdmin);
  }

  @Test
  void getTableMetadata_WithValidNamespaceAndTable_ShouldReturnTableMetadata()
      throws ExecutionException, TableMetadataException {
    // Arrange
    String namespace = "test_namespace";
    String tableName = "test_table";
    TableMetadata expectedMetadata = mock(TableMetadata.class);
    when(storageAdmin.getTableMetadata(namespace, tableName)).thenReturn(expectedMetadata);

    // Act
    TableMetadata actualMetadata = tableMetadataService.getTableMetadata(namespace, tableName);

    // Assert
    assertSame(expectedMetadata, actualMetadata);
    verify(storageAdmin).getTableMetadata(namespace, tableName);
  }

  @Test
  void getTableMetadata_WithMissingNamespaceOrTable_ShouldThrowTableMetadataException()
      throws ExecutionException {
    // Arrange
    String namespace = "test_namespace";
    String tableName = "test_table";
    when(storageAdmin.getTableMetadata(namespace, tableName)).thenReturn(null);

    // Act & Assert
    TableMetadataException exception =
        assertThrows(
            TableMetadataException.class,
            () -> tableMetadataService.getTableMetadata(namespace, tableName));
    assertEquals(
        String.format(ERROR_MISSING_NAMESPACE_OR_TABLE, namespace, tableName),
        exception.getMessage());
    verify(storageAdmin).getTableMetadata(namespace, tableName);
  }

  @Test
  void getTableMetadata_WithExecutionException_ShouldThrowTableMetadataException()
      throws ExecutionException {
    // Arrange
    String namespace = "test_namespace";
    String tableName = "test_table";
    Throwable cause = new RuntimeException("Test exception");
    when(storageAdmin.getTableMetadata(namespace, tableName))
        .thenThrow(new ExecutionException("error"));

    // Act & Assert
    TableMetadataException exception =
        assertThrows(
            TableMetadataException.class,
            () -> tableMetadataService.getTableMetadata(namespace, tableName));
    assertEquals(
        String.format(ERROR_MISSING_NAMESPACE_OR_TABLE, namespace, tableName),
        exception.getMessage());
    verify(storageAdmin).getTableMetadata(namespace, tableName);
  }
}
