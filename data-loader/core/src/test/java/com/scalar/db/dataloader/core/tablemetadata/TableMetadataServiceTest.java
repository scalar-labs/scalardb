package com.scalar.db.dataloader.core.tablemetadata;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.service.StorageFactory;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class TableMetadataServiceTest {

  @Mock private StorageFactory storageFactory;
  @Mock private DistributedStorageAdmin storageAdmin;

  private TableMetadataService tableMetadataService;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    tableMetadataService = new TableMetadataService(storageFactory);
    when(storageFactory.getStorageAdmin()).thenReturn(storageAdmin);
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
        CoreError.DATA_LOADER_MISSING_NAMESPACE_OR_TABLE.buildMessage(namespace, tableName),
        exception.getMessage());
    verify(storageAdmin).getTableMetadata(namespace, tableName);
  }

  @Test
  void getTableMetadata_WithExecutionException_ShouldThrowTableMetadataException()
      throws ExecutionException {
    // Arrange
    String namespace = "test_namespace";
    String tableName = "test_table";
    when(storageAdmin.getTableMetadata(namespace, tableName))
        .thenThrow(new ExecutionException("error"));

    // Act & Assert
    TableMetadataException exception =
        assertThrows(
            TableMetadataException.class,
            () -> tableMetadataService.getTableMetadata(namespace, tableName));
    assertEquals(
        CoreError.DATA_LOADER_MISSING_NAMESPACE_OR_TABLE.buildMessage(namespace, tableName),
        exception.getMessage());
    verify(storageAdmin).getTableMetadata(namespace, tableName);
  }

  @Test
  void getTableMetadata_WithValidRequests_ShouldReturnTableMetadataMap()
      throws ExecutionException, TableMetadataException {
    // Arrange
    String namespace1 = "test_namespace1";
    String tableName1 = "test_table1";
    String namespace2 = "test_namespace2";
    String tableName2 = "test_table2";
    TableMetadataRequest request1 = new TableMetadataRequest(namespace1, tableName1);
    TableMetadataRequest request2 = new TableMetadataRequest(namespace2, tableName2);
    Collection<TableMetadataRequest> requests = Arrays.asList(request1, request2);
    TableMetadata expectedMetadata1 = mock(TableMetadata.class);
    TableMetadata expectedMetadata2 = mock(TableMetadata.class);
    when(storageAdmin.getTableMetadata(namespace1, tableName1)).thenReturn(expectedMetadata1);
    when(storageAdmin.getTableMetadata(namespace2, tableName2)).thenReturn(expectedMetadata2);

    // Act
    Map<String, TableMetadata> actualMetadataMap = tableMetadataService.getTableMetadata(requests);

    // Assert
    assertEquals(2, actualMetadataMap.size());
    assertSame(expectedMetadata1, actualMetadataMap.get(namespace1 + "." + tableName1));
    assertSame(expectedMetadata2, actualMetadataMap.get(namespace2 + "." + tableName2));
    verify(storageAdmin).getTableMetadata(namespace1, tableName1);
    verify(storageAdmin).getTableMetadata(namespace2, tableName2);
  }

  @Test
  void getTableMetadata_WithMissingNamespaceOrTableInRequests_ShouldThrowTableMetadataException()
      throws ExecutionException {
    // Arrange
    String namespace = "test_namespace";
    String tableName = "test_table";
    TableMetadataRequest request = new TableMetadataRequest(namespace, tableName);
    Collection<TableMetadataRequest> requests = Collections.singletonList(request);
    when(storageAdmin.getTableMetadata(namespace, tableName)).thenReturn(null);

    // Act & Assert
    TableMetadataException exception =
        assertThrows(
            TableMetadataException.class, () -> tableMetadataService.getTableMetadata(requests));
    assertEquals(
        CoreError.DATA_LOADER_MISSING_NAMESPACE_OR_TABLE.buildMessage(namespace, tableName),
        exception.getMessage());
    verify(storageAdmin).getTableMetadata(namespace, tableName);
  }

  @Test
  void getTableMetadata_WithExecutionExceptionInRequests_ShouldThrowTableMetadataException()
      throws ExecutionException {
    // Arrange
    String namespace = "test_namespace";
    String tableName = "test_table";
    TableMetadataRequest request = new TableMetadataRequest(namespace, tableName);
    Collection<TableMetadataRequest> requests = Collections.singletonList(request);
    when(storageAdmin.getTableMetadata(namespace, tableName))
        .thenThrow(new ExecutionException("error"));

    // Act & Assert
    TableMetadataException exception =
        assertThrows(
            TableMetadataException.class, () -> tableMetadataService.getTableMetadata(requests));
    assertEquals(
        CoreError.DATA_LOADER_MISSING_NAMESPACE_OR_TABLE.buildMessage(namespace, tableName),
        exception.getMessage());
    verify(storageAdmin).getTableMetadata(namespace, tableName);
  }
}
