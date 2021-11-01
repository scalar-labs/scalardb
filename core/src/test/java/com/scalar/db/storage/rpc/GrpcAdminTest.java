package com.scalar.db.storage.rpc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.LazyStringArrayList;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.rpc.DistributedStorageAdminGrpc;
import com.scalar.db.rpc.GetNamespaceTableNamesResponse;
import com.scalar.db.rpc.GetTableMetadataResponse;
import com.scalar.db.rpc.NamespaceExistsResponse;
import java.util.Collections;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class GrpcAdminTest {

  @Mock private GrpcConfig config;
  @Mock private DistributedStorageAdminGrpc.DistributedStorageAdminBlockingStub stub;

  private GrpcAdmin admin;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    admin = new GrpcAdmin(stub, config);
    when(config.getDeadlineDurationMillis()).thenReturn(60000L);
    when(stub.withDeadlineAfter(anyLong(), any())).thenReturn(stub);
  }

  @Test
  public void createNamespace_IsCalledWithProperArguments_StubShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    String namespace = "namespace";
    boolean ifNotExists = false;
    Map<String, String> options = Collections.emptyMap();

    // Act
    admin.createNamespace(namespace, ifNotExists, options);

    // Assert
    verify(stub).createNamespace(any());
  }

  @Test
  public void dropNamespace_IsCalledWithProperArguments_StubShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    String namespace = "namespace";

    // Act
    admin.dropNamespace(namespace);

    // Assert
    verify(stub).dropNamespace(any());
  }

  @Test
  public void createTable_IsCalledWithProperArguments_StubShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    String namespace = "namespace";
    String table = "table";
    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addColumn("col1", DataType.INT)
            .addColumn("col2", DataType.INT)
            .addPartitionKey("col1")
            .build();
    boolean ifNotExists = false;
    Map<String, String> options = Collections.emptyMap();

    // Act
    admin.createTable(namespace, table, metadata, ifNotExists, options);

    // Assert
    verify(stub).createTable(any());
  }

  @Test
  public void dropTable_IsCalledWithProperArguments_StubShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    String namespace = "namespace";
    String table = "table";

    // Act
    admin.dropTable(namespace, table);

    // Assert
    verify(stub).dropTable(any());
  }

  @Test
  public void truncateTable_IsCalledWithProperArguments_StubShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    String namespace = "namespace";
    String table = "table";

    // Act
    admin.truncateTable(namespace, table);

    // Assert
    verify(stub).truncateTable(any());
  }

  @Test
  public void getTableMetadata_IsCalledWithProperArguments_MetadataManagerShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    String namespace = "namespace";
    String table = "table";
    GetTableMetadataResponse response = mock(GetTableMetadataResponse.class);
    when(stub.getTableMetadata(any())).thenReturn(response);

    // Act
    admin.getTableMetadata(namespace, table);

    // Assert
    verify(stub).getTableMetadata(any());
  }

  @Test
  public void getNamespaceTableNames_IsCalledWithProperArguments_StubShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    String namespace = "namespace";
    GetNamespaceTableNamesResponse response = mock(GetNamespaceTableNamesResponse.class);
    when(stub.getNamespaceTableNames(any())).thenReturn(response);
    when(response.getTableNameList()).thenReturn(new LazyStringArrayList());

    // Act
    admin.getNamespaceTableNames(namespace);

    // Assert
    verify(stub).getNamespaceTableNames(any());
  }

  @Test
  public void namespaceExists_IsCalledWithProperArguments_StubShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    String namespace = "namespace";
    NamespaceExistsResponse response = mock(NamespaceExistsResponse.class);
    when(stub.namespaceExists(any())).thenReturn(response);
    when(response.getExists()).thenReturn(false);

    // Act
    admin.namespaceExists(namespace);

    // Assert
    verify(stub).namespaceExists(any());
  }
}
