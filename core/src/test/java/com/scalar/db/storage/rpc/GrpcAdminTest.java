package com.scalar.db.storage.rpc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.rpc.DistributedStorageAdminGrpc;
import java.util.Collections;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class GrpcAdminTest {

  @Mock private GrpcConfig config;
  @Mock private DistributedStorageAdminGrpc.DistributedStorageAdminBlockingStub stub;
  @Mock private GrpcTableMetadataManager metadataManager;

  private GrpcAdmin admin;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    // Arrange
    admin = new GrpcAdmin(config, stub, metadataManager);
    when(config.getDeadlineDurationMillis()).thenReturn(60000L);
    when(stub.withDeadlineAfter(anyLong(), any())).thenReturn(stub);
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
    Map<String, String> options = Collections.emptyMap();

    // Act
    admin.createTable(namespace, table, metadata, false, options);

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

    // Act
    admin.getTableMetadata(namespace, table);

    // Assert
    verify(metadataManager).getTableMetadata(namespace, table);
  }
}
