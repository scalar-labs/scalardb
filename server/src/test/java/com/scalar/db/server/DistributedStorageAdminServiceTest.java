package com.scalar.db.server;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.google.protobuf.Empty;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.rpc.CreateTableRequest;
import com.scalar.db.rpc.DropTableRequest;
import com.scalar.db.rpc.GetTableMetadataRequest;
import com.scalar.db.rpc.GetTableMetadataResponse;
import com.scalar.db.rpc.TruncateTableRequest;
import com.scalar.db.util.ProtoUtil;
import io.grpc.stub.StreamObserver;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class DistributedStorageAdminServiceTest {

  @Mock private DistributedStorageAdmin admin;
  private DistributedStorageAdminService adminService;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    // Arrange
    adminService = new DistributedStorageAdminService(admin);
  }

  @Test
  public void createTable_IsCalledWithProperArguments_AdminShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    CreateTableRequest request =
        CreateTableRequest.newBuilder()
            .setNamespace("namespace")
            .setTable("table")
            .setTableMetadata(
                ProtoUtil.toTableMetadata(
                    TableMetadata.newBuilder()
                        .addColumn("col1", DataType.INT)
                        .addColumn("col2", DataType.INT)
                        .addPartitionKey("col1")
                        .build()))
            .build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);

    // Act
    adminService.createTable(request, responseObserver);

    // Assert
    verify(admin).createTable(any(), any(), any(), anyMap());
    verify(responseObserver).onNext(any());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void dropTable_IsCalledWithProperArguments_AdminShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    DropTableRequest request =
        DropTableRequest.newBuilder().setNamespace("namespace").setTable("table").build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);

    // Act
    adminService.dropTable(request, responseObserver);

    // Assert
    verify(admin).dropTable(any(), any());
    verify(responseObserver).onNext(any());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void truncateTable_IsCalledWithProperArguments_AdminShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    TruncateTableRequest request =
        TruncateTableRequest.newBuilder().setNamespace("namespace").setTable("table").build();
    @SuppressWarnings("unchecked")
    StreamObserver<Empty> responseObserver = mock(StreamObserver.class);

    // Act
    adminService.truncateTable(request, responseObserver);

    // Assert
    verify(admin).truncateTable(any(), any());
    verify(responseObserver).onNext(any());
    verify(responseObserver).onCompleted();
  }

  @Test
  public void getTableMetadata_IsCalledWithProperArguments_AdminShouldBeCalledProperly()
      throws ExecutionException {
    // Arrange
    GetTableMetadataRequest request =
        GetTableMetadataRequest.newBuilder().setNamespace("namespace").setTable("table").build();
    @SuppressWarnings("unchecked")
    StreamObserver<GetTableMetadataResponse> responseObserver = mock(StreamObserver.class);

    // Act
    adminService.getTableMetadata(request, responseObserver);

    // Assert
    verify(admin).getTableMetadata(any(), any());
    verify(responseObserver).onNext(any());
    verify(responseObserver).onCompleted();
  }
}
