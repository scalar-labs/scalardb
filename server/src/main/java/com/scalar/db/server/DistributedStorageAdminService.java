package com.scalar.db.server;

import com.google.inject.Inject;
import com.google.protobuf.Empty;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.rpc.CreateNamespaceRequest;
import com.scalar.db.rpc.CreateTableRequest;
import com.scalar.db.rpc.DistributedStorageAdminGrpc;
import com.scalar.db.rpc.DropNamespaceRequest;
import com.scalar.db.rpc.DropTableRequest;
import com.scalar.db.rpc.GetNamespaceTableNamesRequest;
import com.scalar.db.rpc.GetNamespaceTableNamesResponse;
import com.scalar.db.rpc.GetTableMetadataRequest;
import com.scalar.db.rpc.GetTableMetadataResponse;
import com.scalar.db.rpc.NamespaceExistsRequest;
import com.scalar.db.rpc.NamespaceExistsResponse;
import com.scalar.db.rpc.TruncateTableRequest;
import com.scalar.db.util.ProtoUtils;
import com.scalar.db.util.ThrowableRunnable;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class DistributedStorageAdminService
    extends DistributedStorageAdminGrpc.DistributedStorageAdminImplBase {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(DistributedStorageAdminService.class);
  private static final String SERVICE_NAME = "distributed_storage_admin";

  private final DistributedStorageAdmin admin;
  private final Metrics metrics;

  @Inject
  public DistributedStorageAdminService(DistributedStorageAdmin admin, Metrics metrics) {
    this.admin = admin;
    this.metrics = metrics;
  }

  @Override
  public void createNamespace(
      CreateNamespaceRequest request, StreamObserver<Empty> responseObserver) {
    execute(
        () -> {
          admin.createNamespace(
              request.getNamespace(), request.getIfNotExists(), request.getOptionsMap());
          responseObserver.onNext(Empty.getDefaultInstance());
          responseObserver.onCompleted();
        },
        responseObserver,
        "create_namespace");
  }

  @Override
  public void dropNamespace(DropNamespaceRequest request, StreamObserver<Empty> responseObserver) {
    execute(
        () -> {
          admin.dropNamespace(request.getNamespace());
          responseObserver.onNext(Empty.getDefaultInstance());
          responseObserver.onCompleted();
        },
        responseObserver,
        "drop_namespace");
  }

  @Override
  public void createTable(CreateTableRequest request, StreamObserver<Empty> responseObserver) {
    execute(
        () -> {
          admin.createTable(
              request.getNamespace(),
              request.getTable(),
              ProtoUtils.toTableMetadata(request.getTableMetadata()),
              request.getIfNotExists(),
              request.getOptionsMap());
          responseObserver.onNext(Empty.getDefaultInstance());
          responseObserver.onCompleted();
        },
        responseObserver,
        "create_table");
  }

  @Override
  public void dropTable(DropTableRequest request, StreamObserver<Empty> responseObserver) {
    execute(
        () -> {
          admin.dropTable(request.getNamespace(), request.getTable());
          responseObserver.onNext(Empty.getDefaultInstance());
          responseObserver.onCompleted();
        },
        responseObserver,
        "drop_table");
  }

  @Override
  public void truncateTable(TruncateTableRequest request, StreamObserver<Empty> responseObserver) {
    execute(
        () -> {
          admin.truncateTable(request.getNamespace(), request.getTable());
          responseObserver.onNext(Empty.getDefaultInstance());
          responseObserver.onCompleted();
        },
        responseObserver,
        "truncate_table");
  }

  @Override
  public void getTableMetadata(
      GetTableMetadataRequest request, StreamObserver<GetTableMetadataResponse> responseObserver) {
    execute(
        () -> {
          TableMetadata tableMetadata =
              admin.getTableMetadata(request.getNamespace(), request.getTable());
          GetTableMetadataResponse.Builder builder = GetTableMetadataResponse.newBuilder();
          if (tableMetadata != null) {
            builder.setTableMetadata(ProtoUtils.toTableMetadata(tableMetadata));
          }
          responseObserver.onNext(builder.build());
          responseObserver.onCompleted();
        },
        responseObserver,
        "get_table_metadata");
  }

  @Override
  public void getNamespaceTableNames(
      GetNamespaceTableNamesRequest request,
      StreamObserver<GetNamespaceTableNamesResponse> responseObserver) {
    execute(
        () -> {
          Set<String> tableNames = admin.getNamespaceTableNames(request.getNamespace());
          GetNamespaceTableNamesResponse.Builder builder =
              GetNamespaceTableNamesResponse.newBuilder();
          tableNames.forEach(builder::addTableName);
          responseObserver.onNext(builder.build());
          responseObserver.onCompleted();
        },
        responseObserver,
        "get_namespace_table_names");
  }

  @Override
  public void namespaceExists(
      NamespaceExistsRequest request, StreamObserver<NamespaceExistsResponse> responseObserver) {
    execute(
        () -> {
          boolean exists = admin.namespaceExists(request.getNamespace());
          responseObserver.onNext(NamespaceExistsResponse.newBuilder().setExists(exists).build());
          responseObserver.onCompleted();
        },
        responseObserver,
        "namespace_exists");
  }

  private void execute(
      ThrowableRunnable<Throwable> runnable, StreamObserver<?> responseObserver, String method) {
    try {
      metrics.measure(SERVICE_NAME, method, runnable);
    } catch (IllegalArgumentException | IllegalStateException e) {
      responseObserver.onError(
          Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
    } catch (Throwable t) {
      LOGGER.error("an internal error happened during the execution", t);
      responseObserver.onError(
          Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
      if (t instanceof Error) {
        throw (Error) t;
      }
    }
  }
}
