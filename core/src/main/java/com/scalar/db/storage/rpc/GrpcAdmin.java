package com.scalar.db.storage.rpc;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
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
import com.scalar.db.util.ThrowableSupplier;
import io.grpc.ManagedChannel;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class GrpcAdmin implements DistributedStorageAdmin {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcAdmin.class);

  private final GrpcConfig config;
  private final ManagedChannel channel;
  private final DistributedStorageAdminGrpc.DistributedStorageAdminBlockingStub stub;

  @Inject
  public GrpcAdmin(GrpcConfig config) {
    this.config = config;
    channel =
        NettyChannelBuilder.forAddress(config.getContactPoints().get(0), config.getContactPort())
            .usePlaintext()
            .build();
    stub = DistributedStorageAdminGrpc.newBlockingStub(channel);
  }

  public GrpcAdmin(ManagedChannel channel, GrpcConfig config) {
    this.channel = channel;
    this.config = config;
    stub = DistributedStorageAdminGrpc.newBlockingStub(channel);
  }

  @VisibleForTesting
  GrpcAdmin(
      DistributedStorageAdminGrpc.DistributedStorageAdminBlockingStub stub, GrpcConfig config) {
    channel = null;
    this.stub = stub;
    this.config = config;
  }

  @Override
  public void createNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    createNamespace(namespace, false, options);
  }

  @Override
  public void createNamespace(String namespace, boolean ifNotExists) throws ExecutionException {
    createNamespace(namespace, ifNotExists, Collections.emptyMap());
  }

  @Override
  public void createNamespace(String namespace) throws ExecutionException {
    createNamespace(namespace, false, Collections.emptyMap());
  }

  @Override
  public void createNamespace(String namespace, boolean ifNotExists, Map<String, String> options)
      throws ExecutionException {
    execute(
        () ->
            stub.withDeadlineAfter(config.getDeadlineDurationMillis(), TimeUnit.MILLISECONDS)
                .createNamespace(
                    CreateNamespaceRequest.newBuilder()
                        .setNamespace(namespace)
                        .setIfNotExists(ifNotExists)
                        .putAllOptions(options)
                        .build()));
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, boolean ifNotExists)
      throws ExecutionException {
    createTable(namespace, table, metadata, ifNotExists, Collections.emptyMap());
  }

  @Override
  public void createTable(String namespace, String table, TableMetadata metadata)
      throws ExecutionException {
    createTable(namespace, table, metadata, false, Collections.emptyMap());
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    createTable(namespace, table, metadata, false, options);
  }

  @Override
  public void createTable(
      String namespace,
      String table,
      TableMetadata metadata,
      boolean ifNotExists,
      Map<String, String> options)
      throws ExecutionException {
    execute(
        () ->
            stub.withDeadlineAfter(config.getDeadlineDurationMillis(), TimeUnit.MILLISECONDS)
                .createTable(
                    CreateTableRequest.newBuilder()
                        .setNamespace(namespace)
                        .setTable(table)
                        .setTableMetadata(ProtoUtils.toTableMetadata(metadata))
                        .setIfNotExists(ifNotExists)
                        .putAllOptions(options)
                        .build()));
  }

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    execute(
        () ->
            stub.withDeadlineAfter(config.getDeadlineDurationMillis(), TimeUnit.MILLISECONDS)
                .dropTable(
                    DropTableRequest.newBuilder().setNamespace(namespace).setTable(table).build()));
  }

  @Override
  public void dropNamespace(String namespace) throws ExecutionException {
    execute(
        () ->
            stub.withDeadlineAfter(config.getDeadlineDurationMillis(), TimeUnit.MILLISECONDS)
                .dropNamespace(DropNamespaceRequest.newBuilder().setNamespace(namespace).build()));
  }

  @Override
  public void truncateTable(String namespace, String table) throws ExecutionException {
    execute(
        () ->
            stub.withDeadlineAfter(config.getDeadlineDurationMillis(), TimeUnit.MILLISECONDS)
                .truncateTable(
                    TruncateTableRequest.newBuilder()
                        .setNamespace(namespace)
                        .setTable(table)
                        .build()));
  }

  @Override
  public TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException {
    return execute(
        () -> {
          GetTableMetadataResponse response =
              stub.withDeadlineAfter(config.getDeadlineDurationMillis(), TimeUnit.MILLISECONDS)
                  .getTableMetadata(
                      GetTableMetadataRequest.newBuilder()
                          .setNamespace(namespace)
                          .setTable(table)
                          .build());
          if (!response.hasTableMetadata()) {
            return null;
          }
          return ProtoUtils.toTableMetadata(response.getTableMetadata());
        });
  }

  @Override
  public Set<String> getNamespaceTableNames(String namespace) throws ExecutionException {
    return execute(
        () -> {
          GetNamespaceTableNamesResponse response =
              stub.withDeadlineAfter(config.getDeadlineDurationMillis(), TimeUnit.MILLISECONDS)
                  .getNamespaceTableNames(
                      GetNamespaceTableNamesRequest.newBuilder().setNamespace(namespace).build());
          return new HashSet<>(response.getTableNameList());
        });
  }

  @Override
  public boolean namespaceExists(String namespace) throws ExecutionException {
    return execute(
        () -> {
          NamespaceExistsResponse response =
              stub.withDeadlineAfter(config.getDeadlineDurationMillis(), TimeUnit.MILLISECONDS)
                  .namespaceExists(
                      NamespaceExistsRequest.newBuilder().setNamespace(namespace).build());
          return response.getExists();
        });
  }

  private static <T> T execute(ThrowableSupplier<T, ExecutionException> supplier)
      throws ExecutionException {
    try {
      return supplier.get();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Code.INVALID_ARGUMENT) {
        throw new IllegalArgumentException(e.getMessage(), e);
      }
      throw new ExecutionException(e.getMessage(), e);
    }
  }

  @Override
  public void close() {
    try {
      channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOGGER.warn("failed to shutdown the channel", e);
    }
  }
}
