package com.scalar.db.transaction.rpc;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.rpc.CoordinatorTablesExistRequest;
import com.scalar.db.rpc.CoordinatorTablesExistResponse;
import com.scalar.db.rpc.CreateCoordinatorTablesRequest;
import com.scalar.db.rpc.CreateIndexRequest;
import com.scalar.db.rpc.CreateNamespaceRequest;
import com.scalar.db.rpc.CreateTableRequest;
import com.scalar.db.rpc.DistributedTransactionAdminGrpc;
import com.scalar.db.rpc.DropCoordinatorTablesRequest;
import com.scalar.db.rpc.DropIndexRequest;
import com.scalar.db.rpc.DropNamespaceRequest;
import com.scalar.db.rpc.DropTableRequest;
import com.scalar.db.rpc.GetNamespaceTableNamesRequest;
import com.scalar.db.rpc.GetNamespaceTableNamesResponse;
import com.scalar.db.rpc.GetTableMetadataRequest;
import com.scalar.db.rpc.GetTableMetadataResponse;
import com.scalar.db.rpc.NamespaceExistsRequest;
import com.scalar.db.rpc.NamespaceExistsResponse;
import com.scalar.db.rpc.TruncateCoordinatorTablesRequest;
import com.scalar.db.rpc.TruncateTableRequest;
import com.scalar.db.storage.rpc.GrpcAdmin;
import com.scalar.db.storage.rpc.GrpcConfig;
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
public class GrpcTransactionAdmin implements DistributedTransactionAdmin {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcAdmin.class);

  private final GrpcConfig config;
  private final ManagedChannel channel;
  private final DistributedTransactionAdminGrpc.DistributedTransactionAdminBlockingStub stub;

  @Inject
  public GrpcTransactionAdmin(DatabaseConfig databaseConfig) {
    config = new GrpcConfig(databaseConfig);
    channel =
        NettyChannelBuilder.forAddress(config.getHost(), config.getPort()).usePlaintext().build();
    stub = DistributedTransactionAdminGrpc.newBlockingStub(channel);
  }

  @VisibleForTesting
  GrpcTransactionAdmin(
      DistributedTransactionAdminGrpc.DistributedTransactionAdminBlockingStub stub,
      GrpcConfig config) {
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
    dropTable(namespace, table, false);
  }

  @Override
  public void dropTable(String namespace, String table, boolean ifExists)
      throws ExecutionException {
    execute(
        () ->
            stub.withDeadlineAfter(config.getDeadlineDurationMillis(), TimeUnit.MILLISECONDS)
                .dropTable(
                    DropTableRequest.newBuilder()
                        .setNamespace(namespace)
                        .setTable(table)
                        .setIfExists(ifExists)
                        .build()));
  }

  @Override
  public void dropNamespace(String namespace) throws ExecutionException {
    dropNamespace(namespace, false);
  }

  @Override
  public void dropNamespace(String namespace, boolean ifExists) throws ExecutionException {
    execute(
        () ->
            stub.withDeadlineAfter(config.getDeadlineDurationMillis(), TimeUnit.MILLISECONDS)
                .dropNamespace(
                    DropNamespaceRequest.newBuilder()
                        .setNamespace(namespace)
                        .setIfExists(ifExists)
                        .build()));
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
  public void createIndex(String namespace, String table, String columnName)
      throws ExecutionException {
    createIndex(namespace, table, columnName, false, Collections.emptyMap());
  }

  @Override
  public void createIndex(String namespace, String table, String columnName, boolean ifNotExists)
      throws ExecutionException {
    createIndex(namespace, table, columnName, ifNotExists, Collections.emptyMap());
  }

  @Override
  public void createIndex(
      String namespace, String table, String columnName, Map<String, String> options)
      throws ExecutionException {
    createIndex(namespace, table, columnName, false, options);
  }

  @Override
  public void createIndex(
      String namespace,
      String table,
      String columnName,
      boolean ifNotExists,
      Map<String, String> options)
      throws ExecutionException {
    execute(
        () ->
            stub.withDeadlineAfter(config.getDeadlineDurationMillis(), TimeUnit.MILLISECONDS)
                .createIndex(
                    CreateIndexRequest.newBuilder()
                        .setNamespace(namespace)
                        .setTable(table)
                        .setColumnName(columnName)
                        .setIfNotExists(ifNotExists)
                        .putAllOptions(options)
                        .build()));
  }

  @Override
  public void dropIndex(String namespace, String table, String columnName)
      throws ExecutionException {
    dropIndex(namespace, table, columnName, false);
  }

  @Override
  public void dropIndex(String namespace, String table, String columnName, boolean ifExists)
      throws ExecutionException {
    execute(
        () ->
            stub.withDeadlineAfter(config.getDeadlineDurationMillis(), TimeUnit.MILLISECONDS)
                .dropIndex(
                    DropIndexRequest.newBuilder()
                        .setNamespace(namespace)
                        .setTable(table)
                        .setColumnName(columnName)
                        .setIfExists(ifExists)
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

  @Override
  public void createCoordinatorTables() throws ExecutionException {
    createCoordinatorTables(false, Collections.emptyMap());
  }

  @Override
  public void createCoordinatorTables(Map<String, String> options) throws ExecutionException {
    createCoordinatorTables(false, options);
  }

  @Override
  public void createCoordinatorTables(boolean ifNotExist) throws ExecutionException {
    createCoordinatorTables(ifNotExist, Collections.emptyMap());
  }

  @Override
  public void createCoordinatorTables(boolean ifNotExist, Map<String, String> options)
      throws ExecutionException {
    execute(
        () ->
            stub.withDeadlineAfter(config.getDeadlineDurationMillis(), TimeUnit.MILLISECONDS)
                .createCoordinatorTables(
                    CreateCoordinatorTablesRequest.newBuilder()
                        .setIfNotExist(ifNotExist)
                        .putAllOptions(options)
                        .build()));
  }

  @Override
  public void dropCoordinatorTables() throws ExecutionException {
    dropCoordinatorTables(false);
  }

  @Override
  public void dropCoordinatorTables(boolean ifExist) throws ExecutionException {
    execute(
        () ->
            stub.withDeadlineAfter(config.getDeadlineDurationMillis(), TimeUnit.MILLISECONDS)
                .dropCoordinatorTables(
                    DropCoordinatorTablesRequest.newBuilder().setIfExist(ifExist).build()));
  }

  @Override
  public void truncateCoordinatorTables() throws ExecutionException {
    execute(
        () ->
            stub.withDeadlineAfter(config.getDeadlineDurationMillis(), TimeUnit.MILLISECONDS)
                .truncateCoordinatorTables(TruncateCoordinatorTablesRequest.getDefaultInstance()));
  }

  @Override
  public boolean coordinatorTablesExist() throws ExecutionException {
    return execute(
        () -> {
          CoordinatorTablesExistResponse response =
              stub.withDeadlineAfter(config.getDeadlineDurationMillis(), TimeUnit.MILLISECONDS)
                  .coordinatorTablesExist(CoordinatorTablesExistRequest.getDefaultInstance());
          return response.getExist();
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
