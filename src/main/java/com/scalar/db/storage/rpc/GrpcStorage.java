package com.scalar.db.storage.rpc;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.rpc.DistributedStorageAdminGrpc;
import com.scalar.db.rpc.DistributedStorageGrpc;
import com.scalar.db.rpc.GetRequest;
import com.scalar.db.rpc.GetResponse;
import com.scalar.db.rpc.MutateRequest;
import com.scalar.db.rpc.util.ProtoUtil;
import com.scalar.db.storage.common.util.Utility;
import io.grpc.ManagedChannel;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class GrpcStorage implements DistributedStorage {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcStorage.class);

  private final ManagedChannel channel;
  private final DistributedStorageGrpc.DistributedStorageBlockingStub stub;
  private final GrpcTableMetadataManager metadataManager;

  private Optional<String> namespace;
  private Optional<String> tableName;

  @Inject
  public GrpcStorage(DatabaseConfig config) {
    channel =
        NettyChannelBuilder.forAddress(config.getContactPoints().get(0), config.getContactPort())
            .usePlaintext()
            .build();
    stub = DistributedStorageGrpc.newBlockingStub(channel);
    metadataManager =
        new GrpcTableMetadataManager(DistributedStorageAdminGrpc.newBlockingStub(channel));
    namespace = Optional.empty();
    tableName = Optional.empty();
  }

  @VisibleForTesting
  GrpcStorage(
      DistributedStorageGrpc.DistributedStorageBlockingStub stub,
      GrpcTableMetadataManager metadataManager) {
    channel = null;
    this.stub = stub;
    this.metadataManager = metadataManager;
    namespace = Optional.empty();
    tableName = Optional.empty();
  }

  @Override
  public void with(String namespace, String tableName) {
    this.namespace = Optional.ofNullable(namespace);
    this.tableName = Optional.ofNullable(tableName);
  }

  @Override
  public void withNamespace(String namespace) {
    this.namespace = Optional.ofNullable(namespace);
  }

  @Override
  public Optional<String> getNamespace() {
    return namespace;
  }

  @Override
  public void withTable(String tableName) {
    this.tableName = Optional.ofNullable(tableName);
  }

  @Override
  public Optional<String> getTable() {
    return tableName;
  }

  @Override
  public Optional<Result> get(Get get) throws ExecutionException {
    return execute(
        () -> {
          Utility.setTargetToIfNot(get, Optional.empty(), namespace, tableName);

          GetResponse response =
              stub.get(GetRequest.newBuilder().setGet(ProtoUtil.toGet(get)).build());
          if (response.hasResult()) {
            TableMetadata tableMetadata = metadataManager.getTableMetadata(get);
            return Optional.of(ProtoUtil.toResult(response.getResult(), tableMetadata));
          }
          return Optional.empty();
        });
  }

  @Override
  public Scanner scan(Scan scan) throws ExecutionException {
    return execute(
        () -> {
          Utility.setTargetToIfNot(scan, Optional.empty(), namespace, tableName);

          TableMetadata tableMetadata = metadataManager.getTableMetadata(scan);
          return new ScannerImpl(scan, stub, tableMetadata);
        });
  }

  @Override
  public void put(Put put) throws ExecutionException {
    mutate(put);
  }

  @Override
  public void put(List<Put> puts) throws ExecutionException {
    mutate(puts);
  }

  @Override
  public void delete(Delete delete) throws ExecutionException {
    mutate(delete);
  }

  @Override
  public void delete(List<Delete> deletes) throws ExecutionException {
    mutate(deletes);
  }

  private void mutate(Mutation mutation) throws ExecutionException {
    execute(
        () -> {
          Utility.setTargetToIfNot(mutation, Optional.empty(), namespace, tableName);

          stub.mutate(
              MutateRequest.newBuilder().addMutations(ProtoUtil.toMutation(mutation)).build());
          return null;
        });
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws ExecutionException {
    execute(
        () -> {
          Utility.setTargetToIfNot(mutations, Optional.empty(), namespace, tableName);

          MutateRequest.Builder builder = MutateRequest.newBuilder();
          mutations.forEach(m -> builder.addMutations(ProtoUtil.toMutation(m)));
          stub.mutate(builder.build());
          return null;
        });
  }

  static <T> T execute(Supplier<T> supplier) throws ExecutionException {
    try {
      return supplier.get();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Code.INVALID_ARGUMENT) {
        throw new IllegalArgumentException(e.getMessage());
      } else if (e.getStatus().getCode() == Code.FAILED_PRECONDITION) {
        throw new NoMutationException(e.getMessage());
      }
      throw new ExecutionException(e.getMessage());
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
