package com.scalar.db.storage.rpc;

import static com.scalar.db.util.retry.Retry.executeWithRetries;

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
import com.scalar.db.util.ProtoUtil;
import com.scalar.db.util.ThrowableSupplier;
import com.scalar.db.util.Utility;
import com.scalar.db.util.retry.Retry;
import com.scalar.db.util.retry.ServiceTemporaryUnavailableException;
import io.grpc.ManagedChannel;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class GrpcStorage implements DistributedStorage {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcStorage.class);

  private static final Retry.ExceptionFactory<ExecutionException> EXCEPTION_FACTORY =
      (message, cause) -> {
        if (cause == null) {
          return new ExecutionException(message);
        }
        if (cause instanceof ExecutionException) {
          return (ExecutionException) cause;
        }
        return new ExecutionException(message, cause);
      };

  private final ManagedChannel channel;
  private final DistributedStorageGrpc.DistributedStorageStub stub;
  private final DistributedStorageGrpc.DistributedStorageBlockingStub blockingStub;
  private final GrpcTableMetadataManager metadataManager;

  private Optional<String> namespace;
  private Optional<String> tableName;

  @Inject
  public GrpcStorage(DatabaseConfig config) {
    channel =
        NettyChannelBuilder.forAddress(config.getContactPoints().get(0), config.getContactPort())
            .usePlaintext()
            .build();
    stub = DistributedStorageGrpc.newStub(channel);
    blockingStub = DistributedStorageGrpc.newBlockingStub(channel);
    metadataManager =
        new GrpcTableMetadataManager(DistributedStorageAdminGrpc.newBlockingStub(channel));
    namespace = Optional.empty();
    tableName = Optional.empty();
  }

  @VisibleForTesting
  GrpcStorage(
      DistributedStorageGrpc.DistributedStorageStub stub,
      DistributedStorageGrpc.DistributedStorageBlockingStub blockingStub,
      GrpcTableMetadataManager metadataManager) {
    channel = null;
    this.stub = stub;
    this.blockingStub = blockingStub;
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
    Utility.setTargetToIfNot(get, namespace, tableName);
    return execute(
        () -> {
          GetResponse response =
              blockingStub.get(GetRequest.newBuilder().setGet(ProtoUtil.toGet(get)).build());
          if (response.hasResult()) {
            TableMetadata tableMetadata = metadataManager.getTableMetadata(get);
            return Optional.of(ProtoUtil.toResult(response.getResult(), tableMetadata));
          }
          return Optional.empty();
        });
  }

  @Override
  public Scanner scan(Scan scan) throws ExecutionException {
    Utility.setTargetToIfNot(scan, namespace, tableName);
    return executeWithRetries(
        () -> {
          TableMetadata tableMetadata = metadataManager.getTableMetadata(scan);
          return new ScannerImpl(scan, stub, tableMetadata);
        },
        EXCEPTION_FACTORY);
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
    Utility.setTargetToIfNot(mutation, namespace, tableName);
    execute(
        () -> {
          blockingStub.mutate(
              MutateRequest.newBuilder().addMutation(ProtoUtil.toMutation(mutation)).build());
          return null;
        });
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws ExecutionException {
    Utility.setTargetToIfNot(mutations, namespace, tableName);
    execute(
        () -> {
          MutateRequest.Builder builder = MutateRequest.newBuilder();
          mutations.forEach(m -> builder.addMutation(ProtoUtil.toMutation(m)));
          blockingStub.mutate(builder.build());
          return null;
        });
  }

  private static <T> T execute(ThrowableSupplier<T, ExecutionException> supplier)
      throws ExecutionException {
    return executeWithRetries(
        () -> {
          try {
            return supplier.get();
          } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Code.INVALID_ARGUMENT) {
              throw new IllegalArgumentException(e.getMessage(), e);
            }
            if (e.getStatus().getCode() == Code.FAILED_PRECONDITION) {
              throw new NoMutationException(e.getMessage(), e);
            }
            if (e.getStatus().getCode() == Code.UNAVAILABLE) {
              throw new ServiceTemporaryUnavailableException(e.getMessage(), e);
            }
            throw new ExecutionException(e.getMessage(), e);
          }
        },
        EXCEPTION_FACTORY);
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
