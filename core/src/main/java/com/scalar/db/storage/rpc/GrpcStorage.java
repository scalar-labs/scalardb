package com.scalar.db.storage.rpc;

import static com.scalar.db.util.retry.Retry.executeWithRetries;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.AbstractDistributedStorage;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.rpc.DistributedStorageGrpc;
import com.scalar.db.rpc.GetRequest;
import com.scalar.db.rpc.GetResponse;
import com.scalar.db.rpc.MutateRequest;
import com.scalar.db.util.ProtoUtils;
import com.scalar.db.util.ThrowableSupplier;
import com.scalar.db.util.retry.Retry;
import com.scalar.db.util.retry.ServiceTemporaryUnavailableException;
import io.grpc.ManagedChannel;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class GrpcStorage extends AbstractDistributedStorage {
  private static final Logger logger = LoggerFactory.getLogger(GrpcStorage.class);

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

  private final GrpcConfig config;
  private final ManagedChannel channel;
  private final DistributedStorageGrpc.DistributedStorageStub stub;
  private final DistributedStorageGrpc.DistributedStorageBlockingStub blockingStub;
  private final TableMetadataManager metadataManager;

  @Inject
  public GrpcStorage(DatabaseConfig databaseConfig) {
    super(databaseConfig);
    config = new GrpcConfig(databaseConfig);
    channel = GrpcUtils.createChannel(config);
    stub = DistributedStorageGrpc.newStub(channel);
    blockingStub = DistributedStorageGrpc.newBlockingStub(channel);
    metadataManager =
        new TableMetadataManager(
            new GrpcAdmin(channel, config), databaseConfig.getMetadataCacheExpirationTimeSecs());
  }

  @VisibleForTesting
  GrpcStorage(
      DatabaseConfig databaseConfig,
      GrpcConfig config,
      DistributedStorageGrpc.DistributedStorageStub stub,
      DistributedStorageGrpc.DistributedStorageBlockingStub blockingStub,
      TableMetadataManager metadataManager) {
    super(databaseConfig);
    this.config = config;
    channel = null;
    this.stub = stub;
    this.blockingStub = blockingStub;
    this.metadataManager = metadataManager;
  }

  @Override
  public Optional<Result> get(Get originalGet) throws ExecutionException {
    Get get = copyAndSetTargetToIfNot(originalGet);
    return execute(
        () -> {
          GetResponse response =
              blockingStub
                  .withDeadlineAfter(config.getDeadlineDurationMillis(), TimeUnit.MILLISECONDS)
                  .get(GetRequest.newBuilder().setGet(ProtoUtils.toGet(get)).build());
          if (response.hasResult()) {
            TableMetadata tableMetadata = metadataManager.getTableMetadata(get);
            return Optional.of(ProtoUtils.toResult(response.getResult(), tableMetadata));
          }
          return Optional.empty();
        });
  }

  @Override
  public Scanner scan(Scan originalScan) throws ExecutionException {
    Scan scan = copyAndSetTargetToIfNot(originalScan);
    return executeWithRetries(
        () -> {
          TableMetadata tableMetadata = metadataManager.getTableMetadata(scan);
          return new ScannerImpl(config, scan, stub, tableMetadata);
        },
        EXCEPTION_FACTORY);
  }

  @Override
  public void put(Put put) throws ExecutionException {
    put = copyAndSetTargetToIfNot(put);
    mutate(put);
  }

  @Override
  public void put(List<Put> puts) throws ExecutionException {
    mutate(puts);
  }

  @Override
  public void delete(Delete delete) throws ExecutionException {
    delete = copyAndSetTargetToIfNot(delete);
    mutate(delete);
  }

  @Override
  public void delete(List<Delete> deletes) throws ExecutionException {
    mutate(deletes);
  }

  private void mutate(Mutation mutation) throws ExecutionException {
    execute(
        () -> {
          blockingStub
              .withDeadlineAfter(config.getDeadlineDurationMillis(), TimeUnit.MILLISECONDS)
              .mutate(
                  MutateRequest.newBuilder().addMutations(ProtoUtils.toMutation(mutation)).build());
          return null;
        });
  }

  @Override
  public void mutate(List<? extends Mutation> originalMutations) throws ExecutionException {
    List<? extends Mutation> mutations = copyAndSetTargetToIfNot(originalMutations);
    execute(
        () -> {
          MutateRequest.Builder builder = MutateRequest.newBuilder();
          mutations.forEach(m -> builder.addMutations(ProtoUtils.toMutation(m)));
          blockingStub
              .withDeadlineAfter(config.getDeadlineDurationMillis(), TimeUnit.MILLISECONDS)
              .mutate(builder.build());
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
      logger.warn("Failed to shutdown the channel", e);
    }
  }
}
