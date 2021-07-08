package com.scalar.db.transaction.rpc;

import static com.scalar.db.util.retry.Retry.executeWithRetries;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Isolation;
import com.scalar.db.api.SerializableStrategy;
import com.scalar.db.api.TransactionState;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.rpc.AbortRequest;
import com.scalar.db.rpc.AbortResponse;
import com.scalar.db.rpc.DistributedStorageAdminGrpc;
import com.scalar.db.rpc.DistributedTransactionGrpc;
import com.scalar.db.rpc.GetTransactionStateRequest;
import com.scalar.db.rpc.GetTransactionStateResponse;
import com.scalar.db.storage.rpc.GrpcTableMetadataManager;
import com.scalar.db.util.ProtoUtil;
import com.scalar.db.util.ThrowableSupplier;
import com.scalar.db.util.retry.Retry;
import com.scalar.db.util.retry.ServiceTemporaryUnavailableException;
import io.grpc.ManagedChannel;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class GrpcTransactionManager implements DistributedTransactionManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcTransactionManager.class);

  private static final Retry.ExceptionFactory<TransactionException> EXCEPTION_FACTORY =
      (message, cause) -> {
        if (cause == null) {
          return new TransactionException(message);
        }
        if (cause instanceof TransactionException) {
          return (TransactionException) cause;
        }
        return new TransactionException(message, cause);
      };

  private final ManagedChannel channel;
  private final DistributedTransactionGrpc.DistributedTransactionStub stub;
  private final DistributedTransactionGrpc.DistributedTransactionBlockingStub blockingStub;
  private final GrpcTableMetadataManager metadataManager;

  private Optional<String> namespace;
  private Optional<String> tableName;

  @Inject
  public GrpcTransactionManager(DatabaseConfig config) {
    channel =
        NettyChannelBuilder.forAddress(config.getContactPoints().get(0), config.getContactPort())
            .usePlaintext()
            .build();
    stub = DistributedTransactionGrpc.newStub(channel);
    blockingStub = DistributedTransactionGrpc.newBlockingStub(channel);
    metadataManager =
        new GrpcTableMetadataManager(DistributedStorageAdminGrpc.newBlockingStub(channel));
    namespace = Optional.empty();
    tableName = Optional.empty();
  }

  @VisibleForTesting
  GrpcTransactionManager(
      DistributedTransactionGrpc.DistributedTransactionStub stub,
      DistributedTransactionGrpc.DistributedTransactionBlockingStub blockingStub,
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
  public GrpcTransaction start() throws TransactionException {
    return startInternal(null);
  }

  @Override
  public GrpcTransaction start(String txId) throws TransactionException {
    return startInternal(Objects.requireNonNull(txId));
  }

  private GrpcTransaction startInternal(@Nullable String txId) throws TransactionException {
    return executeWithRetries(
        () -> {
          GrpcTransactionOnBidirectionalStream stream =
              new GrpcTransactionOnBidirectionalStream(stub, metadataManager);
          String transactionId = stream.startTransaction(txId);
          return new GrpcTransaction(transactionId, stream, namespace, tableName);
        },
        EXCEPTION_FACTORY);
  }

  @Deprecated
  @Override
  public GrpcTransaction start(Isolation isolation) throws TransactionException {
    return start();
  }

  @Deprecated
  @Override
  public GrpcTransaction start(String txId, Isolation isolation) throws TransactionException {
    return start(txId);
  }

  @Deprecated
  @Override
  public GrpcTransaction start(Isolation isolation, SerializableStrategy strategy)
      throws TransactionException {
    return start();
  }

  @Deprecated
  @Override
  public GrpcTransaction start(SerializableStrategy strategy) throws TransactionException {
    return start();
  }

  @Deprecated
  @Override
  public GrpcTransaction start(String txId, SerializableStrategy strategy)
      throws TransactionException {
    return start(txId);
  }

  @Deprecated
  @Override
  public GrpcTransaction start(String txId, Isolation isolation, SerializableStrategy strategy)
      throws TransactionException {
    return start(txId);
  }

  @Override
  public TransactionState getState(String txId) throws TransactionException {
    return execute(
        () -> {
          GetTransactionStateResponse response =
              blockingStub.getState(
                  GetTransactionStateRequest.newBuilder().setTransactionId(txId).build());
          return ProtoUtil.toTransactionState(response.getState());
        });
  }

  @Override
  public TransactionState abort(String txId) throws TransactionException {
    return execute(
        () -> {
          AbortResponse response =
              blockingStub.abort(AbortRequest.newBuilder().setTransactionId(txId).build());
          return ProtoUtil.toTransactionState(response.getState());
        });
  }

  private static <T> T execute(ThrowableSupplier<T, TransactionException> supplier)
      throws TransactionException {
    return executeWithRetries(
        () -> {
          try {
            return supplier.get();
          } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Code.INVALID_ARGUMENT) {
              throw new IllegalArgumentException(e.getMessage());
            }
            if (e.getStatus().getCode() == Code.UNAVAILABLE) {
              throw new ServiceTemporaryUnavailableException(e.getMessage());
            }
            throw new TransactionException(e.getMessage());
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
