package com.scalar.db.transaction.rpc;

import static com.scalar.db.util.retry.Retry.executeWithRetries;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.scalar.db.api.Isolation;
import com.scalar.db.api.SerializableStrategy;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.rpc.AbortRequest;
import com.scalar.db.rpc.AbortResponse;
import com.scalar.db.rpc.DistributedTransactionGrpc;
import com.scalar.db.rpc.GetTransactionStateRequest;
import com.scalar.db.rpc.GetTransactionStateResponse;
import com.scalar.db.storage.rpc.GrpcAdmin;
import com.scalar.db.storage.rpc.GrpcConfig;
import com.scalar.db.transaction.common.AbstractDistributedTransactionManager;
import com.scalar.db.util.ProtoUtils;
import com.scalar.db.util.TableMetadataManager;
import com.scalar.db.util.ThrowableSupplier;
import com.scalar.db.util.retry.Retry;
import com.scalar.db.util.retry.ServiceTemporaryUnavailableException;
import io.grpc.ManagedChannel;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class GrpcTransactionManager extends AbstractDistributedTransactionManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcTransactionManager.class);
  static final int DEFAULT_SCALAR_DB_SERVER_PORT = 60051;

  static final Retry.ExceptionFactory<TransactionException> EXCEPTION_FACTORY =
      (message, cause) -> {
        if (cause == null) {
          return new TransactionException(message);
        }
        if (cause instanceof TransactionException) {
          return (TransactionException) cause;
        }
        return new TransactionException(message, cause);
      };

  private final GrpcConfig config;
  private final ManagedChannel channel;
  private final DistributedTransactionGrpc.DistributedTransactionStub stub;
  private final DistributedTransactionGrpc.DistributedTransactionBlockingStub blockingStub;
  private final TableMetadataManager metadataManager;

  @Inject
  public GrpcTransactionManager(GrpcConfig config) {
    this.config = config;
    channel =
        NettyChannelBuilder.forAddress(
                config.getContactPoints().get(0),
                config.getContactPort() == 0
                    ? DEFAULT_SCALAR_DB_SERVER_PORT
                    : config.getContactPort())
            .usePlaintext()
            .build();
    stub = DistributedTransactionGrpc.newStub(channel);
    blockingStub = DistributedTransactionGrpc.newBlockingStub(channel);
    metadataManager =
        new TableMetadataManager(
            new GrpcAdmin(channel, config), config.getTableMetadataCacheExpirationTimeSecs());
  }

  @VisibleForTesting
  GrpcTransactionManager(
      GrpcConfig config,
      DistributedTransactionGrpc.DistributedTransactionStub stub,
      DistributedTransactionGrpc.DistributedTransactionBlockingStub blockingStub,
      TableMetadataManager metadataManager) {
    this.config = config;
    channel = null;
    this.stub = stub;
    this.blockingStub = blockingStub;
    this.metadataManager = metadataManager;
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
              new GrpcTransactionOnBidirectionalStream(config, stub, metadataManager);
          String transactionId = stream.startTransaction(txId);
          GrpcTransaction transaction = new GrpcTransaction(transactionId, stream);
          getNamespace().ifPresent(transaction::withNamespace);
          getTable().ifPresent(transaction::withTable);
          return transaction;
        },
        EXCEPTION_FACTORY);
  }

  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  @Override
  public GrpcTransaction start(Isolation isolation) throws TransactionException {
    return start();
  }

  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  @Override
  public GrpcTransaction start(String txId, Isolation isolation) throws TransactionException {
    return start(txId);
  }

  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  @Override
  public GrpcTransaction start(Isolation isolation, SerializableStrategy strategy)
      throws TransactionException {
    return start();
  }

  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  @Override
  public GrpcTransaction start(SerializableStrategy strategy) throws TransactionException {
    return start();
  }

  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  @Override
  public GrpcTransaction start(String txId, SerializableStrategy strategy)
      throws TransactionException {
    return start(txId);
  }

  @SuppressWarnings("InlineMeSuggester")
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
              blockingStub
                  .withDeadlineAfter(config.getDeadlineDurationMillis(), TimeUnit.MILLISECONDS)
                  .getState(GetTransactionStateRequest.newBuilder().setTransactionId(txId).build());
          return ProtoUtils.toTransactionState(response.getState());
        });
  }

  @Override
  public TransactionState abort(String txId) throws TransactionException {
    return execute(
        () -> {
          AbortResponse response =
              blockingStub
                  .withDeadlineAfter(config.getDeadlineDurationMillis(), TimeUnit.MILLISECONDS)
                  .abort(AbortRequest.newBuilder().setTransactionId(txId).build());
          return ProtoUtils.toTransactionState(response.getState());
        });
  }

  static <T> T execute(ThrowableSupplier<T, TransactionException> supplier)
      throws TransactionException {
    return executeWithRetries(
        () -> {
          try {
            return supplier.get();
          } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Code.INVALID_ARGUMENT) {
              throw new IllegalArgumentException(e.getMessage(), e);
            }
            if (e.getStatus().getCode() == Code.UNAVAILABLE) {
              throw new ServiceTemporaryUnavailableException(e.getMessage(), e);
            }
            throw new TransactionException(e.getMessage(), e);
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
