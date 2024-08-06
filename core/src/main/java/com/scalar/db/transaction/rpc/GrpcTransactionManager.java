package com.scalar.db.transaction.rpc;

import static com.scalar.db.util.retry.Retry.executeWithRetries;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Isolation;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.SerializableStrategy;
import com.scalar.db.api.TransactionState;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.common.ActiveTransactionManagedDistributedTransactionManager;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.rpc.AbortRequest;
import com.scalar.db.rpc.AbortResponse;
import com.scalar.db.rpc.DistributedTransactionGrpc;
import com.scalar.db.rpc.GetTransactionStateRequest;
import com.scalar.db.rpc.GetTransactionStateResponse;
import com.scalar.db.rpc.RollbackRequest;
import com.scalar.db.rpc.RollbackResponse;
import com.scalar.db.storage.rpc.GrpcAdmin;
import com.scalar.db.storage.rpc.GrpcConfig;
import com.scalar.db.storage.rpc.GrpcUtils;
import com.scalar.db.util.ProtoUtils;
import com.scalar.db.util.ThrowableFunction;
import com.scalar.db.util.ThrowableSupplier;
import com.scalar.db.util.retry.Retry;
import com.scalar.db.util.retry.ServiceTemporaryUnavailableException;
import io.grpc.ManagedChannel;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class GrpcTransactionManager extends ActiveTransactionManagedDistributedTransactionManager {
  private static final Logger logger = LoggerFactory.getLogger(GrpcTransactionManager.class);

  static final Retry.ExceptionFactory<TransactionException> EXCEPTION_FACTORY =
      (message, cause) -> {
        if (cause == null) {
          return new TransactionException(message, null);
        }
        if (cause instanceof TransactionException) {
          return (TransactionException) cause;
        }
        return new TransactionException(message, cause, null);
      };

  private final GrpcConfig config;
  private final ManagedChannel channel;
  private final DistributedTransactionGrpc.DistributedTransactionStub stub;
  private final DistributedTransactionGrpc.DistributedTransactionBlockingStub blockingStub;
  private final TableMetadataManager metadataManager;

  @Inject
  public GrpcTransactionManager(DatabaseConfig databaseConfig) {
    super(databaseConfig);
    config = new GrpcConfig(databaseConfig);
    channel = GrpcUtils.createChannel(config);
    stub = DistributedTransactionGrpc.newStub(channel);
    blockingStub = DistributedTransactionGrpc.newBlockingStub(channel);
    metadataManager =
        new TableMetadataManager(
            new GrpcAdmin(channel, config), databaseConfig.getMetadataCacheExpirationTimeSecs());
  }

  @VisibleForTesting
  GrpcTransactionManager(
      DatabaseConfig databaseConfig,
      GrpcConfig config,
      DistributedTransactionGrpc.DistributedTransactionStub stub,
      DistributedTransactionGrpc.DistributedTransactionBlockingStub blockingStub,
      TableMetadataManager metadataManager) {
    super(databaseConfig);
    this.config = config;
    channel = null;
    this.stub = stub;
    this.blockingStub = blockingStub;
    this.metadataManager = metadataManager;
  }

  @Override
  public DistributedTransaction begin() throws TransactionException {
    return beginInternal(null);
  }

  @Override
  public DistributedTransaction begin(String txId) throws TransactionException {
    return beginInternal(Objects.requireNonNull(txId));
  }

  private DistributedTransaction beginInternal(@Nullable String txId) throws TransactionException {
    return executeWithRetries(
        () -> {
          GrpcTransactionOnBidirectionalStream stream = getStream();
          String transactionId = stream.beginTransaction(txId);
          GrpcTransaction transaction = createTransaction(stream, transactionId);
          return decorate(transaction);
        },
        EXCEPTION_FACTORY);
  }

  private GrpcTransaction createTransaction(
      GrpcTransactionOnBidirectionalStream stream, String transactionId) {
    GrpcTransaction transaction = new GrpcTransaction(transactionId, stream);
    getNamespace().ifPresent(transaction::withNamespace);
    getTable().ifPresent(transaction::withTable);

    return transaction;
  }

  @Override
  public DistributedTransaction start() throws TransactionException {
    return startInternal(null);
  }

  @Override
  public DistributedTransaction start(String txId) throws TransactionException {
    return startInternal(Objects.requireNonNull(txId));
  }

  private DistributedTransaction startInternal(@Nullable String txId) throws TransactionException {
    return executeWithRetries(
        () -> {
          GrpcTransactionOnBidirectionalStream stream = getStream();
          String transactionId = stream.startTransaction(txId);
          GrpcTransaction transaction = createTransaction(stream, transactionId);
          return decorate(transaction);
        },
        EXCEPTION_FACTORY);
  }

  @VisibleForTesting
  GrpcTransactionOnBidirectionalStream getStream() {
    return new GrpcTransactionOnBidirectionalStream(config, stub, metadataManager);
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  @Override
  public DistributedTransaction start(Isolation isolation) throws TransactionException {
    return begin();
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  @Override
  public DistributedTransaction start(String txId, Isolation isolation)
      throws TransactionException {
    return begin(txId);
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  @Override
  public DistributedTransaction start(Isolation isolation, SerializableStrategy strategy)
      throws TransactionException {
    return begin();
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  @Override
  public DistributedTransaction start(SerializableStrategy strategy) throws TransactionException {
    return begin();
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  @Override
  public DistributedTransaction start(String txId, SerializableStrategy strategy)
      throws TransactionException {
    return begin(txId);
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  @Override
  public DistributedTransaction start(
      String txId, Isolation isolation, SerializableStrategy strategy) throws TransactionException {
    return begin(txId);
  }

  @Override
  public Optional<Result> get(Get get) throws CrudException, UnknownTransactionStatusException {
    return executeTransaction(t -> t.get(copyAndSetTargetToIfNot(get)));
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException, UnknownTransactionStatusException {
    return executeTransaction(t -> t.scan(copyAndSetTargetToIfNot(scan)));
  }

  @Deprecated
  @Override
  public void put(Put put) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.put(copyAndSetTargetToIfNot(put));
          return null;
        });
  }

  @Deprecated
  @Override
  public void put(List<Put> puts) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.put(copyAndSetTargetToIfNot(puts));
          return null;
        });
  }

  @Override
  public void insert(Insert insert) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.insert(copyAndSetTargetToIfNot(insert));
          return null;
        });
  }

  @Override
  public void upsert(Upsert upsert) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.upsert(copyAndSetTargetToIfNot(upsert));
          return null;
        });
  }

  @Override
  public void update(Update update) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.update(copyAndSetTargetToIfNot(update));
          return null;
        });
  }

  @Override
  public void delete(Delete delete) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.delete(copyAndSetTargetToIfNot(delete));
          return null;
        });
  }

  @Deprecated
  @Override
  public void delete(List<Delete> deletes) throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.delete(copyAndSetTargetToIfNot(deletes));
          return null;
        });
  }

  @Override
  public void mutate(List<? extends Mutation> mutations)
      throws CrudException, UnknownTransactionStatusException {
    executeTransaction(
        t -> {
          t.mutate(copyAndSetTargetToIfNot(mutations));
          return null;
        });
  }

  private <R> R executeTransaction(
      ThrowableFunction<DistributedTransaction, R, TransactionException> throwableFunction)
      throws CrudException, UnknownTransactionStatusException {
    DistributedTransaction transaction;
    try {
      transaction = begin();
    } catch (TransactionNotFoundException e) {
      throw new CrudConflictException(e.getMessage(), e, e.getTransactionId().orElse(null));
    } catch (TransactionException e) {
      throw new CrudException(e.getMessage(), e, e.getTransactionId().orElse(null));
    }

    try {
      R result = throwableFunction.apply(transaction);
      transaction.commit();
      return result;
    } catch (CrudException e) {
      rollbackTransaction(transaction);
      throw e;
    } catch (CommitConflictException e) {
      rollbackTransaction(transaction);
      throw new CrudConflictException(e.getMessage(), e, e.getTransactionId().orElse(null));
    } catch (UnknownTransactionStatusException e) {
      throw e;
    } catch (TransactionException e) {
      rollbackTransaction(transaction);
      throw new CrudException(e.getMessage(), e, e.getTransactionId().orElse(null));
    }
  }

  private void rollbackTransaction(DistributedTransaction transaction) {
    try {
      transaction.rollback();
    } catch (RollbackException e) {
      logger.warn("Rolling back the transaction failed", e);
    }
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
  public TransactionState rollback(String txId) throws TransactionException {
    return execute(
        () -> {
          RollbackResponse response =
              blockingStub
                  .withDeadlineAfter(config.getDeadlineDurationMillis(), TimeUnit.MILLISECONDS)
                  .rollback(RollbackRequest.newBuilder().setTransactionId(txId).build());
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
            throw new TransactionException(e.getMessage(), e, null);
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
