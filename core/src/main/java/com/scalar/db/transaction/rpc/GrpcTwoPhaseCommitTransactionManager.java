package com.scalar.db.transaction.rpc;

import static com.scalar.db.transaction.rpc.GrpcTransactionManager.EXCEPTION_FACTORY;
import static com.scalar.db.transaction.rpc.GrpcTransactionManager.execute;
import static com.scalar.db.util.retry.Retry.executeWithRetries;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TransactionState;
import com.scalar.db.api.TwoPhaseCommitTransaction;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.common.ActiveTransactionManagedTwoPhaseCommitTransactionManager;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.rpc.AbortRequest;
import com.scalar.db.rpc.AbortResponse;
import com.scalar.db.rpc.GetTransactionStateRequest;
import com.scalar.db.rpc.GetTransactionStateResponse;
import com.scalar.db.rpc.RollbackRequest;
import com.scalar.db.rpc.RollbackResponse;
import com.scalar.db.rpc.TwoPhaseCommitTransactionGrpc;
import com.scalar.db.storage.rpc.GrpcAdmin;
import com.scalar.db.storage.rpc.GrpcConfig;
import com.scalar.db.storage.rpc.GrpcUtils;
import com.scalar.db.util.ProtoUtils;
import com.scalar.db.util.ThrowableFunction;
import io.grpc.ManagedChannel;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class GrpcTwoPhaseCommitTransactionManager
    extends ActiveTransactionManagedTwoPhaseCommitTransactionManager {
  private static final Logger logger =
      LoggerFactory.getLogger(GrpcTwoPhaseCommitTransactionManager.class);

  private final GrpcConfig config;
  private final ManagedChannel channel;
  private final TwoPhaseCommitTransactionGrpc.TwoPhaseCommitTransactionStub stub;
  private final TwoPhaseCommitTransactionGrpc.TwoPhaseCommitTransactionBlockingStub blockingStub;
  private final TableMetadataManager metadataManager;

  @Inject
  public GrpcTwoPhaseCommitTransactionManager(DatabaseConfig databaseConfig) {
    super(databaseConfig);
    config = new GrpcConfig(databaseConfig);
    channel = GrpcUtils.createChannel(config);
    stub = TwoPhaseCommitTransactionGrpc.newStub(channel);
    blockingStub = TwoPhaseCommitTransactionGrpc.newBlockingStub(channel);
    metadataManager =
        new TableMetadataManager(
            new GrpcAdmin(channel, config), databaseConfig.getMetadataCacheExpirationTimeSecs());
  }

  @VisibleForTesting
  GrpcTwoPhaseCommitTransactionManager(
      GrpcConfig config,
      DatabaseConfig databaseConfig,
      TwoPhaseCommitTransactionGrpc.TwoPhaseCommitTransactionStub stub,
      TwoPhaseCommitTransactionGrpc.TwoPhaseCommitTransactionBlockingStub blockingStub,
      TableMetadataManager metadataManager) {
    super(databaseConfig);
    this.config = config;
    channel = null;
    this.stub = stub;
    this.blockingStub = blockingStub;
    this.metadataManager = metadataManager;
  }

  @Override
  public TwoPhaseCommitTransaction begin() throws TransactionException {
    return beginInternal(null);
  }

  @Override
  public TwoPhaseCommitTransaction begin(String txId) throws TransactionException {
    return beginInternal(txId);
  }

  private TwoPhaseCommitTransaction beginInternal(@Nullable String txId)
      throws TransactionException {
    return executeWithRetries(
        () -> {
          GrpcTwoPhaseCommitTransactionOnBidirectionalStream stream = getStream();
          String transactionId = stream.beginTransaction(txId);
          GrpcTwoPhaseCommitTransaction transaction = createTransaction(stream, transactionId);
          return decorate(transaction);
        },
        EXCEPTION_FACTORY);
  }

  private GrpcTwoPhaseCommitTransaction createTransaction(
      GrpcTwoPhaseCommitTransactionOnBidirectionalStream stream, String transactionId) {
    GrpcTwoPhaseCommitTransaction transaction =
        new GrpcTwoPhaseCommitTransaction(transactionId, stream);
    getNamespace().ifPresent(transaction::withNamespace);
    getTable().ifPresent(transaction::withTable);

    return transaction;
  }

  @Override
  public TwoPhaseCommitTransaction start() throws TransactionException {
    return startInternal(null);
  }

  @Override
  public TwoPhaseCommitTransaction start(String txId) throws TransactionException {
    return startInternal(txId);
  }

  private TwoPhaseCommitTransaction startInternal(@Nullable String txId)
      throws TransactionException {
    return executeWithRetries(
        () -> {
          GrpcTwoPhaseCommitTransactionOnBidirectionalStream stream = getStream();
          String transactionId = stream.startTransaction(txId);
          GrpcTwoPhaseCommitTransaction transaction = createTransaction(stream, transactionId);
          return decorate(transaction);
        },
        EXCEPTION_FACTORY);
  }

  @Override
  public TwoPhaseCommitTransaction join(String txId) throws TransactionException {
    return executeWithRetries(
        () -> {
          GrpcTwoPhaseCommitTransactionOnBidirectionalStream stream = getStream();
          stream.joinTransaction(txId);
          GrpcTwoPhaseCommitTransaction transaction = createTransaction(stream, txId);
          return decorate(transaction);
        },
        EXCEPTION_FACTORY);
  }

  @VisibleForTesting
  GrpcTwoPhaseCommitTransactionOnBidirectionalStream getStream() {
    return new GrpcTwoPhaseCommitTransactionOnBidirectionalStream(config, stub, metadataManager);
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
      ThrowableFunction<TwoPhaseCommitTransaction, R, TransactionException> throwableFunction)
      throws CrudException, UnknownTransactionStatusException {
    TwoPhaseCommitTransaction transaction;
    try {
      transaction = begin();
    } catch (TransactionNotFoundException e) {
      throw new CrudConflictException(e.getMessage(), e, e.getTransactionId().orElse(null));
    } catch (TransactionException e) {
      throw new CrudException(e.getMessage(), e, e.getTransactionId().orElse(null));
    }

    try {
      R result = throwableFunction.apply(transaction);
      transaction.prepare();
      transaction.validate();
      transaction.commit();
      return result;
    } catch (CrudException e) {
      rollbackTransaction(transaction);
      throw e;
    } catch (PreparationConflictException
        | ValidationConflictException
        | CommitConflictException e) {
      rollbackTransaction(transaction);
      throw new CrudConflictException(e.getMessage(), e, e.getTransactionId().orElse(null));
    } catch (UnknownTransactionStatusException e) {
      throw e;
    } catch (TransactionException e) {
      rollbackTransaction(transaction);
      throw new CrudException(e.getMessage(), e, e.getTransactionId().orElse(null));
    }
  }

  private void rollbackTransaction(TwoPhaseCommitTransaction transaction) {
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

  @Override
  public void close() {
    try {
      channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      logger.warn("Failed to shutdown the channel", e);
    }
  }
}
