package com.scalar.db.transaction.rpc;

import static com.scalar.db.transaction.rpc.GrpcTransactionManager.EXCEPTION_FACTORY;
import static com.scalar.db.transaction.rpc.GrpcTransactionManager.execute;
import static com.scalar.db.util.retry.Retry.executeWithRetries;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.scalar.db.api.TransactionState;
import com.scalar.db.api.TwoPhaseCommitTransaction;
import com.scalar.db.common.ActiveTransactionManagedTwoPhaseCommitTransactionManager;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.TransactionException;
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
import io.grpc.ManagedChannel;
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
