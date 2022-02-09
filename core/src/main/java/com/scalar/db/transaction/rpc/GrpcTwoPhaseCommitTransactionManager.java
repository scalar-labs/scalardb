package com.scalar.db.transaction.rpc;

import static com.scalar.db.transaction.rpc.GrpcTransactionManager.DEFAULT_SCALAR_DB_SERVER_PORT;
import static com.scalar.db.transaction.rpc.GrpcTransactionManager.EXCEPTION_FACTORY;
import static com.scalar.db.transaction.rpc.GrpcTransactionManager.execute;
import static com.scalar.db.util.retry.Retry.executeWithRetries;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.rpc.AbortRequest;
import com.scalar.db.rpc.AbortResponse;
import com.scalar.db.rpc.GetTransactionStateRequest;
import com.scalar.db.rpc.GetTransactionStateResponse;
import com.scalar.db.rpc.TwoPhaseCommitTransactionGrpc;
import com.scalar.db.storage.rpc.GrpcAdmin;
import com.scalar.db.storage.rpc.GrpcConfig;
import com.scalar.db.transaction.common.AbstractTwoPhaseCommitTransactionManager;
import com.scalar.db.util.ActiveExpiringMap;
import com.scalar.db.util.ProtoUtils;
import com.scalar.db.util.TableMetadataManager;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class GrpcTwoPhaseCommitTransactionManager extends AbstractTwoPhaseCommitTransactionManager {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(GrpcTwoPhaseCommitTransactionManager.class);

  private static final long TRANSACTION_LIFETIME_MILLIS = 60000;
  private static final long TRANSACTION_EXPIRATION_INTERVAL_MILLIS = 1000;

  private final GrpcConfig config;
  private final ManagedChannel channel;
  private final TwoPhaseCommitTransactionGrpc.TwoPhaseCommitTransactionStub stub;
  private final TwoPhaseCommitTransactionGrpc.TwoPhaseCommitTransactionBlockingStub blockingStub;
  private final TableMetadataManager metadataManager;

  @Nullable
  private final ActiveExpiringMap<String, GrpcTwoPhaseCommitTransaction> activeTransactions;

  @Inject
  public GrpcTwoPhaseCommitTransactionManager(GrpcConfig config) {
    this.config = config;
    channel =
        NettyChannelBuilder.forAddress(
                config.getContactPoints().get(0),
                config.getContactPort() == 0
                    ? DEFAULT_SCALAR_DB_SERVER_PORT
                    : config.getContactPort())
            .usePlaintext()
            .build();
    stub = TwoPhaseCommitTransactionGrpc.newStub(channel);
    blockingStub = TwoPhaseCommitTransactionGrpc.newBlockingStub(channel);
    metadataManager =
        new TableMetadataManager(
            new GrpcAdmin(channel, config), config.getTableMetadataCacheExpirationTimeSecs());
    if (config.isActiveTransactionsManagementEnabled()) {
      activeTransactions =
          new ActiveExpiringMap<>(
              TRANSACTION_LIFETIME_MILLIS,
              TRANSACTION_EXPIRATION_INTERVAL_MILLIS,
              t -> {
                LOGGER.warn("the transaction is expired. transactionId: {}", t.getId());
                try {
                  t.rollback();
                } catch (RollbackException e) {
                  LOGGER.warn("rollback failed", e);
                }
              });
    } else {
      activeTransactions = null;
    }
  }

  @VisibleForTesting
  GrpcTwoPhaseCommitTransactionManager(
      GrpcConfig config,
      TwoPhaseCommitTransactionGrpc.TwoPhaseCommitTransactionStub stub,
      TwoPhaseCommitTransactionGrpc.TwoPhaseCommitTransactionBlockingStub blockingStub,
      TableMetadataManager metadataManager) {
    this.config = config;
    channel = null;
    this.stub = stub;
    this.blockingStub = blockingStub;
    this.metadataManager = metadataManager;
    if (config.isActiveTransactionsManagementEnabled()) {
      activeTransactions = new ActiveExpiringMap<>(Long.MAX_VALUE, Long.MAX_VALUE, t -> {});
    } else {
      activeTransactions = null;
    }
  }

  @Override
  public GrpcTwoPhaseCommitTransaction start() throws TransactionException {
    return startInternal(null);
  }

  @Override
  public GrpcTwoPhaseCommitTransaction start(String txId) throws TransactionException {
    return startInternal(txId);
  }

  private GrpcTwoPhaseCommitTransaction startInternal(@Nullable String txId)
      throws TransactionException {
    return executeWithRetries(
        () -> {
          GrpcTwoPhaseCommitTransactionOnBidirectionalStream stream =
              new GrpcTwoPhaseCommitTransactionOnBidirectionalStream(config, stub, metadataManager);
          String transactionId = stream.startTransaction(txId);
          GrpcTwoPhaseCommitTransaction transaction =
              new GrpcTwoPhaseCommitTransaction(transactionId, stream, true, this);
          getNamespace().ifPresent(transaction::withNamespace);
          getTable().ifPresent(transaction::withTable);
          return transaction;
        },
        EXCEPTION_FACTORY);
  }

  @Override
  public GrpcTwoPhaseCommitTransaction join(String txId) throws TransactionException {
    return executeWithRetries(
        () -> {
          GrpcTwoPhaseCommitTransactionOnBidirectionalStream stream =
              new GrpcTwoPhaseCommitTransactionOnBidirectionalStream(config, stub, metadataManager);
          stream.joinTransaction(txId);
          GrpcTwoPhaseCommitTransaction transaction =
              new GrpcTwoPhaseCommitTransaction(txId, stream, false, this);
          getNamespace().ifPresent(transaction::withNamespace);
          getTable().ifPresent(transaction::withTable);
          if (activeTransactions != null) {
            if (activeTransactions.putIfAbsent(txId, transaction) != null) {
              transaction.rollback();
              throw new TransactionException(
                  "The transaction associated with the specified transaction ID already exists");
            }
          }
          return transaction;
        },
        EXCEPTION_FACTORY);
  }

  @Override
  public GrpcTwoPhaseCommitTransaction resume(String txId) throws TransactionException {
    if (activeTransactions == null) {
      throw new UnsupportedOperationException(
          "unsupported when setting \""
              + GrpcConfig.ACTIVE_TRANSACTIONS_MANAGEMENT_ENABLED
              + "\" to false");
    }

    return activeTransactions
        .get(txId)
        .orElseThrow(
            () ->
                new TransactionException(
                    "A transaction associated with the specified transaction ID is not found. "
                        + "It might have been expired"));
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

  @Override
  public void close() {
    try {
      channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOGGER.warn("failed to shutdown the channel", e);
    }
  }

  void removeTransaction(String txId) {
    if (activeTransactions == null) {
      return;
    }
    activeTransactions.remove(txId);
  }

  void updateTransactionExpirationTime(String txId) {
    if (activeTransactions == null) {
      return;
    }
    activeTransactions.updateExpirationTime(txId);
  }
}
