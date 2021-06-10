package com.scalar.db.server;

import com.google.inject.Inject;
import com.google.protobuf.Empty;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.rpc.AbortRequest;
import com.scalar.db.rpc.CommitRequest;
import com.scalar.db.rpc.DistributedTransactionGrpc;
import com.scalar.db.rpc.GetTransactionStateRequest;
import com.scalar.db.rpc.GetTransactionStateResponse;
import com.scalar.db.rpc.StartTransactionRequest;
import com.scalar.db.rpc.StartTransactionResponse;
import com.scalar.db.rpc.TransactionalGetRequest;
import com.scalar.db.rpc.TransactionalGetResponse;
import com.scalar.db.rpc.TransactionalMutateRequest;
import com.scalar.db.rpc.TransactionalScanRequest;
import com.scalar.db.rpc.TransactionalScanResponse;
import com.scalar.db.util.ProtoUtil;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedTransactionService
    extends DistributedTransactionGrpc.DistributedTransactionImplBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(DistributedTransactionService.class);

  private static final long TRANSACTION_EXPIRATION_TIME_MILLIS = 60000;
  private static final long TRANSACTION_EXPIRATION_INTERVAL_MILLIS = 1000;

  private final DistributedTransactionManager manager;
  private final ConcurrentMap<String, ActiveTransaction> activeTransactions =
      new ConcurrentHashMap<>();

  @Inject
  public DistributedTransactionService(DistributedTransactionManager manager) {
    this.manager = manager;

    Thread transactionExpirationThread =
        new Thread(
            () -> {
              while (true) {
                try {
                  activeTransactions.entrySet().stream()
                      .filter(e -> e.getValue().isExpired())
                      .map(Entry::getKey)
                      .forEach(
                          id -> {
                            activeTransactions.remove(id);
                            LOGGER.warn("the transaction is expired. transactionId: " + id);
                          });
                  TimeUnit.MILLISECONDS.sleep(TRANSACTION_EXPIRATION_INTERVAL_MILLIS);
                } catch (Exception e) {
                  LOGGER.warn("failed to expire transactions", e);
                }
              }
            });
    transactionExpirationThread.setDaemon(true);
    transactionExpirationThread.setName("Transaction expiration thread");
    transactionExpirationThread.start();
  }

  @Override
  public void start(
      StartTransactionRequest request, StreamObserver<StartTransactionResponse> responseObserver) {
    execute(
        () -> {
          DistributedTransaction transaction;
          String transactionId = request.getTransactionId();
          if (transactionId.isEmpty()) {
            transaction = manager.start();
          } else {
            transaction = manager.start(transactionId);
          }
          activeTransactions.put(
              transaction.getId(),
              new ActiveTransaction(transaction, TRANSACTION_EXPIRATION_TIME_MILLIS));
          responseObserver.onNext(
              StartTransactionResponse.newBuilder().setTransactionId(transaction.getId()).build());
          responseObserver.onCompleted();
        },
        responseObserver);
  }

  @Override
  public void get(
      TransactionalGetRequest request, StreamObserver<TransactionalGetResponse> responseObserver) {
    execute(
        () -> {
          DistributedTransaction transaction = getTransaction(request.getTransactionId());
          Get get = ProtoUtil.toGet(request.getGet());
          Optional<Result> result = transaction.get(get);
          TransactionalGetResponse.Builder builder = TransactionalGetResponse.newBuilder();
          result.ifPresent(r -> builder.setResult(ProtoUtil.toResult(r)));
          responseObserver.onNext(builder.build());
          responseObserver.onCompleted();
        },
        responseObserver);
  }

  @Override
  public void scan(
      TransactionalScanRequest request,
      StreamObserver<TransactionalScanResponse> responseObserver) {
    execute(
        () -> {
          DistributedTransaction transaction = getTransaction(request.getTransactionId());
          Scan scan = ProtoUtil.toScan(request.getScan());
          List<Result> results = transaction.scan(scan);
          TransactionalScanResponse.Builder builder = TransactionalScanResponse.newBuilder();
          results.forEach(r -> builder.addResult(ProtoUtil.toResult(r)));
          responseObserver.onNext(builder.build());
          responseObserver.onCompleted();
        },
        responseObserver);
  }

  @Override
  public void mutate(TransactionalMutateRequest request, StreamObserver<Empty> responseObserver) {
    execute(
        () -> {
          DistributedTransaction transaction = getTransaction(request.getTransactionId());

          List<Mutation> mutations = new ArrayList<>(request.getMutationsCount());
          for (com.scalar.db.rpc.Mutation mutation : request.getMutationsList()) {
            mutations.add(ProtoUtil.toMutation(mutation));
          }
          transaction.mutate(mutations);
          responseObserver.onNext(Empty.getDefaultInstance());
          responseObserver.onCompleted();
        },
        responseObserver);
  }

  @Override
  public void commit(CommitRequest request, StreamObserver<Empty> responseObserver) {
    execute(
        () -> {
          DistributedTransaction transaction = getTransaction(request.getTransactionId());
          try {
            transaction.commit();
          } finally {
            activeTransactions.remove(transaction.getId());
          }
          responseObserver.onNext(Empty.getDefaultInstance());
          responseObserver.onCompleted();
        },
        responseObserver);
  }

  @Override
  public void abort(AbortRequest request, StreamObserver<Empty> responseObserver) {
    execute(
        () -> {
          DistributedTransaction transaction = getTransaction(request.getTransactionId());
          try {
            transaction.abort();
          } finally {
            activeTransactions.remove(transaction.getId());
          }
          responseObserver.onNext(Empty.getDefaultInstance());
          responseObserver.onCompleted();
        },
        responseObserver);
  }

  @Override
  public void getState(
      GetTransactionStateRequest request,
      StreamObserver<GetTransactionStateResponse> responseObserver) {
    execute(
        () -> {
          TransactionState state = manager.getState(request.getTransactionId());
          responseObserver.onNext(
              GetTransactionStateResponse.newBuilder()
                  .setState(ProtoUtil.toTransactionState(state))
                  .build());
          responseObserver.onCompleted();
        },
        responseObserver);
  }

  private DistributedTransaction getTransaction(String transactionId) throws TransactionException {
    ActiveTransaction activeTransaction = activeTransactions.get(transactionId);
    if (activeTransaction == null) {
      throw new TransactionException(
          "the transaction is not found. transactionId: " + transactionId);
    }
    return activeTransaction.get();
  }

  private static void execute(
      ThrowableRunnable<Throwable> runnable, StreamObserver<?> responseObserver) {
    try {
      runnable.run();
    } catch (IllegalArgumentException | IllegalStateException e) {
      LOGGER.error("an invalid argument error happened during the execution", e);
      responseObserver.onError(
          Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
    } catch (CrudConflictException | CommitConflictException e) {
      responseObserver.onError(
          Status.FAILED_PRECONDITION.withDescription(e.getMessage()).asRuntimeException());
    } catch (UnknownTransactionStatusException e) {
      LOGGER.error("an unknown transaction error happened during the execution", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asRuntimeException());
    } catch (Throwable t) {
      LOGGER.error("an internal error happened during the execution", t);
      responseObserver.onError(
          Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
      if (t instanceof Error) {
        throw (Error) t;
      }
    }
  }

  private static class ActiveTransaction {

    private final DistributedTransaction transaction;
    private final long transactionExpirationTimeMillis;
    private final AtomicLong expirationTime = new AtomicLong();

    public ActiveTransaction(
        DistributedTransaction transaction, long transactionExpirationTimeMillis) {
      this.transaction = Objects.requireNonNull(transaction);
      this.transactionExpirationTimeMillis = transactionExpirationTimeMillis;
      updateExpirationTime();
    }

    public void updateExpirationTime() {
      expirationTime.set(System.currentTimeMillis() + transactionExpirationTimeMillis);
    }

    public DistributedTransaction get() {
      updateExpirationTime();
      return transaction;
    }

    public boolean isExpired() {
      return System.currentTimeMillis() >= expirationTime.get();
    }
  }
}
