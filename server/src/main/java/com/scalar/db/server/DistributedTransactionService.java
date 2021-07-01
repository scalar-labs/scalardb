package com.scalar.db.server;

import com.google.inject.Inject;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.rpc.AbortRequest;
import com.scalar.db.rpc.CommitRequest;
import com.scalar.db.rpc.DistributedTransactionGrpc;
import com.scalar.db.rpc.GetTransactionStateRequest;
import com.scalar.db.rpc.GetTransactionStateResponse;
import com.scalar.db.rpc.StartTransactionRequest;
import com.scalar.db.rpc.StartTransactionResponse;
import com.scalar.db.rpc.TransactionRequest;
import com.scalar.db.rpc.TransactionRequest.RequestCase;
import com.scalar.db.rpc.TransactionResponse;
import com.scalar.db.rpc.TransactionResponse.Error.ErrorCode;
import com.scalar.db.rpc.TransactionalGetRequest;
import com.scalar.db.rpc.TransactionalGetResponse;
import com.scalar.db.rpc.TransactionalMutateRequest;
import com.scalar.db.rpc.TransactionalScanRequest;
import com.scalar.db.rpc.TransactionalScanResponse;
import com.scalar.db.util.ProtoUtil;
import com.scalar.db.util.ThrowableRunnable;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedTransactionService
    extends DistributedTransactionGrpc.DistributedTransactionImplBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(DistributedTransactionService.class);

  private final DistributedTransactionManager manager;

  @Inject
  public DistributedTransactionService(DistributedTransactionManager manager) {
    this.manager = manager;
  }

  @Override
  public StreamObserver<TransactionRequest> transaction(
      StreamObserver<TransactionResponse> responseObserver) {
    return new TransactionStreamObserver(manager, responseObserver);
  }

  @Override
  public void getState(
      GetTransactionStateRequest request,
      StreamObserver<GetTransactionStateResponse> responseObserver) {
    try {
      TransactionState state = manager.getState(request.getTransactionId());
      responseObserver.onNext(
          GetTransactionStateResponse.newBuilder()
              .setState(ProtoUtil.toTransactionState(state))
              .build());
      responseObserver.onCompleted();
    } catch (IllegalArgumentException | IllegalStateException e) {
      LOGGER.error("an invalid argument error happened during the execution", e);
      responseObserver.onError(
          Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
    } catch (Throwable t) {
      LOGGER.error("an internal error happened during the execution", t);
      responseObserver.onError(
          Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
      if (t instanceof Error) {
        throw (Error) t;
      }
    }
  }

  private static class TransactionStreamObserver implements StreamObserver<TransactionRequest> {

    private final DistributedTransactionManager manager;
    private final StreamObserver<TransactionResponse> responseObserver;

    private DistributedTransaction transaction;

    public TransactionStreamObserver(
        DistributedTransactionManager manager,
        StreamObserver<TransactionResponse> responseObserver) {
      this.manager = manager;
      this.responseObserver = responseObserver;
    }

    @Override
    public void onNext(TransactionRequest request) {
      if (request.getRequestCase() == RequestCase.START_TRANSACTION_REQUEST) {
        startTransaction(request);
      } else {
        executeTransaction(request);
      }
    }

    private void startTransaction(TransactionRequest transactionRequest) {
      if (transactionStarted()) {
        respondInvalidArgumentError("transaction is already started");
        return;
      }

      try {
        StartTransactionRequest request = transactionRequest.getStartTransactionRequest();
        if (!request.hasTransactionId()) {
          transaction = manager.start();
        } else {
          transaction = manager.start(request.getTransactionId());
        }
        responseObserver.onNext(
            TransactionResponse.newBuilder()
                .setStartTransactionResponse(
                    StartTransactionResponse.newBuilder()
                        .setTransactionId(transaction.getId())
                        .build())
                .build());
      } catch (Throwable t) {
        LOGGER.error("an internal error happened when starting a transaction", t);
        respondInternalError(t.getMessage());
        if (t instanceof Error) {
          throw (Error) t;
        }
      }
    }

    private void executeTransaction(TransactionRequest request) {
      if (!transactionStarted()) {
        respondInvalidArgumentError("transaction is not started");
        return;
      }

      TransactionResponse.Builder responseBuilder = TransactionResponse.newBuilder();

      boolean completed = false;
      switch (request.getRequestCase()) {
        case TRANSACTIONAL_GET_REQUEST:
          get(request.getTransactionalGetRequest(), responseBuilder);
          break;
        case TRANSACTIONAL_SCAN_REQUEST:
          scan(request.getTransactionalScanRequest(), responseBuilder);
          break;
        case TRANSACTIONAL_MUTATE_REQUEST:
          mutate(request.getTransactionalMutateRequest(), responseBuilder);
          break;
        case COMMIT_REQUEST:
          commit(request.getCommitRequest(), responseBuilder);
          completed = true;
          break;
        case ABORT_REQUEST:
          abort(request.getAbortRequest(), responseBuilder);
          completed = true;
          break;
        default:
          respondInvalidArgumentError("invalid request specified: " + request.getRequestCase());
          return;
      }

      responseObserver.onNext(responseBuilder.build());
      if (completed) {
        responseObserver.onCompleted();
      }
    }

    private boolean transactionStarted() {
      return transaction != null;
    }

    @Override
    public void onError(Throwable t) {
      LOGGER.error("an error received", t);
      abort();
    }

    @Override
    public void onCompleted() {}

    private void get(TransactionalGetRequest request, TransactionResponse.Builder responseBuilder) {
      execute(
          () -> {
            Get get = ProtoUtil.toGet(request.getGet());
            Optional<Result> result = transaction.get(get);
            TransactionalGetResponse.Builder builder = TransactionalGetResponse.newBuilder();
            result.ifPresent(r -> builder.setResult(ProtoUtil.toResult(r)));
            responseBuilder.setTransactionalGetResponse(builder);
          },
          responseBuilder);
    }

    private void scan(
        TransactionalScanRequest request, TransactionResponse.Builder responseBuilder) {
      execute(
          () -> {
            Scan scan = ProtoUtil.toScan(request.getScan());
            List<Result> results = transaction.scan(scan);
            TransactionalScanResponse.Builder builder = TransactionalScanResponse.newBuilder();
            results.forEach(r -> builder.addResult(ProtoUtil.toResult(r)));
            responseBuilder.setTransactionalScanResponse(builder);
          },
          responseBuilder);
    }

    private void mutate(
        TransactionalMutateRequest request, TransactionResponse.Builder responseBuilder) {
      execute(
          () ->
              transaction.mutate(
                  request.getMutationList().stream()
                      .map(ProtoUtil::toMutation)
                      .collect(Collectors.toList())),
          responseBuilder);
    }

    private void commit(CommitRequest unused, TransactionResponse.Builder responseBuilder) {
      execute(() -> transaction.commit(), responseBuilder);
    }

    private void abort(AbortRequest unused, TransactionResponse.Builder responseBuilder) {
      execute(() -> transaction.abort(), responseBuilder);
    }

    private void abort() {
      if (transaction != null) {
        try {
          transaction.abort();
        } catch (AbortException e) {
          LOGGER.warn("abort failed", e);
        }
      }
    }

    private void respondInternalError(String message) {
      responseObserver.onError(Status.INTERNAL.withDescription(message).asRuntimeException());
      abort();
    }

    private void respondInvalidArgumentError(String message) {
      responseObserver.onError(
          Status.INVALID_ARGUMENT.withDescription(message).asRuntimeException());
      abort();
    }

    private void execute(
        ThrowableRunnable<Throwable> runnable, TransactionResponse.Builder responseBuilder) {
      try {
        runnable.run();
      } catch (IllegalArgumentException | IllegalStateException e) {
        LOGGER.error("an invalid argument error happened during the execution", e);
        responseBuilder.setError(
            TransactionResponse.Error.newBuilder()
                .setErrorCode(ErrorCode.INVALID_ARGUMENT)
                .setMessage(e.getMessage())
                .build());
      } catch (CrudConflictException | CommitConflictException e) {
        responseBuilder.setError(
            TransactionResponse.Error.newBuilder()
                .setErrorCode(ErrorCode.CONFLICT)
                .setMessage(e.getMessage())
                .build());
      } catch (UnknownTransactionStatusException e) {
        LOGGER.error("an unknown transaction error happened during the execution", e);
        responseBuilder.setError(
            TransactionResponse.Error.newBuilder()
                .setErrorCode(ErrorCode.UNKNOWN_TRANSACTION)
                .setMessage(e.getMessage())
                .build());
      } catch (Throwable t) {
        LOGGER.error("an internal error happened during the execution", t);
        responseBuilder.setError(
            TransactionResponse.Error.newBuilder()
                .setErrorCode(ErrorCode.OTHER)
                .setMessage(t.getMessage())
                .build());
        if (t instanceof Error) {
          throw (Error) t;
        }
      }
    }
  }
}
