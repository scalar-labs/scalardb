package com.scalar.db.server;

import com.google.inject.Inject;
import com.scalar.db.api.Get;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TransactionState;
import com.scalar.db.api.TwoPhaseCommitTransaction;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.rpc.AbortRequest;
import com.scalar.db.rpc.AbortResponse;
import com.scalar.db.rpc.GetTransactionStateRequest;
import com.scalar.db.rpc.GetTransactionStateResponse;
import com.scalar.db.rpc.TwoPhaseCommitTransactionGrpc;
import com.scalar.db.rpc.TwoPhaseCommitTransactionRequest;
import com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.CommitRequest;
import com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.GetRequest;
import com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.JoinRequest;
import com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.MutateRequest;
import com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.PrepareRequest;
import com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.RequestCase;
import com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.RollbackRequest;
import com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.ScanRequest;
import com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.StartRequest;
import com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.ValidateRequest;
import com.scalar.db.rpc.TwoPhaseCommitTransactionResponse;
import com.scalar.db.rpc.TwoPhaseCommitTransactionResponse.Error.ErrorCode;
import com.scalar.db.rpc.TwoPhaseCommitTransactionResponse.GetResponse;
import com.scalar.db.rpc.TwoPhaseCommitTransactionResponse.ScanResponse;
import com.scalar.db.rpc.TwoPhaseCommitTransactionResponse.StartResponse;
import com.scalar.db.util.ProtoUtils;
import com.scalar.db.util.ThrowableRunnable;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class TwoPhaseCommitTransactionService
    extends TwoPhaseCommitTransactionGrpc.TwoPhaseCommitTransactionImplBase {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(TwoPhaseCommitTransactionService.class);
  private static final String SERVICE_NAME = "two_phase_commit_transaction";

  private final TwoPhaseCommitTransactionManager manager;
  private final GateKeeper gateKeeper;
  private final Metrics metrics;

  @Inject
  public TwoPhaseCommitTransactionService(
      TwoPhaseCommitTransactionManager manager, GateKeeper gateKeeper, Metrics metrics) {
    this.manager = manager;
    this.gateKeeper = gateKeeper;
    this.metrics = metrics;
  }

  @Override
  public StreamObserver<TwoPhaseCommitTransactionRequest> twoPhaseCommitTransaction(
      StreamObserver<TwoPhaseCommitTransactionResponse> responseObserver) {
    return new TwoPhaseCommitTransactionStreamObserver(
        manager, responseObserver, metrics, this::preProcess, this::postProcess);
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
                  .setState(ProtoUtils.toTransactionState(state))
                  .build());
          responseObserver.onCompleted();
        },
        responseObserver,
        "get_state");
  }

  @Override
  public void abort(AbortRequest request, StreamObserver<AbortResponse> responseObserver) {
    execute(
        () -> {
          TransactionState state = manager.abort(request.getTransactionId());
          responseObserver.onNext(
              AbortResponse.newBuilder().setState(ProtoUtils.toTransactionState(state)).build());
          responseObserver.onCompleted();
        },
        responseObserver,
        "abort");
  }

  private void execute(
      ThrowableRunnable<Throwable> runnable, StreamObserver<?> responseObserver, String method) {
    if (!preProcess(responseObserver)) {
      // Unavailable
      return;
    }

    try {
      metrics.measure(SERVICE_NAME, method, runnable);
    } catch (IllegalArgumentException | IllegalStateException e) {
      responseObserver.onError(
          Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
    } catch (Throwable t) {
      LOGGER.error("an internal error happened during the execution", t);
      responseObserver.onError(
          Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
      if (t instanceof Error) {
        throw (Error) t;
      }
    } finally {
      postProcess();
    }
  }

  private boolean preProcess(StreamObserver<?> responseObserver) {
    if (!gateKeeper.letIn()) {
      respondUnavailableError(responseObserver);
      return false;
    }
    return true;
  }

  private void respondUnavailableError(StreamObserver<?> responseObserver) {
    responseObserver.onError(
        Status.UNAVAILABLE.withDescription("the server is paused").asRuntimeException());
  }

  private void postProcess() {
    gateKeeper.letOut();
  }

  private static class TwoPhaseCommitTransactionStreamObserver
      implements StreamObserver<TwoPhaseCommitTransactionRequest> {

    private final TwoPhaseCommitTransactionManager manager;
    private final StreamObserver<TwoPhaseCommitTransactionResponse> responseObserver;
    private final Metrics metrics;
    private final Function<StreamObserver<?>, Boolean> preProcessor;
    private final Runnable postProcessor;
    private final AtomicBoolean preProcessed = new AtomicBoolean();

    private TwoPhaseCommitTransaction transaction;

    public TwoPhaseCommitTransactionStreamObserver(
        TwoPhaseCommitTransactionManager manager,
        StreamObserver<TwoPhaseCommitTransactionResponse> responseObserver,
        Metrics metrics,
        Function<StreamObserver<?>, Boolean> preProcessor,
        Runnable postProcessor) {
      this.manager = manager;
      this.responseObserver = responseObserver;
      this.metrics = metrics;
      this.preProcessor = preProcessor;
      this.postProcessor = postProcessor;
    }

    @Override
    public void onNext(TwoPhaseCommitTransactionRequest request) {
      if (preProcessed.compareAndSet(false, true)) {
        if (!preProcessor.apply(responseObserver)) {
          return;
        }
      }

      if (request.getRequestCase() == RequestCase.START_REQUEST) {
        startTransaction(request);
      } else if (request.getRequestCase() == RequestCase.JOIN_REQUEST) {
        joinTransaction(request);
      } else {
        executeTransaction(request);
      }
    }

    private void startTransaction(TwoPhaseCommitTransactionRequest transactionRequest) {
      if (transactionStarted()) {
        respondInvalidArgumentError("transaction is already started");
        return;
      }

      try {
        metrics.measure(
            SERVICE_NAME,
            "transaction.start",
            () -> {
              StartRequest request = transactionRequest.getStartRequest();
              if (!request.hasTransactionId()) {
                transaction = manager.start();
              } else {
                transaction = manager.start(request.getTransactionId());
              }
            });
        responseObserver.onNext(
            TwoPhaseCommitTransactionResponse.newBuilder()
                .setStartResponse(
                    StartResponse.newBuilder().setTransactionId(transaction.getId()).build())
                .build());
      } catch (IllegalArgumentException e) {
        respondInvalidArgumentError(e.getMessage());
      } catch (Throwable t) {
        LOGGER.error("an internal error happened when starting a transaction", t);
        respondInternalError(t.getMessage());
        if (t instanceof Error) {
          throw (Error) t;
        }
      }
    }

    private void joinTransaction(TwoPhaseCommitTransactionRequest transactionRequest) {
      if (transactionStarted()) {
        respondInvalidArgumentError("transaction is already started");
        return;
      }

      try {
        metrics.measure(
            SERVICE_NAME,
            "transaction.join",
            () -> {
              JoinRequest request = transactionRequest.getJoinRequest();
              transaction = manager.join(request.getTransactionId());
            });
        responseObserver.onNext(TwoPhaseCommitTransactionResponse.getDefaultInstance());
      } catch (IllegalArgumentException e) {
        respondInvalidArgumentError(e.getMessage());
      } catch (Throwable t) {
        LOGGER.error("an internal error happened when joining a transaction", t);
        respondInternalError(t.getMessage());
        if (t instanceof Error) {
          throw (Error) t;
        }
      }
    }

    private void executeTransaction(TwoPhaseCommitTransactionRequest request) {
      if (!transactionStarted()) {
        respondInvalidArgumentError("transaction is not started");
        return;
      }

      TwoPhaseCommitTransactionResponse.Builder responseBuilder =
          TwoPhaseCommitTransactionResponse.newBuilder();

      boolean completed = false;
      switch (request.getRequestCase()) {
        case GET_REQUEST:
          get(request.getGetRequest(), responseBuilder);
          break;
        case SCAN_REQUEST:
          scan(request.getScanRequest(), responseBuilder);
          break;
        case MUTATE_REQUEST:
          mutate(request.getMutateRequest(), responseBuilder);
          break;
        case PREPARE_REQUEST:
          prepare(request.getPrepareRequest(), responseBuilder);
          break;
        case VALIDATE_REQUEST:
          validate(request.getValidateRequest(), responseBuilder);
          break;
        case COMMIT_REQUEST:
          commit(request.getCommitRequest(), responseBuilder);
          completed = true;
          break;
        case ROLLBACK_REQUEST:
          rollback(request.getRollbackRequest(), responseBuilder);
          completed = true;
          break;
        default:
          respondInvalidArgumentError("invalid request specified: " + request.getRequestCase());
          return;
      }

      responseObserver.onNext(responseBuilder.build());
      if (completed) {
        responseObserver.onCompleted();
        postProcessor.run();
      }
    }

    private boolean transactionStarted() {
      return transaction != null;
    }

    @Override
    public void onError(Throwable t) {
      LOGGER.error("an error received", t);
      cleanUp();
    }

    @Override
    public void onCompleted() {}

    private void get(
        GetRequest request, TwoPhaseCommitTransactionResponse.Builder responseBuilder) {
      execute(
          () -> {
            Get get = ProtoUtils.toGet(request.getGet());
            Optional<Result> result = transaction.get(get);
            GetResponse.Builder builder = GetResponse.newBuilder();
            result.ifPresent(r -> builder.setResult(ProtoUtils.toResult(r)));
            responseBuilder.setGetResponse(builder);
          },
          responseBuilder,
          "transaction.get");
    }

    private void scan(
        ScanRequest request, TwoPhaseCommitTransactionResponse.Builder responseBuilder) {
      execute(
          () -> {
            Scan scan = ProtoUtils.toScan(request.getScan());
            List<Result> results = transaction.scan(scan);
            ScanResponse.Builder builder = ScanResponse.newBuilder();
            results.forEach(r -> builder.addResult(ProtoUtils.toResult(r)));
            responseBuilder.setScanResponse(builder);
          },
          responseBuilder,
          "transaction.scan");
    }

    private void mutate(
        MutateRequest request, TwoPhaseCommitTransactionResponse.Builder responseBuilder) {
      execute(
          () ->
              transaction.mutate(
                  request.getMutationList().stream()
                      .map(ProtoUtils::toMutation)
                      .collect(Collectors.toList())),
          responseBuilder,
          "transaction.mutate");
    }

    private void prepare(
        PrepareRequest unused, TwoPhaseCommitTransactionResponse.Builder responseBuilder) {
      execute(() -> transaction.prepare(), responseBuilder, "transaction.prepare");
    }

    private void validate(
        ValidateRequest unused, TwoPhaseCommitTransactionResponse.Builder responseBuilder) {
      execute(() -> transaction.validate(), responseBuilder, "transaction.validate");
    }

    private void commit(
        CommitRequest unused, TwoPhaseCommitTransactionResponse.Builder responseBuilder) {
      execute(() -> transaction.commit(), responseBuilder, "transaction.commit");
    }

    private void rollback(
        RollbackRequest unused, TwoPhaseCommitTransactionResponse.Builder responseBuilder) {
      execute(() -> transaction.rollback(), responseBuilder, "transaction.rollback");
    }

    private void cleanUp() {
      if (transaction != null) {
        try {
          transaction.rollback();
        } catch (RollbackException e) {
          LOGGER.warn("rollback failed", e);
        }
      }

      postProcessor.run();
    }

    private void respondInternalError(String message) {
      responseObserver.onError(Status.INTERNAL.withDescription(message).asRuntimeException());
      cleanUp();
    }

    private void respondInvalidArgumentError(String message) {
      responseObserver.onError(
          Status.INVALID_ARGUMENT.withDescription(message).asRuntimeException());
      cleanUp();
    }

    private void execute(
        ThrowableRunnable<Throwable> runnable,
        TwoPhaseCommitTransactionResponse.Builder responseBuilder,
        String method) {
      try {
        metrics.measure(SERVICE_NAME, method, runnable);
      } catch (IllegalArgumentException | IllegalStateException e) {
        responseBuilder.setError(
            TwoPhaseCommitTransactionResponse.Error.newBuilder()
                .setErrorCode(ErrorCode.INVALID_ARGUMENT)
                .setMessage(e.getMessage())
                .build());
      } catch (CrudConflictException
          | CommitConflictException
          | PreparationConflictException
          | ValidationConflictException e) {
        responseBuilder.setError(
            TwoPhaseCommitTransactionResponse.Error.newBuilder()
                .setErrorCode(ErrorCode.CONFLICT)
                .setMessage(e.getMessage())
                .build());
      } catch (UnknownTransactionStatusException e) {
        responseBuilder.setError(
            TwoPhaseCommitTransactionResponse.Error.newBuilder()
                .setErrorCode(ErrorCode.UNKNOWN_TRANSACTION)
                .setMessage(e.getMessage())
                .build());
      } catch (Throwable t) {
        LOGGER.error("an internal error happened during the execution", t);
        if (t instanceof Error) {
          throw (Error) t;
        }
        responseBuilder.setError(
            TwoPhaseCommitTransactionResponse.Error.newBuilder()
                .setErrorCode(ErrorCode.OTHER)
                .setMessage(t.getMessage())
                .build());
      }
    }
  }
}
