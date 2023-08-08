package com.scalar.db.server;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.UnsatisfiedConditionException;
import com.scalar.db.rpc.AbortRequest;
import com.scalar.db.rpc.AbortResponse;
import com.scalar.db.rpc.DistributedTransactionGrpc;
import com.scalar.db.rpc.GetTransactionStateRequest;
import com.scalar.db.rpc.GetTransactionStateResponse;
import com.scalar.db.rpc.RollbackRequest;
import com.scalar.db.rpc.RollbackResponse;
import com.scalar.db.rpc.TransactionRequest;
import com.scalar.db.rpc.TransactionRequest.CommitRequest;
import com.scalar.db.rpc.TransactionRequest.GetRequest;
import com.scalar.db.rpc.TransactionRequest.MutateRequest;
import com.scalar.db.rpc.TransactionRequest.RequestCase;
import com.scalar.db.rpc.TransactionRequest.ScanRequest;
import com.scalar.db.rpc.TransactionRequest.StartRequest;
import com.scalar.db.rpc.TransactionResponse;
import com.scalar.db.rpc.TransactionResponse.Error.ErrorCode;
import com.scalar.db.rpc.TransactionResponse.GetResponse;
import com.scalar.db.rpc.TransactionResponse.ScanResponse;
import com.scalar.db.rpc.TransactionResponse.StartResponse;
import com.scalar.db.util.ProtoUtils;
import com.scalar.db.util.ThrowableRunnable;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class DistributedTransactionService
    extends DistributedTransactionGrpc.DistributedTransactionImplBase {
  private static final Logger logger = LoggerFactory.getLogger(DistributedTransactionService.class);
  private static final String SERVICE_NAME = "distributed_transaction";

  private final DistributedTransactionManager manager;
  private final TableMetadataManager tableMetadataManager;
  private final GateKeeper gateKeeper;
  private final Metrics metrics;

  public DistributedTransactionService(
      DistributedTransactionManager manager,
      TableMetadataManager tableMetadataManager,
      GateKeeper gateKeeper,
      Metrics metrics) {
    this.manager = manager;
    this.tableMetadataManager = tableMetadataManager;
    this.gateKeeper = gateKeeper;
    this.metrics = metrics;
  }

  @Override
  public StreamObserver<TransactionRequest> transaction(
      StreamObserver<TransactionResponse> responseObserver) {
    return new TransactionStreamObserver(
        manager,
        tableMetadataManager,
        responseObserver,
        metrics,
        this::preProcess,
        this::postProcess);
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
  public void rollback(RollbackRequest request, StreamObserver<RollbackResponse> responseObserver) {
    execute(
        () -> {
          TransactionState state = manager.rollback(request.getTransactionId());
          responseObserver.onNext(
              RollbackResponse.newBuilder().setState(ProtoUtils.toTransactionState(state)).build());
          responseObserver.onCompleted();
        },
        responseObserver,
        "rollback");
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
      logger.error("An internal error happened during the execution", t);
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

  @VisibleForTesting
  static class TransactionStreamObserver implements StreamObserver<TransactionRequest> {

    private final DistributedTransactionManager manager;
    private final TableMetadataManager tableMetadataManager;
    private final StreamObserver<TransactionResponse> responseObserver;
    private final Metrics metrics;
    private final Function<StreamObserver<?>, Boolean> preProcessor;
    private final Runnable postProcessor;
    private final AtomicBoolean preProcessed = new AtomicBoolean();
    private final AtomicBoolean postProcessed = new AtomicBoolean();

    private DistributedTransaction transaction;

    public TransactionStreamObserver(
        DistributedTransactionManager manager,
        TableMetadataManager tableMetadataManager,
        StreamObserver<TransactionResponse> responseObserver,
        Metrics metrics,
        Function<StreamObserver<?>, Boolean> preProcessor,
        Runnable postProcessor) {
      this.manager = manager;
      this.tableMetadataManager = tableMetadataManager;
      this.responseObserver = responseObserver;
      this.metrics = metrics;
      this.preProcessor = preProcessor;
      this.postProcessor = postProcessor;
    }

    @Override
    public void onNext(TransactionRequest request) {
      if (preProcessed.compareAndSet(false, true)) {
        if (!preProcessor.apply(responseObserver)) {
          return;
        }
      }

      if (request.getRequestCase() == RequestCase.BEGIN_REQUEST) {
        beginTransaction(request);
      } else if (request.getRequestCase() == RequestCase.START_REQUEST) {
        startTransaction(request);
      } else {
        executeTransaction(request);
      }
    }

    private void beginTransaction(TransactionRequest transactionRequest) {
      if (transactionBegun()) {
        respondInvalidArgumentError("transaction is already begun");
        return;
      }

      try {
        metrics.measure(
            SERVICE_NAME,
            "transaction.begin",
            () -> {
              TransactionRequest.BeginRequest request = transactionRequest.getBeginRequest();
              if (!request.hasTransactionId()) {
                transaction = manager.begin();
              } else {
                transaction = manager.begin(request.getTransactionId());
              }
            });
        responseObserver.onNext(
            TransactionResponse.newBuilder()
                .setBeginResponse(
                    TransactionResponse.BeginResponse.newBuilder()
                        .setTransactionId(transaction.getId())
                        .build())
                .build());
      } catch (IllegalArgumentException e) {
        respondInvalidArgumentError(e.getMessage());
      } catch (Throwable t) {
        logger.error("An internal error happened when beginning a transaction", t);
        respondInternalError(t.getMessage());
        if (t instanceof Error) {
          throw (Error) t;
        }
      }
    }

    private void startTransaction(TransactionRequest transactionRequest) {
      if (transactionBegun()) {
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
            TransactionResponse.newBuilder()
                .setStartResponse(
                    StartResponse.newBuilder().setTransactionId(transaction.getId()).build())
                .build());
      } catch (IllegalArgumentException e) {
        respondInvalidArgumentError(e.getMessage());
      } catch (Throwable t) {
        logger.error("An internal error happened when starting a transaction", t);
        respondInternalError(t.getMessage());
        if (t instanceof Error) {
          throw (Error) t;
        }
      }
    }

    private void executeTransaction(TransactionRequest request) {
      if (!transactionBegun()) {
        respondInvalidArgumentError("transaction is not started");
        return;
      }

      TransactionResponse.Builder responseBuilder = TransactionResponse.newBuilder();

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
        case COMMIT_REQUEST:
          commit(request.getCommitRequest(), responseBuilder);
          completed = true;
          break;
        case ROLLBACK_REQUEST:
          rollback(request.getRollbackRequest(), responseBuilder);
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
        postProcess();
      }
    }

    private void postProcess() {
      // postProcessor is executed only once after preProcessor is executed
      if (preProcessed.get() && postProcessed.compareAndSet(false, true)) {
        postProcessor.run();
      }
    }

    private boolean transactionBegun() {
      return transaction != null;
    }

    @Override
    public void onError(Throwable t) {
      logger.error("An error was received", t);
      cleanUp();
    }

    @Override
    public void onCompleted() {}

    private void get(GetRequest request, TransactionResponse.Builder responseBuilder) {
      execute(
          () -> {
            TableMetadata metadata =
                tableMetadataManager.getTableMetadata(
                    request.getGet().getNamespace(), request.getGet().getTable());
            if (metadata == null) {
              throw new IllegalArgumentException("The specified table is not found");
            }

            Get get = ProtoUtils.toGet(request.getGet(), metadata);
            Optional<Result> result = transaction.get(get);

            GetResponse.Builder builder = GetResponse.newBuilder();

            // For backward compatibility
            if (ProtoUtils.isRequestFromOldClient(request.getGet())) {
              result.ifPresent(r -> builder.setResult(ProtoUtils.toResultWithValue(r)));
            } else {
              result.ifPresent(r -> builder.setResult(ProtoUtils.toResult(r)));
            }

            responseBuilder.setGetResponse(builder);
          },
          responseBuilder,
          "transaction.get");
    }

    private void scan(ScanRequest request, TransactionResponse.Builder responseBuilder) {
      execute(
          () -> {
            TableMetadata metadata =
                tableMetadataManager.getTableMetadata(
                    request.getScan().getNamespace(), request.getScan().getTable());
            if (metadata == null) {
              throw new IllegalArgumentException("The specified table is not found");
            }

            Scan scan = ProtoUtils.toScan(request.getScan(), metadata);
            List<Result> results = transaction.scan(scan);
            ScanResponse.Builder builder = ScanResponse.newBuilder();

            // For backward compatibility
            if (ProtoUtils.isRequestFromOldClient(request.getScan())) {
              results.forEach(r -> builder.addResults(ProtoUtils.toResultWithValue(r)));
            } else {
              results.forEach(r -> builder.addResults(ProtoUtils.toResult(r)));
            }

            responseBuilder.setScanResponse(builder);
          },
          responseBuilder,
          "transaction.scan");
    }

    private void mutate(MutateRequest request, TransactionResponse.Builder responseBuilder) {
      execute(
          () -> {
            List<Mutation> mutations;
            if (request.getMutationsCount() > 0) {
              TableMetadata metadata =
                  tableMetadataManager.getTableMetadata(
                      request.getMutationsList().get(0).getNamespace(),
                      request.getMutationsList().get(0).getTable());
              if (metadata == null) {
                throw new IllegalArgumentException("The specified table is not found");
              }

              mutations =
                  request.getMutationsList().stream()
                      .map(m -> ProtoUtils.toMutation(m, metadata))
                      .collect(Collectors.toList());
            } else {
              mutations = Collections.emptyList();
            }
            transaction.mutate(mutations);
          },
          responseBuilder,
          "transaction.mutate");
    }

    private void commit(CommitRequest unused, TransactionResponse.Builder responseBuilder) {
      execute(() -> transaction.commit(), responseBuilder, "transaction.commit");
    }

    private void rollback(
        TransactionRequest.RollbackRequest unused, TransactionResponse.Builder responseBuilder) {
      execute(() -> transaction.rollback(), responseBuilder, "transaction.rollback");
    }

    private void abort(
        TransactionRequest.AbortRequest unused, TransactionResponse.Builder responseBuilder) {
      execute(() -> transaction.abort(), responseBuilder, "transaction.abort");
    }

    private void cleanUp() {
      if (transaction != null) {
        try {
          transaction.rollback();
        } catch (RollbackException e) {
          logger.warn("Rollback failed", e);
        }
      }

      postProcess();
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
        TransactionResponse.Builder responseBuilder,
        String method) {
      try {
        metrics.measure(SERVICE_NAME, method, runnable);
      } catch (IllegalArgumentException | IllegalStateException e) {
        responseBuilder.setError(
            TransactionResponse.Error.newBuilder()
                .setErrorCode(ErrorCode.INVALID_ARGUMENT)
                .setMessage(e.getMessage())
                .build());
      } catch (CrudConflictException | CommitConflictException e) {
        responseBuilder.setError(
            TransactionResponse.Error.newBuilder()
                .setErrorCode(ErrorCode.TRANSACTION_CONFLICT)
                .setMessage(e.getMessage())
                .build());
      } catch (UnsatisfiedConditionException e) {
        responseBuilder.setError(
            TransactionResponse.Error.newBuilder()
                .setErrorCode(ErrorCode.UNSATISFIED_CONDITION)
                .setMessage(e.getMessage())
                .build());
      } catch (UnknownTransactionStatusException e) {
        logger.error(
            "The transaction status is unknown. transaction ID: {}",
            e.getTransactionId().orElse("null"),
            e);
        responseBuilder.setError(
            TransactionResponse.Error.newBuilder()
                .setErrorCode(ErrorCode.UNKNOWN_TRANSACTION_STATUS)
                .setMessage(e.getMessage())
                .build());
      } catch (Throwable t) {
        logger.error("An internal error happened during the execution", t);
        if (t instanceof Error) {
          throw (Error) t;
        }
        responseBuilder.setError(
            TransactionResponse.Error.newBuilder()
                .setErrorCode(ErrorCode.OTHER)
                .setMessage(t.getMessage())
                .build());
      }
    }
  }
}
