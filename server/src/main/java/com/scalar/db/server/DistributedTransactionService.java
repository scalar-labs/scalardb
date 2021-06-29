package com.scalar.db.server;

import com.google.inject.Inject;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.rpc.DistributedTransactionGrpc;
import com.scalar.db.rpc.GetTransactionStateRequest;
import com.scalar.db.rpc.GetTransactionStateResponse;
import com.scalar.db.rpc.TransactionRequest;
import com.scalar.db.rpc.TransactionRequest.Command;
import com.scalar.db.rpc.TransactionResponse;
import com.scalar.db.util.ProtoUtil;
import com.scalar.db.util.ThrowableRunnable;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
    return new StreamObserver<TransactionRequest>() {

      private DistributedTransaction transaction;

      @Override
      public void onNext(TransactionRequest request) {
        if (request.getCommand() == Command.START) {
          transaction = start(request, responseObserver);
          return;
        }

        if (!checkTransaction(responseObserver)) {
          return;
        }

        switch (request.getCommand()) {
          case GET:
            get(transaction, request, responseObserver);
            break;
          case SCAN:
            scan(transaction, request, responseObserver);
            break;
          case MUTATION:
            mutate(transaction, request, responseObserver);
            break;
          case COMMIT:
            commit(transaction, request, responseObserver);
            break;
          case ABORT:
            abort(transaction, request, responseObserver);
            break;
          default:
            responseObserver.onError(
                Status.INVALID_ARGUMENT
                    .withDescription("invalid command specified: " + request.getCommand())
                    .asRuntimeException());
            break;
        }
      }

      private boolean checkTransaction(StreamObserver<TransactionResponse> responseObserver) {
        if (transaction == null) {
          responseObserver.onError(
              Status.INVALID_ARGUMENT
                  .withDescription("transaction is not started")
                  .asRuntimeException());
          return false;
        }
        return true;
      }

      @Override
      public void onError(Throwable t) {
        if (transaction != null) {
          try {
            transaction.abort();
          } catch (AbortException ignored) {
          }
        }
      }

      @Override
      public void onCompleted() {}
    };
  }

  private DistributedTransaction start(
      TransactionRequest request, StreamObserver<TransactionResponse> responseObserver) {
    try {
      DistributedTransaction ret;
      if (!request.hasTransactionId()) {
        ret = manager.start();
      } else {
        ret = manager.start(request.getTransactionId());
      }
      responseObserver.onNext(
          TransactionResponse.newBuilder().setTransactionId(ret.getId()).build());
      return ret;
    } catch (Throwable t) {
      LOGGER.error("an internal error happened during the execution", t);
      responseObserver.onError(
          Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
      if (t instanceof Error) {
        throw (Error) t;
      }
    }
    return null;
  }

  private void get(
      DistributedTransaction transaction,
      TransactionRequest request,
      StreamObserver<TransactionResponse> responseObserver) {
    execute(
        () -> {
          Get get = ProtoUtil.toGet(request.getGet());
          Optional<Result> result = transaction.get(get);
          TransactionResponse.Builder builder = TransactionResponse.newBuilder();
          result.ifPresent(
              r ->
                  builder.setResults(
                      TransactionResponse.Results.newBuilder()
                          .addResult(ProtoUtil.toResult(r))
                          .build()));
          responseObserver.onNext(builder.build());
        },
        responseObserver);
  }

  private void scan(
      DistributedTransaction transaction,
      TransactionRequest request,
      StreamObserver<TransactionResponse> responseObserver) {
    execute(
        () -> {
          Scan scan = ProtoUtil.toScan(request.getScan());
          List<Result> results = transaction.scan(scan);
          TransactionResponse.Results.Builder resultsBuilder =
              TransactionResponse.Results.newBuilder();
          results.forEach(r -> resultsBuilder.addResult(ProtoUtil.toResult(r)));
          responseObserver.onNext(
              TransactionResponse.newBuilder().setResults(resultsBuilder).build());
        },
        responseObserver);
  }

  private void mutate(
      DistributedTransaction transaction,
      TransactionRequest request,
      StreamObserver<TransactionResponse> responseObserver) {
    execute(
        () -> {
          if (!request.hasMutations()) {
            responseObserver.onNext(TransactionResponse.getDefaultInstance());
            return;
          }
          List<Mutation> mutations = new ArrayList<>(request.getMutations().getMutationCount());
          for (com.scalar.db.rpc.Mutation mutation : request.getMutations().getMutationList()) {
            mutations.add(ProtoUtil.toMutation(mutation));
          }
          transaction.mutate(mutations);
          responseObserver.onNext(TransactionResponse.getDefaultInstance());
        },
        responseObserver);
  }

  private void commit(
      DistributedTransaction transaction,
      TransactionRequest unused,
      StreamObserver<TransactionResponse> responseObserver) {
    execute(
        () -> {
          transaction.commit();
          responseObserver.onNext(TransactionResponse.getDefaultInstance());
          responseObserver.onCompleted();
        },
        responseObserver,
        true);
  }

  private void abort(
      DistributedTransaction transaction,
      TransactionRequest unused,
      StreamObserver<TransactionResponse> responseObserver) {
    execute(
        () -> {
          transaction.abort();
          responseObserver.onNext(TransactionResponse.getDefaultInstance());
          responseObserver.onCompleted();
        },
        responseObserver,
        true);
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

  private static void execute(
      ThrowableRunnable<Throwable> runnable, StreamObserver<TransactionResponse> responseObserver) {
    execute(runnable, responseObserver, false);
  }

  private static void execute(
      ThrowableRunnable<Throwable> runnable,
      StreamObserver<TransactionResponse> responseObserver,
      boolean onComplete) {
    try {
      runnable.run();
    } catch (IllegalArgumentException | IllegalStateException e) {
      LOGGER.error("an invalid argument error happened during the execution", e);
      responseObserver.onNext(
          TransactionResponse.newBuilder()
              .setError(
                  TransactionResponse.Error.newBuilder()
                      .setErrorCode(TransactionResponse.Error.ErrorCode.INVALID_ARGUMENT)
                      .setMessage(e.getMessage())
                      .build())
              .build());
      if (onComplete) {
        responseObserver.onCompleted();
      }
    } catch (CrudConflictException | CommitConflictException e) {
      responseObserver.onNext(
          TransactionResponse.newBuilder()
              .setError(
                  TransactionResponse.Error.newBuilder()
                      .setErrorCode(TransactionResponse.Error.ErrorCode.CONFLICT)
                      .setMessage(e.getMessage())
                      .build())
              .build());
      if (onComplete) {
        responseObserver.onCompleted();
      }
    } catch (UnknownTransactionStatusException e) {
      LOGGER.error("an unknown transaction error happened during the execution", e);
      responseObserver.onNext(
          TransactionResponse.newBuilder()
              .setError(
                  TransactionResponse.Error.newBuilder()
                      .setErrorCode(TransactionResponse.Error.ErrorCode.UNKNOWN_TRANSACTION)
                      .setMessage(e.getMessage())
                      .build())
              .build());
    } catch (Throwable t) {
      LOGGER.error("an internal error happened during the execution", t);
      responseObserver.onNext(
          TransactionResponse.newBuilder()
              .setError(
                  TransactionResponse.Error.newBuilder()
                      .setErrorCode(TransactionResponse.Error.ErrorCode.OTHER)
                      .setMessage(t.getMessage())
                      .build())
              .build());
      if (onComplete) {
        responseObserver.onCompleted();
      }
      if (t instanceof Error) {
        throw (Error) t;
      }
    }
  }
}
