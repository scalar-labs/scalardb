package com.scalar.db.transaction.rpc;

import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.rpc.AbortRequest;
import com.scalar.db.rpc.CommitRequest;
import com.scalar.db.rpc.DistributedTransactionGrpc.DistributedTransactionStub;
import com.scalar.db.rpc.StartTransactionRequest;
import com.scalar.db.rpc.TransactionRequest;
import com.scalar.db.rpc.TransactionResponse;
import com.scalar.db.rpc.TransactionResponse.Error.ErrorCode;
import com.scalar.db.rpc.TransactionalGetRequest;
import com.scalar.db.rpc.TransactionalGetResponse;
import com.scalar.db.rpc.TransactionalMutateRequest;
import com.scalar.db.rpc.TransactionalScanRequest;
import com.scalar.db.storage.rpc.GrpcTableMetadataManager;
import com.scalar.db.util.ProtoUtil;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class GrpcTransactionBidirectionalStream implements StreamObserver<TransactionResponse> {

  private final StreamObserver<TransactionRequest> requestObserver;
  private final GrpcTableMetadataManager metadataManager;
  private final BlockingQueue<ResponseOrError> queue = new LinkedBlockingQueue<>();
  private final AtomicBoolean finished = new AtomicBoolean();

  public GrpcTransactionBidirectionalStream(
      DistributedTransactionStub stub, GrpcTableMetadataManager metadataManager) {
    requestObserver = stub.transaction(this);
    this.metadataManager = metadataManager;
  }

  @Override
  public void onNext(TransactionResponse response) {
    try {
      queue.put(new ResponseOrError(response));
    } catch (InterruptedException ignored) {
      // InterruptedException should not be thrown
    }
  }

  @Override
  public void onError(Throwable t) {
    try {
      queue.put(new ResponseOrError(t));
    } catch (InterruptedException ignored) {
      // InterruptedException should not be thrown
    }
  }

  @Override
  public void onCompleted() {}

  private ResponseOrError sendRequest(TransactionRequest request) {
    requestObserver.onNext(request);
    try {
      return queue.take();
    } catch (InterruptedException ignored) {
      // InterruptedException should not be thrown
      return null;
    }
  }

  private void throwIfTransactionFinished() {
    if (finished.get()) {
      throw new IllegalStateException("the transaction is finished");
    }
  }

  public String startTransaction() throws TransactionException {
    return startTransaction(null);
  }

  public String startTransaction(@Nullable String transactionId) throws TransactionException {
    throwIfTransactionFinished();

    StartTransactionRequest request;
    if (transactionId == null) {
      request = StartTransactionRequest.getDefaultInstance();
    } else {
      request = StartTransactionRequest.newBuilder().setTransactionId(transactionId).build();
    }
    ResponseOrError responseOrError =
        sendRequest(TransactionRequest.newBuilder().setStartTransactionRequest(request).build());
    throwIfErrorForStart(responseOrError);
    return responseOrError.getResponse().getStartTransactionResponse().getTransactionId();
  }

  private void throwIfErrorForStart(ResponseOrError responseOrError) throws TransactionException {
    if (responseOrError.isError()) {
      finished.set(true);
      Throwable error = responseOrError.getError();
      if (error instanceof StatusRuntimeException) {
        StatusRuntimeException e = (StatusRuntimeException) error;
        if (e.getStatus() == Status.INVALID_ARGUMENT) {
          throw new IllegalArgumentException(e.getMessage());
        }
      }
      if (error instanceof Error) {
        throw (Error) error;
      }
      throw new TransactionException(error.getMessage());
    }

    TransactionResponse response = responseOrError.getResponse();
    if (response.hasError()) {
      if (response.getError().getErrorCode() == ErrorCode.INVALID_ARGUMENT) {
        throw new IllegalArgumentException(response.getError().getMessage());
      }
      throw new TransactionException(response.getError().getMessage());
    }
  }

  public Optional<Result> get(Get get) throws CrudException {
    throwIfTransactionFinished();

    ResponseOrError responseOrError =
        sendRequest(
            TransactionRequest.newBuilder()
                .setTransactionalGetRequest(
                    TransactionalGetRequest.newBuilder().setGet(ProtoUtil.toGet(get)))
                .build());
    throwIfErrorForCrud(responseOrError);

    TransactionalGetResponse transactionalGetResponse =
        responseOrError.getResponse().getTransactionalGetResponse();
    if (transactionalGetResponse.hasResult()) {
      TableMetadata tableMetadata = metadataManager.getTableMetadata(get);
      return Optional.of(ProtoUtil.toResult(transactionalGetResponse.getResult(), tableMetadata));
    }

    return Optional.empty();
  }

  public List<Result> scan(Scan scan) throws CrudException {
    throwIfTransactionFinished();

    ResponseOrError responseOrError =
        sendRequest(
            TransactionRequest.newBuilder()
                .setTransactionalScanRequest(
                    TransactionalScanRequest.newBuilder().setScan(ProtoUtil.toScan(scan)))
                .build());
    throwIfErrorForCrud(responseOrError);

    TableMetadata tableMetadata = metadataManager.getTableMetadata(scan);
    return responseOrError.getResponse().getTransactionalScanResponse().getResultList().stream()
        .map(r -> ProtoUtil.toResult(r, tableMetadata))
        .collect(Collectors.toList());
  }

  public void mutate(Mutation mutation) throws CrudException {
    throwIfTransactionFinished();

    ResponseOrError responseOrError =
        sendRequest(
            TransactionRequest.newBuilder()
                .setTransactionalMutateRequest(
                    TransactionalMutateRequest.newBuilder()
                        .addMutation(ProtoUtil.toMutation(mutation)))
                .build());
    throwIfErrorForCrud(responseOrError);
  }

  public void mutate(List<? extends Mutation> mutations) throws CrudException {
    throwIfTransactionFinished();

    TransactionalMutateRequest.Builder builder = TransactionalMutateRequest.newBuilder();
    mutations.forEach(m -> builder.addMutation(ProtoUtil.toMutation(m)));
    ResponseOrError responseOrError =
        sendRequest(TransactionRequest.newBuilder().setTransactionalMutateRequest(builder).build());
    throwIfErrorForCrud(responseOrError);
  }

  private void throwIfErrorForCrud(ResponseOrError responseOrError) throws CrudException {
    if (responseOrError.isError()) {
      finished.set(true);
      Throwable error = responseOrError.getError();
      if (error instanceof Error) {
        throw (Error) error;
      }
      throw new CrudException(error.getMessage());
    }

    TransactionResponse response = responseOrError.getResponse();
    if (response.hasError()) {
      switch (response.getError().getErrorCode()) {
        case INVALID_ARGUMENT:
          throw new IllegalArgumentException(response.getError().getMessage());
        case CONFLICT:
          throw new CrudConflictException(response.getError().getMessage());
        default:
          throw new CrudException(response.getError().getMessage());
      }
    }
  }

  public void commit() throws CommitException, UnknownTransactionStatusException {
    throwIfTransactionFinished();

    ResponseOrError responseOrError =
        sendRequest(
            TransactionRequest.newBuilder()
                .setCommitRequest(CommitRequest.getDefaultInstance())
                .build());
    finished.set(true);
    throwIfErrorForCommit(responseOrError);
  }

  private void throwIfErrorForCommit(ResponseOrError responseOrError)
      throws CommitException, UnknownTransactionStatusException {
    if (responseOrError.isError()) {
      finished.set(true);
      Throwable error = responseOrError.getError();
      if (error instanceof Error) {
        throw (Error) error;
      }
      throw new CommitException(error.getMessage());
    }

    if (responseOrError.getResponse().hasError()) {
      switch (responseOrError.getResponse().getError().getErrorCode()) {
        case INVALID_ARGUMENT:
          throw new IllegalArgumentException(responseOrError.getResponse().getError().getMessage());
        case CONFLICT:
          throw new CommitConflictException(responseOrError.getResponse().getError().getMessage());
        case UNKNOWN_TRANSACTION:
          throw new UnknownTransactionStatusException(
              responseOrError.getResponse().getError().getMessage());
        default:
          throw new CommitException(responseOrError.getResponse().getError().getMessage());
      }
    }
  }

  public void abort() throws AbortException {
    if (finished.get()) {
      return;
    }

    ResponseOrError responseOrError =
        sendRequest(
            TransactionRequest.newBuilder()
                .setAbortRequest(AbortRequest.getDefaultInstance())
                .build());
    finished.set(true);
    throwIfErrorForAbort(responseOrError);
  }

  private void throwIfErrorForAbort(ResponseOrError responseOrError) throws AbortException {
    if (responseOrError.isError()) {
      finished.set(true);
      Throwable error = responseOrError.getError();
      if (error instanceof Error) {
        throw (Error) error;
      }
      throw new AbortException(error.getMessage());
    }

    TransactionResponse response = responseOrError.getResponse();
    if (response.hasError()) {
      if (response.getError().getErrorCode() == ErrorCode.INVALID_ARGUMENT) {
        throw new IllegalArgumentException(response.getError().getMessage());
      }
      throw new AbortException(response.getError().getMessage());
    }
  }

  private static class ResponseOrError {
    private final TransactionResponse response;
    private final Throwable error;

    public ResponseOrError(TransactionResponse response) {
      this.response = response;
      this.error = null;
    }

    public ResponseOrError(Throwable error) {
      this.response = null;
      this.error = error;
    }

    private boolean isError() {
      return error != null;
    }

    public TransactionResponse getResponse() {
      return response;
    }

    public Throwable getError() {
      return error;
    }
  }
}
