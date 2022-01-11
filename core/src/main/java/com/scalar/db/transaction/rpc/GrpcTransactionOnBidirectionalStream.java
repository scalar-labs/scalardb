package com.scalar.db.transaction.rpc;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.rpc.DistributedTransactionGrpc.DistributedTransactionStub;
import com.scalar.db.rpc.TransactionRequest;
import com.scalar.db.rpc.TransactionRequest.AbortRequest;
import com.scalar.db.rpc.TransactionRequest.CommitRequest;
import com.scalar.db.rpc.TransactionRequest.GetRequest;
import com.scalar.db.rpc.TransactionRequest.MutateRequest;
import com.scalar.db.rpc.TransactionRequest.ScanRequest;
import com.scalar.db.rpc.TransactionRequest.StartRequest;
import com.scalar.db.rpc.TransactionResponse;
import com.scalar.db.rpc.TransactionResponse.GetResponse;
import com.scalar.db.storage.rpc.GrpcConfig;
import com.scalar.db.util.ProtoUtils;
import com.scalar.db.util.ScalarDbUtils;
import com.scalar.db.util.TableMetadataManager;
import com.scalar.db.util.retry.ServiceTemporaryUnavailableException;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class GrpcTransactionOnBidirectionalStream
    implements ClientResponseObserver<TransactionRequest, TransactionResponse> {

  private final GrpcConfig config;
  private final TableMetadataManager metadataManager;
  private final BlockingQueue<ResponseOrError> queue = new LinkedBlockingQueue<>();
  private final AtomicBoolean finished = new AtomicBoolean();

  private ClientCallStreamObserver<TransactionRequest> requestStream;

  public GrpcTransactionOnBidirectionalStream(
      GrpcConfig config, DistributedTransactionStub stub, TableMetadataManager metadataManager) {
    this.config = config;
    this.metadataManager = metadataManager;
    stub.transaction(this);
  }

  @Override
  public void beforeStart(ClientCallStreamObserver<TransactionRequest> requestStream) {
    this.requestStream = requestStream;
  }

  @Override
  public void onNext(TransactionResponse response) {
    Uninterruptibles.putUninterruptibly(queue, new ResponseOrError(response));
  }

  @Override
  public void onError(Throwable t) {
    Uninterruptibles.putUninterruptibly(queue, new ResponseOrError(t));
  }

  @Override
  public void onCompleted() {
    requestStream.onCompleted();
  }

  private ResponseOrError sendRequest(TransactionRequest request) {
    requestStream.onNext(request);

    ResponseOrError responseOrError =
        ScalarDbUtils.pollUninterruptibly(
            queue, config.getDeadlineDurationMillis(), TimeUnit.MILLISECONDS);
    if (responseOrError == null) {
      requestStream.cancel("deadline exceeded", null);

      // Should receive a CANCELED error
      return Uninterruptibles.takeUninterruptibly(queue);
    }
    return responseOrError;
  }

  private void throwIfTransactionFinished() {
    if (finished.get()) {
      throw new IllegalStateException("the transaction is finished");
    }
  }

  public String startTransaction(@Nullable String transactionId) throws TransactionException {
    throwIfTransactionFinished();

    StartRequest request;
    if (transactionId == null) {
      request = StartRequest.getDefaultInstance();
    } else {
      request = StartRequest.newBuilder().setTransactionId(transactionId).build();
    }
    ResponseOrError responseOrError =
        sendRequest(TransactionRequest.newBuilder().setStartRequest(request).build());
    throwIfErrorForStart(responseOrError);
    return responseOrError.getResponse().getStartResponse().getTransactionId();
  }

  private void throwIfErrorForStart(ResponseOrError responseOrError) throws TransactionException {
    if (responseOrError.isError()) {
      finished.set(true);
      Throwable error = responseOrError.getError();
      if (error instanceof StatusRuntimeException) {
        StatusRuntimeException e = (StatusRuntimeException) error;
        if (e.getStatus().getCode() == Code.INVALID_ARGUMENT) {
          throw new IllegalArgumentException(e.getMessage(), e);
        }
        if (e.getStatus().getCode() == Code.UNAVAILABLE) {
          throw new ServiceTemporaryUnavailableException(e.getMessage(), e);
        }
      }
      if (error instanceof Error) {
        throw (Error) error;
      }
      throw new TransactionException("failed to start", error);
    }
  }

  public Optional<Result> get(Get get) throws CrudException {
    throwIfTransactionFinished();

    ResponseOrError responseOrError =
        sendRequest(
            TransactionRequest.newBuilder()
                .setGetRequest(GetRequest.newBuilder().setGet(ProtoUtils.toGet(get)))
                .build());
    throwIfErrorForCrud(responseOrError);

    GetResponse getResponse = responseOrError.getResponse().getGetResponse();
    if (getResponse.hasResult()) {
      TableMetadata tableMetadata = getTableMetadata(get);
      return Optional.of(ProtoUtils.toResult(getResponse.getResult(), tableMetadata));
    }

    return Optional.empty();
  }

  public List<Result> scan(Scan scan) throws CrudException {
    throwIfTransactionFinished();

    ResponseOrError responseOrError =
        sendRequest(
            TransactionRequest.newBuilder()
                .setScanRequest(ScanRequest.newBuilder().setScan(ProtoUtils.toScan(scan)))
                .build());
    throwIfErrorForCrud(responseOrError);

    TableMetadata tableMetadata = getTableMetadata(scan);
    return responseOrError.getResponse().getScanResponse().getResultList().stream()
        .map(r -> ProtoUtils.toResult(r, tableMetadata))
        .collect(Collectors.toList());
  }

  private TableMetadata getTableMetadata(Operation operation) throws CrudException {
    try {
      return metadataManager.getTableMetadata(operation);
    } catch (ExecutionException e) {
      throw new CrudException("getting a metadata failed", e);
    }
  }

  public void mutate(Mutation mutation) throws CrudException {
    throwIfTransactionFinished();

    ResponseOrError responseOrError =
        sendRequest(
            TransactionRequest.newBuilder()
                .setMutateRequest(
                    MutateRequest.newBuilder().addMutation(ProtoUtils.toMutation(mutation)))
                .build());
    throwIfErrorForCrud(responseOrError);
  }

  public void mutate(List<? extends Mutation> mutations) throws CrudException {
    throwIfTransactionFinished();

    MutateRequest.Builder builder = MutateRequest.newBuilder();
    mutations.forEach(m -> builder.addMutation(ProtoUtils.toMutation(m)));
    ResponseOrError responseOrError =
        sendRequest(TransactionRequest.newBuilder().setMutateRequest(builder).build());
    throwIfErrorForCrud(responseOrError);
  }

  private void throwIfErrorForCrud(ResponseOrError responseOrError) throws CrudException {
    if (responseOrError.isError()) {
      finished.set(true);
      Throwable error = responseOrError.getError();
      if (error instanceof Error) {
        throw (Error) error;
      }
      throw new CrudException("failed to execute crud", error);
    }

    TransactionResponse response = responseOrError.getResponse();
    if (response.hasError()) {
      TransactionResponse.Error error = response.getError();
      switch (error.getErrorCode()) {
        case INVALID_ARGUMENT:
          throw new IllegalArgumentException(error.getMessage());
        case CONFLICT:
          throw new CrudConflictException(error.getMessage());
        default:
          throw new CrudException(error.getMessage());
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
      Throwable error = responseOrError.getError();
      if (error instanceof Error) {
        throw (Error) error;
      }
      throw new CommitException("failed to commit", error);
    }

    TransactionResponse response = responseOrError.getResponse();
    if (response.hasError()) {
      TransactionResponse.Error error = response.getError();
      switch (error.getErrorCode()) {
        case CONFLICT:
          throw new CommitConflictException(error.getMessage());
        case UNKNOWN_TRANSACTION:
          throw new UnknownTransactionStatusException(error.getMessage());
        default:
          throw new CommitException(error.getMessage());
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
      Throwable error = responseOrError.getError();
      if (error instanceof Error) {
        throw (Error) error;
      }
      throw new AbortException("failed to abort", error);
    }

    TransactionResponse response = responseOrError.getResponse();
    if (response.hasError()) {
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
