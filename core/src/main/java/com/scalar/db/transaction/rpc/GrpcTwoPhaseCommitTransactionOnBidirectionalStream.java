package com.scalar.db.transaction.rpc;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.UnsatisfiedConditionException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.exception.transaction.ValidationException;
import com.scalar.db.rpc.TwoPhaseCommitTransactionGrpc;
import com.scalar.db.rpc.TwoPhaseCommitTransactionRequest;
import com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.CommitRequest;
import com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.GetRequest;
import com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.JoinRequest;
import com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.MutateRequest;
import com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.PrepareRequest;
import com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.RollbackRequest;
import com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.ScanRequest;
import com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.StartRequest;
import com.scalar.db.rpc.TwoPhaseCommitTransactionRequest.ValidateRequest;
import com.scalar.db.rpc.TwoPhaseCommitTransactionResponse;
import com.scalar.db.rpc.TwoPhaseCommitTransactionResponse.Error.ErrorCode;
import com.scalar.db.rpc.TwoPhaseCommitTransactionResponse.GetResponse;
import com.scalar.db.storage.rpc.GrpcConfig;
import com.scalar.db.util.ProtoUtils;
import com.scalar.db.util.ScalarDbUtils;
import com.scalar.db.util.retry.ServiceTemporaryUnavailableException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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
public class GrpcTwoPhaseCommitTransactionOnBidirectionalStream
    implements ClientResponseObserver<
        TwoPhaseCommitTransactionRequest, TwoPhaseCommitTransactionResponse> {

  private final GrpcConfig config;
  private final TableMetadataManager metadataManager;
  private final BlockingQueue<ResponseOrError> queue = new LinkedBlockingQueue<>();
  private final AtomicBoolean finished = new AtomicBoolean();

  private ClientCallStreamObserver<TwoPhaseCommitTransactionRequest> requestStream;
  private String transactionId;

  public GrpcTwoPhaseCommitTransactionOnBidirectionalStream(
      GrpcConfig config,
      TwoPhaseCommitTransactionGrpc.TwoPhaseCommitTransactionStub stub,
      TableMetadataManager metadataManager) {
    this.config = config;
    this.metadataManager = metadataManager;
    stub.twoPhaseCommitTransaction(this);
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  @Override
  public void beforeStart(
      ClientCallStreamObserver<TwoPhaseCommitTransactionRequest> requestStream) {
    this.requestStream = requestStream;
  }

  @Override
  public void onNext(TwoPhaseCommitTransactionResponse response) {
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

  private ResponseOrError sendRequest(TwoPhaseCommitTransactionRequest request) {
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
      throw new IllegalStateException("The transaction is finished");
    }
  }

  public String beginTransaction(@Nullable String transactionId) throws TransactionException {
    throwIfTransactionFinished();

    TwoPhaseCommitTransactionRequest.BeginRequest request;
    if (transactionId == null) {
      request = TwoPhaseCommitTransactionRequest.BeginRequest.getDefaultInstance();
    } else {
      request =
          TwoPhaseCommitTransactionRequest.BeginRequest.newBuilder()
              .setTransactionId(transactionId)
              .build();
    }
    ResponseOrError responseOrError =
        sendRequest(TwoPhaseCommitTransactionRequest.newBuilder().setBeginRequest(request).build());
    throwIfErrorForBeginOrStartOrJoin(responseOrError, "begin");
    this.transactionId = responseOrError.getResponse().getBeginResponse().getTransactionId();
    return this.transactionId;
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
        sendRequest(TwoPhaseCommitTransactionRequest.newBuilder().setStartRequest(request).build());
    throwIfErrorForBeginOrStartOrJoin(responseOrError, "start");
    this.transactionId = responseOrError.getResponse().getStartResponse().getTransactionId();
    return this.transactionId;
  }

  public void joinTransaction(String transactionId) throws TransactionException {
    throwIfTransactionFinished();

    ResponseOrError responseOrError =
        sendRequest(
            TwoPhaseCommitTransactionRequest.newBuilder()
                .setJoinRequest(JoinRequest.newBuilder().setTransactionId(transactionId).build())
                .build());
    throwIfErrorForBeginOrStartOrJoin(responseOrError, "join");

    this.transactionId = transactionId;
  }

  private void throwIfErrorForBeginOrStartOrJoin(ResponseOrError responseOrError, String command)
      throws TransactionException {
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
      throw new TransactionException("Failed to " + command, error, null);
    }
  }

  public Optional<Result> get(Get get) throws CrudException {
    throwIfTransactionFinished();

    ResponseOrError responseOrError =
        sendRequest(
            TwoPhaseCommitTransactionRequest.newBuilder()
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
            TwoPhaseCommitTransactionRequest.newBuilder()
                .setScanRequest(ScanRequest.newBuilder().setScan(ProtoUtils.toScan(scan)))
                .build());
    throwIfErrorForCrud(responseOrError);

    TableMetadata tableMetadata = getTableMetadata(scan);
    return responseOrError.getResponse().getScanResponse().getResultsList().stream()
        .map(r -> ProtoUtils.toResult(r, tableMetadata))
        .collect(Collectors.toList());
  }

  private TableMetadata getTableMetadata(Operation operation) throws CrudException {
    try {
      return metadataManager.getTableMetadata(operation);
    } catch (ExecutionException e) {
      throw new CrudException("Getting a metadata failed", e, transactionId);
    }
  }

  public void mutate(Mutation mutation) throws CrudException {
    throwIfTransactionFinished();

    ResponseOrError responseOrError =
        sendRequest(
            TwoPhaseCommitTransactionRequest.newBuilder()
                .setMutateRequest(
                    MutateRequest.newBuilder().addMutations(ProtoUtils.toMutation(mutation)))
                .build());
    throwIfErrorForCrud(responseOrError);
  }

  public void mutate(List<? extends Mutation> mutations) throws CrudException {
    throwIfTransactionFinished();

    MutateRequest.Builder builder = MutateRequest.newBuilder();
    mutations.forEach(m -> builder.addMutations(ProtoUtils.toMutation(m)));
    ResponseOrError responseOrError =
        sendRequest(
            TwoPhaseCommitTransactionRequest.newBuilder().setMutateRequest(builder).build());
    throwIfErrorForCrud(responseOrError);
  }

  private void throwIfErrorForCrud(ResponseOrError responseOrError) throws CrudException {
    if (responseOrError.isError()) {
      finished.set(true);
      Throwable error = responseOrError.getError();
      if (error instanceof Error) {
        throw (Error) error;
      }
      throw new CrudException("Failed to execute crud", error, transactionId);
    }

    TwoPhaseCommitTransactionResponse response = responseOrError.getResponse();
    if (response.hasError()) {
      TwoPhaseCommitTransactionResponse.Error error = response.getError();
      switch (error.getErrorCode()) {
        case INVALID_ARGUMENT:
          throw new IllegalArgumentException(error.getMessage());
        case TRANSACTION_CONFLICT:
          throw new CrudConflictException(error.getMessage(), transactionId);
        case UNSATISFIED_CONDITION:
          throw new UnsatisfiedConditionException(error.getMessage(), transactionId);
        default:
          throw new CrudException(error.getMessage(), transactionId);
      }
    }
  }

  public void prepare() throws PreparationException {
    throwIfTransactionFinished();

    ResponseOrError responseOrError =
        sendRequest(
            TwoPhaseCommitTransactionRequest.newBuilder()
                .setPrepareRequest(PrepareRequest.getDefaultInstance())
                .build());
    throwIfErrorForPreparation(responseOrError);
  }

  private void throwIfErrorForPreparation(ResponseOrError responseOrError)
      throws PreparationException {
    if (responseOrError.isError()) {
      finished.set(true);
      Throwable error = responseOrError.getError();
      if (error instanceof Error) {
        throw (Error) error;
      }
      throw new PreparationException("Failed to prepare", error, transactionId);
    }

    TwoPhaseCommitTransactionResponse response = responseOrError.getResponse();
    if (response.hasError()) {
      TwoPhaseCommitTransactionResponse.Error error = response.getError();
      if (error.getErrorCode() == ErrorCode.TRANSACTION_CONFLICT) {
        throw new PreparationConflictException(error.getMessage(), transactionId);
      }
      throw new PreparationException(error.getMessage(), transactionId);
    }
  }

  public void validate() throws ValidationException {
    throwIfTransactionFinished();

    ResponseOrError responseOrError =
        sendRequest(
            TwoPhaseCommitTransactionRequest.newBuilder()
                .setValidateRequest(ValidateRequest.getDefaultInstance())
                .build());
    throwIfErrorForValidation(responseOrError);
  }

  private void throwIfErrorForValidation(ResponseOrError responseOrError)
      throws ValidationException {
    if (responseOrError.isError()) {
      finished.set(true);
      Throwable error = responseOrError.getError();
      if (error instanceof Error) {
        throw (Error) error;
      }
      throw new ValidationException("Failed to validate", error, transactionId);
    }

    TwoPhaseCommitTransactionResponse response = responseOrError.getResponse();
    if (response.hasError()) {
      TwoPhaseCommitTransactionResponse.Error error = response.getError();
      if (error.getErrorCode() == ErrorCode.TRANSACTION_CONFLICT) {
        throw new ValidationConflictException(error.getMessage(), transactionId);
      }
      throw new ValidationException(error.getMessage(), transactionId);
    }
  }

  public void commit() throws CommitException, UnknownTransactionStatusException {
    throwIfTransactionFinished();

    ResponseOrError responseOrError =
        sendRequest(
            TwoPhaseCommitTransactionRequest.newBuilder()
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
      throw new CommitException("Failed to commit", error, transactionId);
    }

    TwoPhaseCommitTransactionResponse response = responseOrError.getResponse();
    if (response.hasError()) {
      TwoPhaseCommitTransactionResponse.Error error = response.getError();
      switch (error.getErrorCode()) {
        case TRANSACTION_CONFLICT:
          throw new CommitConflictException(error.getMessage(), transactionId);
        case UNKNOWN_TRANSACTION_STATUS:
          throw new UnknownTransactionStatusException(error.getMessage(), transactionId);
        default:
          throw new CommitException(error.getMessage(), transactionId);
      }
    }
  }

  public void rollback() throws RollbackException {
    if (finished.get()) {
      return;
    }

    ResponseOrError responseOrError =
        sendRequest(
            TwoPhaseCommitTransactionRequest.newBuilder()
                .setRollbackRequest(RollbackRequest.getDefaultInstance())
                .build());
    finished.set(true);
    throwIfErrorForRollback(responseOrError);
  }

  private void throwIfErrorForRollback(ResponseOrError responseOrError) throws RollbackException {
    if (responseOrError.isError()) {
      Throwable error = responseOrError.getError();
      if (error instanceof Error) {
        throw (Error) error;
      }
      throw new RollbackException("Failed to rollback", error, transactionId);
    }
    TwoPhaseCommitTransactionResponse response = responseOrError.getResponse();
    if (response.hasError()) {
      throw new RollbackException(response.getError().getMessage(), transactionId);
    }
  }

  public void abort() throws AbortException {
    if (finished.get()) {
      return;
    }

    ResponseOrError responseOrError =
        sendRequest(
            TwoPhaseCommitTransactionRequest.newBuilder()
                .setAbortRequest(TwoPhaseCommitTransactionRequest.AbortRequest.getDefaultInstance())
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
      throw new AbortException("Failed to abort", error, transactionId);
    }
    TwoPhaseCommitTransactionResponse response = responseOrError.getResponse();
    if (response.hasError()) {
      throw new AbortException(response.getError().getMessage(), transactionId);
    }
  }

  private static class ResponseOrError {
    private final TwoPhaseCommitTransactionResponse response;
    private final Throwable error;

    public ResponseOrError(TwoPhaseCommitTransactionResponse response) {
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

    public TwoPhaseCommitTransactionResponse getResponse() {
      return response;
    }

    public Throwable getError() {
      return error;
    }
  }
}
