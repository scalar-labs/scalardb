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
import com.scalar.db.rpc.TransactionalMutateRequest;
import com.scalar.db.rpc.TransactionalScanRequest;
import com.scalar.db.storage.rpc.GrpcTableMetadataManager;
import com.scalar.db.util.ProtoUtil;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class GrpcTransactionBidirectionalStream implements StreamObserver<TransactionResponse> {

  private final StreamObserver<TransactionRequest> transactionObserver;
  private final GrpcTableMetadataManager metadataManager;

  private final AtomicBoolean finished = new AtomicBoolean();

  private TransactionResponse response;
  private Throwable error;
  private CountDownLatch latch;

  public GrpcTransactionBidirectionalStream(
      DistributedTransactionStub stub, GrpcTableMetadataManager metadataManager) {
    transactionObserver = stub.transaction(this);
    this.metadataManager = metadataManager;
  }

  @Override
  public void onNext(TransactionResponse response) {
    this.response = response;
    latch.countDown();
  }

  @Override
  public void onError(Throwable t) {
    error = t;
    finished.set(true);
    latch.countDown();
  }

  @Override
  public void onCompleted() {}

  private void sendRequest(TransactionRequest request) {
    init();
    transactionObserver.onNext(request);
    await();
  }

  private void init() {
    latch = new CountDownLatch(1);
    response = null;
    error = null;
  }

  private void await() {
    try {
      latch.await();
    } catch (InterruptedException ignored) {
    }
  }

  private boolean hasError() {
    return error != null;
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
    sendRequest(TransactionRequest.newBuilder().setStartTransactionRequest(request).build());
    throwIfErrorForStart();
    return response.getStartTransactionResponse().getTransactionId();
  }

  private void throwIfErrorForStart() throws TransactionException {
    if (hasError()) {
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

    if (response.hasError()) {
      if (response.getError().getErrorCode() == ErrorCode.INVALID_ARGUMENT) {
        throw new IllegalArgumentException(response.getError().getMessage());
      }
      throw new TransactionException(response.getError().getMessage());
    }
  }

  public Optional<Result> get(Get get) throws CrudException {
    throwIfTransactionFinished();

    sendRequest(
        TransactionRequest.newBuilder()
            .setTransactionalGetRequest(
                TransactionalGetRequest.newBuilder().setGet(ProtoUtil.toGet(get)))
            .build());
    throwIfErrorForCrud();

    if (response.getTransactionalGetResponse().hasResult()) {
      TableMetadata tableMetadata = metadataManager.getTableMetadata(get);
      return Optional.of(
          ProtoUtil.toResult(response.getTransactionalGetResponse().getResult(), tableMetadata));
    }

    return Optional.empty();
  }

  public List<Result> scan(Scan scan) throws CrudException {
    throwIfTransactionFinished();

    sendRequest(
        TransactionRequest.newBuilder()
            .setTransactionalScanRequest(
                TransactionalScanRequest.newBuilder().setScan(ProtoUtil.toScan(scan)))
            .build());
    throwIfErrorForCrud();

    TableMetadata tableMetadata = metadataManager.getTableMetadata(scan);
    return response.getTransactionalScanResponse().getResultList().stream()
        .map(r -> ProtoUtil.toResult(r, tableMetadata))
        .collect(Collectors.toList());
  }

  public void mutate(Mutation mutation) throws CrudException {
    throwIfTransactionFinished();

    sendRequest(
        TransactionRequest.newBuilder()
            .setTransactionalMutateRequest(
                TransactionalMutateRequest.newBuilder().addMutation(ProtoUtil.toMutation(mutation)))
            .build());
    throwIfErrorForCrud();
  }

  public void mutate(List<? extends Mutation> mutations) throws CrudException {
    throwIfTransactionFinished();

    TransactionalMutateRequest.Builder builder = TransactionalMutateRequest.newBuilder();
    mutations.forEach(m -> builder.addMutation(ProtoUtil.toMutation(m)));
    sendRequest(TransactionRequest.newBuilder().setTransactionalMutateRequest(builder).build());
    throwIfErrorForCrud();
  }

  private void throwIfErrorForCrud() throws CrudException {
    if (hasError()) {
      if (error instanceof Error) {
        throw (Error) error;
      }
      throw new CrudException(error.getMessage());
    }

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

    sendRequest(
        TransactionRequest.newBuilder()
            .setCommitRequest(CommitRequest.getDefaultInstance())
            .build());
    finished.set(true);
    throwIfErrorForCommit();
  }

  private void throwIfErrorForCommit() throws CommitException, UnknownTransactionStatusException {
    if (hasError()) {
      if (error instanceof Error) {
        throw (Error) error;
      }
      throw new CommitException(error.getMessage());
    }

    if (response.hasError()) {
      switch (response.getError().getErrorCode()) {
        case INVALID_ARGUMENT:
          throw new IllegalArgumentException(response.getError().getMessage());
        case CONFLICT:
          throw new CommitConflictException(response.getError().getMessage());
        case UNKNOWN_TRANSACTION:
          throw new UnknownTransactionStatusException(response.getError().getMessage());
        default:
          throw new CommitException(response.getError().getMessage());
      }
    }
  }

  public void abort() throws AbortException {
    if (finished.get()) {
      return;
    }

    sendRequest(
        TransactionRequest.newBuilder().setAbortRequest(AbortRequest.getDefaultInstance()).build());
    finished.set(true);
    throwIfErrorForAbort();
  }

  private void throwIfErrorForAbort() throws AbortException {
    if (hasError()) {
      if (error instanceof Error) {
        throw (Error) error;
      }
      throw new AbortException(error.getMessage());
    }

    if (response.hasError()) {
      if (response.getError().getErrorCode() == ErrorCode.INVALID_ARGUMENT) {
        throw new IllegalArgumentException(response.getError().getMessage());
      }
      throw new AbortException(response.getError().getMessage());
    }
  }
}
