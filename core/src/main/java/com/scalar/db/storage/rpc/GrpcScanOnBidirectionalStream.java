package com.scalar.db.storage.rpc;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.rpc.DistributedStorageGrpc;
import com.scalar.db.rpc.ScanRequest;
import com.scalar.db.rpc.ScanResponse;
import com.scalar.db.util.ProtoUtils;
import com.scalar.db.util.ScalarDbUtils;
import com.scalar.db.util.retry.ServiceTemporaryUnavailableException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class GrpcScanOnBidirectionalStream
    implements ClientResponseObserver<ScanRequest, ScanResponse> {

  private final GrpcConfig config;
  private final TableMetadata metadata;
  private final BlockingQueue<ResponseOrError> queue = new LinkedBlockingQueue<>();
  private final AtomicBoolean hasMoreResults = new AtomicBoolean(true);

  private ClientCallStreamObserver<ScanRequest> requestStream;

  public GrpcScanOnBidirectionalStream(
      GrpcConfig config,
      DistributedStorageGrpc.DistributedStorageStub stub,
      TableMetadata metadata) {
    this.config = config;
    this.metadata = metadata;
    stub.scan(this);
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  @Override
  public void beforeStart(ClientCallStreamObserver<ScanRequest> requestStream) {
    this.requestStream = requestStream;
  }

  @Override
  public void onNext(ScanResponse response) {
    Uninterruptibles.putUninterruptibly(queue, new ResponseOrError(response));
  }

  @Override
  public void onError(Throwable t) {
    Uninterruptibles.putUninterruptibly(queue, new ResponseOrError(t));
  }

  @Override
  public void onCompleted() {}

  private ResponseOrError sendRequest(ScanRequest request) {
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

  private void throwIfScannerHasNoMoreResults() {
    if (!hasMoreResults.get()) {
      throw new IllegalStateException("The scan operation has no more results");
    }
  }

  private List<Result> getResults(ScanResponse response) throws ExecutionException {
    if (!response.getHasMoreResults()) {
      // We can early close the scanner as it has no more results
      closeScanner();
    }
    return response.getResultsList().stream()
        .map(r -> ProtoUtils.toResult(r, metadata))
        .collect(Collectors.toList());
  }

  public List<Result> openScanner(Scan scan) throws ExecutionException {
    throwIfScannerHasNoMoreResults();
    ResponseOrError responseOrError =
        sendRequest(ScanRequest.newBuilder().setScan(ProtoUtils.toScan(scan)).build());
    throwIfErrorForOpenScanner(responseOrError);
    return getResults(responseOrError.getResponse());
  }

  private void throwIfErrorForOpenScanner(ResponseOrError responseOrError)
      throws ExecutionException {
    if (responseOrError.isError()) {
      hasMoreResults.set(false);
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
      throw new ExecutionException("Failed to open scanner", error);
    }
  }

  public List<Result> next() throws ExecutionException {
    throwIfScannerHasNoMoreResults();
    ResponseOrError responseOrError = sendRequest(ScanRequest.getDefaultInstance());
    throwIfErrorForNext(responseOrError);
    return getResults(responseOrError.getResponse());
  }

  public List<Result> next(int fetchCount) throws ExecutionException {
    throwIfScannerHasNoMoreResults();
    ResponseOrError responseOrError =
        sendRequest(ScanRequest.newBuilder().setFetchCount(fetchCount).build());
    throwIfErrorForNext(responseOrError);
    return getResults(responseOrError.getResponse());
  }

  private void throwIfErrorForNext(ResponseOrError responseOrError) throws ExecutionException {
    if (responseOrError.isError()) {
      hasMoreResults.set(false);
      Throwable error = responseOrError.getError();
      if (error instanceof StatusRuntimeException) {
        StatusRuntimeException e = (StatusRuntimeException) error;
        if (e.getStatus().getCode() == Code.INVALID_ARGUMENT) {
          throw new IllegalArgumentException(e.getMessage(), e);
        }
      }
      if (error instanceof Error) {
        throw (Error) error;
      }
      throw new ExecutionException("Failed to next", error);
    }
  }

  public void closeScanner() throws ExecutionException {
    try {
      if (!hasMoreResults.get()) {
        return;
      }
      hasMoreResults.set(false);
      requestStream.onCompleted();
    } catch (StatusRuntimeException e) {
      throw new ExecutionException("Failed to close the scanner", e);
    }
  }

  public boolean hasMoreResults() {
    return hasMoreResults.get();
  }

  private static class ResponseOrError {
    private final ScanResponse response;
    private final Throwable error;

    public ResponseOrError(ScanResponse response) {
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

    public ScanResponse getResponse() {
      return response;
    }

    public Throwable getError() {
      return error;
    }
  }
}
