package com.scalar.db.storage.rpc;

import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.rpc.DistributedStorageGrpc;
import com.scalar.db.rpc.ScanRequest;
import com.scalar.db.rpc.ScanResponse;
import com.scalar.db.util.ProtoUtil;
import com.scalar.db.util.retry.ServiceTemporaryUnavailableException;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class GrpcScanOnBidirectionalStream implements StreamObserver<ScanResponse> {

  private final StreamObserver<ScanRequest> requestObserver;
  private final TableMetadata metadata;
  private final BlockingQueue<ResponseOrError> queue = new LinkedBlockingQueue<>();
  private final AtomicBoolean hasMoreResults = new AtomicBoolean(true);

  public GrpcScanOnBidirectionalStream(
      DistributedStorageGrpc.DistributedStorageStub stub, TableMetadata metadata) {
    this.metadata = metadata;
    requestObserver = stub.scan(this);
  }

  @Override
  public void onNext(ScanResponse response) {
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

  private ResponseOrError sendRequest(ScanRequest request) {
    requestObserver.onNext(request);
    try {
      return queue.take();
    } catch (InterruptedException ignored) {
      // InterruptedException should not be thrown
      return null;
    }
  }

  private void throwIfScannerHasNoMoreResults() {
    if (!hasMoreResults.get()) {
      throw new IllegalStateException("the scan operation has no more results");
    }
  }

  private List<Result> getResults(ScanResponse response) {
    if (!response.getHasMoreResults()) {
      hasMoreResults.set(false);
    }
    return response.getResultList().stream()
        .map(r -> ProtoUtil.toResult(r, metadata))
        .collect(Collectors.toList());
  }

  public List<Result> openScanner(Scan scan) throws ExecutionException {
    throwIfScannerHasNoMoreResults();
    ResponseOrError responseOrError =
        sendRequest(ScanRequest.newBuilder().setScan(ProtoUtil.toScan(scan)).build());
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
      throw new ExecutionException("failed to open scanner", error);
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
      throw new ExecutionException("failed to next", error);
    }
  }

  public void closeScanner() throws IOException {
    try {
      if (!hasMoreResults.get()) {
        return;
      }
      requestObserver.onCompleted();
      hasMoreResults.set(false);
    } catch (StatusRuntimeException e) {
      throw new IOException("failed to close the scanner", e);
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
