package com.scalar.db.server;

import com.google.inject.Inject;
import com.google.protobuf.Empty;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.rpc.DistributedStorageGrpc;
import com.scalar.db.rpc.GetRequest;
import com.scalar.db.rpc.GetResponse;
import com.scalar.db.rpc.MutateRequest;
import com.scalar.db.rpc.ScanRequest;
import com.scalar.db.rpc.ScanResponse;
import com.scalar.db.util.ProtoUtil;
import com.scalar.db.util.ThrowableRunnable;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedStorageService extends DistributedStorageGrpc.DistributedStorageImplBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(DistributedStorageService.class);

  private static final int DEFAULT_SCAN_FETCH_COUNT = 100;

  private final DistributedStorage storage;

  @Inject
  public DistributedStorageService(DistributedStorage storage) {
    this.storage = storage;
  }

  @Override
  public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
    execute(
        () -> {
          Get get = ProtoUtil.toGet(request.getGet());
          Optional<Result> result = storage.get(get);
          GetResponse.Builder builder = GetResponse.newBuilder();
          result.ifPresent(r -> builder.setResult(ProtoUtil.toResult(r)));
          responseObserver.onNext(builder.build());
          responseObserver.onCompleted();
        },
        responseObserver);
  }

  @Override
  public StreamObserver<ScanRequest> scan(StreamObserver<ScanResponse> responseObserver) {
    return new ScanStreamObserver(storage, responseObserver);
  }

  @Override
  public void mutate(MutateRequest request, StreamObserver<Empty> responseObserver) {
    execute(
        () -> {
          List<Mutation> mutations = new ArrayList<>(request.getMutationCount());
          for (com.scalar.db.rpc.Mutation mutation : request.getMutationList()) {
            mutations.add(ProtoUtil.toMutation(mutation));
          }
          storage.mutate(mutations);
          responseObserver.onNext(Empty.getDefaultInstance());
          responseObserver.onCompleted();
        },
        responseObserver);
  }

  private static void execute(
      ThrowableRunnable<Throwable> runnable, StreamObserver<?> responseObserver) {
    try {
      runnable.run();
    } catch (IllegalArgumentException | IllegalStateException e) {
      responseObserver.onError(
          Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
    } catch (NoMutationException e) {
      responseObserver.onError(
          Status.FAILED_PRECONDITION.withDescription(e.getMessage()).asRuntimeException());
    } catch (Throwable t) {
      LOGGER.error("an internal error happened during the execution", t);
      responseObserver.onError(
          Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
      if (t instanceof Error) {
        throw (Error) t;
      }
    }
  }

  private static class ScanStreamObserver implements StreamObserver<ScanRequest> {

    private final DistributedStorage storage;
    private final StreamObserver<ScanResponse> responseObserver;

    private Scanner scanner;

    public ScanStreamObserver(
        DistributedStorage storage, StreamObserver<ScanResponse> responseObserver) {
      this.storage = storage;
      this.responseObserver = responseObserver;
    }

    @Override
    public void onNext(ScanRequest request) {
      if (scanner == null) {
        if (!request.hasScan()) {
          respondInvalidArgumentError(
              "the request doesn't have a Scan object even though scanner hasn't been opened yet");
          return;
        }
        if (!openScanner(request)) {
          return;
        }
      } else if (request.hasScan()) {
        respondInvalidArgumentError("scanner has already been opened. Don't specify a Scan object");
        return;
      }

      Iterator<Result> resultIterator = scanner.iterator();
      List<Result> results =
          fetch(
              resultIterator,
              request.hasFetchCount() ? request.getFetchCount() : DEFAULT_SCAN_FETCH_COUNT);
      boolean hasMoreResults = resultIterator.hasNext();

      ScanResponse.Builder builder = ScanResponse.newBuilder();
      results.forEach(r -> builder.addResult(ProtoUtil.toResult(r)));
      responseObserver.onNext(builder.setHasMoreResults(hasMoreResults).build());

      if (!hasMoreResults) {
        closeScanner();
        responseObserver.onCompleted();
      }
    }

    private boolean openScanner(ScanRequest request) {
      try {
        Scan scan = ProtoUtil.toScan(request.getScan());
        scanner = storage.scan(scan);
        return true;
      } catch (IllegalArgumentException e) {
        respondInvalidArgumentError(e.getMessage());
        return false;
      } catch (Throwable t) {
        LOGGER.error("an internal error happened when opening a scanner", t);
        respondInternalError(t.getMessage());
        if (t instanceof Error) {
          throw (Error) t;
        }
        return false;
      }
    }

    @Override
    public void onError(Throwable t) {
      LOGGER.error("an error received", t);
      closeScanner();
    }

    @Override
    public void onCompleted() {
      responseObserver.onCompleted();
      closeScanner();
    }

    private List<Result> fetch(Iterator<Result> resultIterator, int fetchCount) {
      List<Result> results = new ArrayList<>(fetchCount);
      for (int i = 0; i < fetchCount; i++) {
        if (!resultIterator.hasNext()) {
          break;
        }
        results.add(resultIterator.next());
      }
      return results;
    }

    private void closeScanner() {
      try {
        if (scanner != null) {
          scanner.close();
        }
      } catch (IOException e) {
        LOGGER.warn("failed to close the scanner");
      }
    }

    private void respondInternalError(String message) {
      responseObserver.onError(Status.INTERNAL.withDescription(message).asRuntimeException());
      closeScanner();
    }

    private void respondInvalidArgumentError(String message) {
      responseObserver.onError(
          Status.INVALID_ARGUMENT.withDescription(message).asRuntimeException());
      closeScanner();
    }
  }
}
