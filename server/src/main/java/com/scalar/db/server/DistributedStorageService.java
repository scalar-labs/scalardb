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
import com.scalar.db.util.ProtoUtils;
import com.scalar.db.util.ThrowableRunnable;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class DistributedStorageService extends DistributedStorageGrpc.DistributedStorageImplBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(DistributedStorageService.class);
  private static final String SERVICE_NAME = "distributed_storage";
  private static final int DEFAULT_SCAN_FETCH_COUNT = 100;

  private final DistributedStorage storage;
  private final GateKeeper gateKeeper;
  private final Metrics metrics;

  @Inject
  public DistributedStorageService(
      DistributedStorage storage, GateKeeper gateKeeper, Metrics metrics) {
    this.storage = storage;
    this.gateKeeper = gateKeeper;
    this.metrics = metrics;
  }

  @Override
  public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
    execute(
        () -> {
          Get get = ProtoUtils.toGet(request.getGet());
          Optional<Result> result = storage.get(get);
          GetResponse.Builder builder = GetResponse.newBuilder();
          result.ifPresent(r -> builder.setResult(ProtoUtils.toResult(r)));
          responseObserver.onNext(builder.build());
          responseObserver.onCompleted();
        },
        responseObserver,
        "get");
  }

  @Override
  public StreamObserver<ScanRequest> scan(StreamObserver<ScanResponse> responseObserver) {
    return new ScanStreamObserver(
        storage, responseObserver, metrics, this::preProcess, this::postProcess);
  }

  @Override
  public void mutate(MutateRequest request, StreamObserver<Empty> responseObserver) {
    execute(
        () -> {
          List<Mutation> mutations = new ArrayList<>(request.getMutationCount());
          for (com.scalar.db.rpc.Mutation mutation : request.getMutationList()) {
            mutations.add(ProtoUtils.toMutation(mutation));
          }
          storage.mutate(mutations);
          responseObserver.onNext(Empty.getDefaultInstance());
          responseObserver.onCompleted();
        },
        responseObserver,
        "mutate");
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

  private static class ScanStreamObserver implements StreamObserver<ScanRequest> {

    private final DistributedStorage storage;
    private final StreamObserver<ScanResponse> responseObserver;
    private final Metrics metrics;
    private final Function<StreamObserver<?>, Boolean> preProcessor;
    private final Runnable postProcessor;
    private final AtomicBoolean preProcessed = new AtomicBoolean();
    private final AtomicBoolean cleanedUp = new AtomicBoolean();

    private Scanner scanner;

    public ScanStreamObserver(
        DistributedStorage storage,
        StreamObserver<ScanResponse> responseObserver,
        Metrics metrics,
        Function<StreamObserver<?>, Boolean> preProcessor,
        Runnable postProcessor) {
      this.storage = storage;
      this.responseObserver = responseObserver;
      this.metrics = metrics;
      this.preProcessor = preProcessor;
      this.postProcessor = postProcessor;
    }

    @Override
    public void onNext(ScanRequest request) {
      if (preProcessed.compareAndSet(false, true)) {
        if (!preProcessor.apply(responseObserver)) {
          return;
        }
      }

      if (scanner == null) {
        if (!request.hasScan()) {
          respondInvalidArgumentError(
              "the request doesn't have a Scan object even though scanner hasn't been opened yet");
          return;
        }
        if (!openScanner(request)) {
          // failed to open a scanner
          return;
        }
      } else if (request.hasScan()) {
        respondInvalidArgumentError("scanner has already been opened. Don't specify a Scan object");
        return;
      }

      ScanResponse response = next(request);
      if (response == null) {
        // failed to next
        return;
      }

      if (response.getHasMoreResults()) {
        responseObserver.onNext(response);
      } else {
        // cleans up and completes this stream if no more results
        cleanUp();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
      }
    }

    private boolean openScanner(ScanRequest request) {
      try {
        metrics.measure(
            SERVICE_NAME,
            "scan.open_scanner",
            () -> {
              Scan scan = ProtoUtils.toScan(request.getScan());
              scanner = storage.scan(scan);
            });
        return true;
      } catch (IllegalArgumentException e) {
        respondInvalidArgumentError(e.getMessage());
      } catch (Throwable t) {
        LOGGER.error("an internal error happened when opening a scanner", t);
        respondInternalError(t.getMessage());
        if (t instanceof Error) {
          throw (Error) t;
        }
      }
      return false;
    }

    private ScanResponse next(ScanRequest request) {
      try {
        return metrics.measure(
            SERVICE_NAME,
            "scan.next",
            () -> {
              Iterator<Result> resultIterator = scanner.iterator();
              List<Result> results =
                  fetch(
                      resultIterator,
                      request.hasFetchCount() ? request.getFetchCount() : DEFAULT_SCAN_FETCH_COUNT);
              ScanResponse.Builder builder = ScanResponse.newBuilder();
              results.forEach(r -> builder.addResult(ProtoUtils.toResult(r)));
              return builder.setHasMoreResults(resultIterator.hasNext()).build();
            });
      } catch (Throwable t) {
        LOGGER.error("an internal error happened during the execution", t);
        respondInternalError(t.getMessage());
        if (t instanceof Error) {
          throw (Error) t;
        }
      }
      return null;
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

    @Override
    public void onError(Throwable t) {
      LOGGER.error("an error received", t);
      cleanUp();
    }

    @Override
    public void onCompleted() {
      if (!cleanedUp.get()) {
        cleanUp();
        responseObserver.onCompleted();
      }
    }

    private void cleanUp() {
      try {
        if (scanner != null) {
          scanner.close();
        }
      } catch (IOException e) {
        LOGGER.warn("failed to close the scanner");
      }

      postProcessor.run();
      cleanedUp.set(true);
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
  }
}
