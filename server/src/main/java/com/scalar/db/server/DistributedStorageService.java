package com.scalar.db.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Empty;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.rpc.DistributedStorageGrpc;
import com.scalar.db.rpc.GetRequest;
import com.scalar.db.rpc.GetResponse;
import com.scalar.db.rpc.MutateRequest;
import com.scalar.db.rpc.ScanRequest;
import com.scalar.db.rpc.ScanResponse;
import com.scalar.db.util.ProtoUtils;
import com.scalar.db.util.ThrowableRunnable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
  private static final Logger logger = LoggerFactory.getLogger(DistributedStorageService.class);
  private static final String SERVICE_NAME = "distributed_storage";
  private static final int DEFAULT_SCAN_FETCH_COUNT = 100;

  private final DistributedStorage storage;
  private final TableMetadataManager tableMetadataManager;
  private final GateKeeper gateKeeper;
  private final Metrics metrics;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public DistributedStorageService(
      DistributedStorage storage,
      TableMetadataManager tableMetadataManager,
      GateKeeper gateKeeper,
      Metrics metrics) {
    this.storage = storage;
    this.tableMetadataManager = tableMetadataManager;
    this.gateKeeper = gateKeeper;
    this.metrics = metrics;
  }

  @Override
  public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
    execute(
        () -> {
          TableMetadata metadata =
              tableMetadataManager.getTableMetadata(
                  request.getGet().getNamespace(), request.getGet().getTable());
          if (metadata == null) {
            throw new IllegalArgumentException("The specified table is not found");
          }

          Get get = ProtoUtils.toGet(request.getGet(), metadata);
          Optional<Result> result = storage.get(get);

          GetResponse.Builder builder = GetResponse.newBuilder();

          // For backward compatibility
          if (ProtoUtils.isRequestFromOldClient(request.getGet())) {
            result.ifPresent(r -> builder.setResult(ProtoUtils.toResultWithValue(r)));
          } else {
            result.ifPresent(r -> builder.setResult(ProtoUtils.toResult(r)));
          }

          responseObserver.onNext(builder.build());
          responseObserver.onCompleted();
        },
        responseObserver,
        "get");
  }

  @Override
  public StreamObserver<ScanRequest> scan(StreamObserver<ScanResponse> responseObserver) {
    return new ScanStreamObserver(
        storage,
        tableMetadataManager,
        responseObserver,
        metrics,
        this::preProcess,
        this::postProcess);
  }

  @Override
  public void mutate(MutateRequest request, StreamObserver<Empty> responseObserver) {
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

            mutations = new ArrayList<>(request.getMutationsCount());
            for (com.scalar.db.rpc.Mutation mutation : request.getMutationsList()) {
              mutations.add(ProtoUtils.toMutation(mutation, metadata));
            }
          } else {
            mutations = Collections.emptyList();
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
  static class ScanStreamObserver implements StreamObserver<ScanRequest> {

    private final DistributedStorage storage;
    private final TableMetadataManager tableMetadataManager;
    private final StreamObserver<ScanResponse> responseObserver;
    private final Metrics metrics;
    private final Function<StreamObserver<?>, Boolean> preProcessor;
    private final Runnable postProcessor;
    private final AtomicBoolean preProcessed = new AtomicBoolean();
    private final AtomicBoolean cleanedUp = new AtomicBoolean();

    private Scanner scanner;

    // For backward compatibility
    private boolean requestFromOldClient;

    public ScanStreamObserver(
        DistributedStorage storage,
        TableMetadataManager tableMetadataManager,
        StreamObserver<ScanResponse> responseObserver,
        Metrics metrics,
        Function<StreamObserver<?>, Boolean> preProcessor,
        Runnable postProcessor) {
      this.storage = storage;
      this.tableMetadataManager = tableMetadataManager;
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
              TableMetadata metadata =
                  tableMetadataManager.getTableMetadata(
                      request.getScan().getNamespace(), request.getScan().getTable());
              if (metadata == null) {
                throw new IllegalArgumentException("The specified table is not found");
              }
              Scan scan = ProtoUtils.toScan(request.getScan(), metadata);

              // For backward compatibility
              requestFromOldClient = ProtoUtils.isRequestFromOldClient(request.getScan());

              scanner = storage.scan(scan);
            });
        return true;
      } catch (IllegalArgumentException e) {
        respondInvalidArgumentError(e.getMessage());
      } catch (Throwable t) {
        logger.error("An internal error happened when opening a scanner", t);
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

              // For backward compatibility
              if (requestFromOldClient) {
                results.forEach(r -> builder.addResults(ProtoUtils.toResultWithValue(r)));
              } else {
                results.forEach(r -> builder.addResults(ProtoUtils.toResult(r)));
              }

              return builder.setHasMoreResults(resultIterator.hasNext()).build();
            });
      } catch (Throwable t) {
        logger.error("An internal error happened during the execution", t);
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
      logger.error("An error was received", t);
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
        logger.warn("Failed to close the scanner");
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
