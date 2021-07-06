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
import com.scalar.db.rpc.CloseScannerRequest;
import com.scalar.db.rpc.DistributedStorageGrpc;
import com.scalar.db.rpc.GetRequest;
import com.scalar.db.rpc.GetResponse;
import com.scalar.db.rpc.MutateRequest;
import com.scalar.db.rpc.OpenScannerRequest;
import com.scalar.db.rpc.OpenScannerResponse;
import com.scalar.db.rpc.ScanNextRequest;
import com.scalar.db.rpc.ScanNextResponse;
import com.scalar.db.util.ProtoUtil;
import com.scalar.db.util.ThrowableRunnable;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedStorageService extends DistributedStorageGrpc.DistributedStorageImplBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(DistributedStorageService.class);

  private static final long SCANNER_EXPIRATION_TIME_MILLIS = 60000;
  private static final long SCANNER_EXPIRATION_INTERVAL_MILLIS = 1000;
  private static final int DEFAULT_SCAN_FETCH_COUNT = 100;

  private final DistributedStorage storage;
  private final ConcurrentMap<String, ScannerHolder> scanners = new ConcurrentHashMap<>();

  @Inject
  public DistributedStorageService(DistributedStorage storage) {
    this.storage = storage;

    Thread scannerExpirationThread =
        new Thread(
            () -> {
              while (true) {
                try {
                  scanners.entrySet().stream()
                      .filter(e -> e.getValue().isExpired())
                      .map(Entry::getKey)
                      .forEach(
                          id -> {
                            closeScanner(id);
                            LOGGER.warn("the scanner is expired. scannerId: " + id);
                          });
                  TimeUnit.MILLISECONDS.sleep(SCANNER_EXPIRATION_INTERVAL_MILLIS);
                } catch (Exception e) {
                  LOGGER.warn("failed to expire scanners", e);
                }
              }
            });
    scannerExpirationThread.setDaemon(true);
    scannerExpirationThread.setName("Scanner expiration thread");
    scannerExpirationThread.start();
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
  public void openScanner(
      OpenScannerRequest request, StreamObserver<OpenScannerResponse> responseObserver) {
    execute(
        () -> {
          Scan scan = ProtoUtil.toScan(request.getScan());
          Scanner scanner = storage.scan(scan);
          Iterator<Result> resultIterator = scanner.iterator();
          String scannerId = createScannerId();

          int fetchCount = request.getFetchCount();
          List<Result> results =
              fetch(resultIterator, fetchCount != 0 ? fetchCount : DEFAULT_SCAN_FETCH_COUNT);
          boolean hasMoreResults = resultIterator.hasNext();

          if (hasMoreResults) {
            scanners.put(scannerId, new ScannerHolder(scanner, SCANNER_EXPIRATION_TIME_MILLIS));
          } else {
            scanner.close();
          }

          OpenScannerResponse.Builder builder =
              OpenScannerResponse.newBuilder()
                  .setScannerId(scannerId)
                  .setHasMoreResults(hasMoreResults);
          results.forEach(r -> builder.addResult(ProtoUtil.toResult(r)));
          responseObserver.onNext(builder.build());
          responseObserver.onCompleted();
        },
        responseObserver);
  }

  private String createScannerId() {
    return UUID.randomUUID().toString();
  }

  @Override
  public void scanNext(ScanNextRequest request, StreamObserver<ScanNextResponse> responseObserver) {
    execute(
        () -> {
          ScannerHolder scannerHolder = scanners.get(request.getScannerId());
          if (scannerHolder == null) {
            throw new IllegalArgumentException(
                "the specified Scanner is not found. scannerId: " + request.getScannerId());
          }
          Scanner scanner = scannerHolder.get();
          Iterator<Result> resultIterator = scanner.iterator();

          int fetchCount = request.getFetchCount();
          List<Result> results =
              fetch(resultIterator, fetchCount != 0 ? fetchCount : DEFAULT_SCAN_FETCH_COUNT);
          boolean hasMoreResults = resultIterator.hasNext();

          if (!hasMoreResults) {
            scanners.remove(request.getScannerId());
            scanner.close();
          }

          ScanNextResponse.Builder builder =
              ScanNextResponse.newBuilder().setHasMoreResults(hasMoreResults);
          results.forEach(r -> builder.addResult(ProtoUtil.toResult(r)));
          responseObserver.onNext(builder.build());
          responseObserver.onCompleted();
        },
        responseObserver);
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
  public void closeScanner(CloseScannerRequest request, StreamObserver<Empty> responseObserver) {
    execute(
        () -> {
          closeScanner(request.getScannerId());
          responseObserver.onNext(Empty.newBuilder().build());
          responseObserver.onCompleted();
        },
        responseObserver);
  }

  private void closeScanner(String scannerId) {
    try {
      ScannerHolder scannerHolder = scanners.remove(scannerId);
      if (scannerHolder != null) {
        scannerHolder.get().close();
      }
    } catch (IOException e) {
      LOGGER.warn("failed to close the scanner. scannerId: " + scannerId);
    }
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
      LOGGER.error("an invalid argument error happened during the execution", e);
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

  private static class ScannerHolder {
    private final Scanner scanner;
    private final long scannerExpirationTimeMillis;
    private final AtomicLong expirationTime = new AtomicLong();

    public ScannerHolder(Scanner scanner, long scannerExpirationTimeMillis) {
      this.scanner = Objects.requireNonNull(scanner);
      this.scannerExpirationTimeMillis = scannerExpirationTimeMillis;
      updateExpirationTime();
    }

    public void updateExpirationTime() {
      expirationTime.set(System.currentTimeMillis() + scannerExpirationTimeMillis);
    }

    public Scanner get() {
      updateExpirationTime();
      return scanner;
    }

    public boolean isExpired() {
      return System.currentTimeMillis() >= expirationTime.get();
    }
  }
}
