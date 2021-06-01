package com.scalar.db.storage.rpc;

import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.rpc.CloseScannerRequest;
import com.scalar.db.rpc.DistributedStorageGrpc;
import com.scalar.db.rpc.OpenScannerRequest;
import com.scalar.db.rpc.OpenScannerResponse;
import com.scalar.db.rpc.ScanNextRequest;
import com.scalar.db.rpc.ScanNextResponse;
import com.scalar.db.rpc.util.ProtoUtil;
import com.scalar.db.storage.common.ScannerIterator;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class ScannerImpl implements Scanner {
  private final DistributedStorageGrpc.DistributedStorageBlockingStub stub;
  private final TableMetadata metadata;
  private final String scannerId;

  private List<Result> results;
  private boolean hasMoreResults;

  public ScannerImpl(
      Scan scan,
      DistributedStorageGrpc.DistributedStorageBlockingStub stub,
      TableMetadata metadata) {
    this.stub = stub;
    this.metadata = metadata;

    OpenScannerResponse openScannerResponse =
        stub.openScanner(OpenScannerRequest.newBuilder().setScan(ProtoUtil.toScan(scan)).build());
    scannerId = openScannerResponse.getScannerId();
    results =
        openScannerResponse.getResultList().stream()
            .map(r -> ProtoUtil.toResult(r, metadata))
            .collect(Collectors.toList());
    hasMoreResults = openScannerResponse.getHasMoreResults();
  }

  @Override
  public Optional<Result> one() throws ExecutionException {
    return GrpcStorage.execute(
        () -> {
          if (results.isEmpty()) {
            return Optional.empty();
          }
          Result result = results.remove(0);
          if (results.isEmpty() && hasMoreResults) {
            ScanNextResponse scanNextResponse =
                stub.scanNext(ScanNextRequest.newBuilder().setScannerId(scannerId).build());
            results =
                scanNextResponse.getResultList().stream()
                    .map(r -> ProtoUtil.toResult(r, metadata))
                    .collect(Collectors.toList());
            hasMoreResults = scanNextResponse.getHasMoreResults();
          }
          return Optional.of(result);
        });
  }

  @Override
  public List<Result> all() throws ExecutionException {
    List<Result> ret = new ArrayList<>();
    while (true) {
      Optional<Result> result = one();
      if (result.isPresent()) {
        ret.add(result.get());
      } else {
        break;
      }
    }
    return ret;
  }

  @Override
  public void close() throws IOException {
    try {
      // if hasMoreResult is false, the scanner should already be closed. So we don't need to close
      // it here
      if (hasMoreResults) {
        stub.closeScanner(CloseScannerRequest.newBuilder().setScannerId(scannerId).build());
      }
    } catch (StatusRuntimeException e) {
      throw new IOException("failed to close the scanner", e);
    }
  }

  @Override
  public Iterator<Result> iterator() {
    return new ScannerIterator(this);
  }
}
