package com.scalar.db.storage.rpc;

import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.ScannerIterator;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.rpc.DistributedStorageGrpc;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class ScannerImpl implements Scanner {

  private final GrpcScanOnBidirectionalStream stream;

  private List<Result> results;

  public ScannerImpl(
      GrpcConfig config,
      Scan scan,
      DistributedStorageGrpc.DistributedStorageStub stub,
      TableMetadata metadata)
      throws ExecutionException {
    stream = new GrpcScanOnBidirectionalStream(config, stub, metadata);
    results = stream.openScanner(scan);
  }

  // For the SpotBugs warning CT_CONSTRUCTOR_THROW
  @Override
  protected final void finalize() {}

  @Override
  public Optional<Result> one() throws ExecutionException {
    if (results.isEmpty()) {
      return Optional.empty();
    }
    Result result = results.remove(0);
    if (results.isEmpty() && stream.hasMoreResults()) {
      results = stream.next();
    }
    return Optional.of(result);
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
      stream.closeScanner();
    } catch (ExecutionException e) {
      throw new IOException("close failed", e);
    }
  }

  @Override
  public Iterator<Result> iterator() {
    return new ScannerIterator(this);
  }
}
