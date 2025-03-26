package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.Result;
import com.scalar.db.api.Scanner;
import com.scalar.db.common.ScannerIterator;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class ScannerImpl implements Scanner {
  private final Iterator<ObjectStorageRecord> recordIterator;
  private final ResultInterpreter resultInterpreter;
  private final int recordCountLimit;

  private ScannerIterator scannerIterator;
  private int recordCount;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public ScannerImpl(
      Iterator<ObjectStorageRecord> recordIterator,
      ResultInterpreter resultInterpreter,
      int recordCountLimit) {
    this.recordIterator = recordIterator;
    this.resultInterpreter = resultInterpreter;
    this.recordCountLimit = recordCountLimit;
    this.recordCount = 0;
  }

  @Override
  @Nonnull
  public Optional<Result> one() {
    if (!recordIterator.hasNext()) {
      return Optional.empty();
    }
    if (recordCountLimit != 0 && recordCount >= recordCountLimit) {
      return Optional.empty();
    }
    recordCount++;
    return Optional.of(resultInterpreter.interpret(recordIterator.next()));
  }

  @Override
  @Nonnull
  public List<Result> all() {
    List<Result> results = new ArrayList<>();
    Optional<Result> result;
    while ((result = one()).isPresent()) {
      results.add(result.get());
    }
    return results;
  }

  @Override
  public void close() throws IOException {}

  @Override
  @Nonnull
  public Iterator<Result> iterator() {
    if (scannerIterator == null) {
      scannerIterator = new ScannerIterator(this);
    }
    return scannerIterator;
  }
}
