package com.scalar.db.storage.cosmos;

import static com.google.common.base.Preconditions.checkNotNull;

import com.scalar.db.api.Result;
import com.scalar.db.api.Scanner;
import com.scalar.db.storage.common.ScannerIterator;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public final class ScannerImpl implements Scanner {
  private final List<Record> records;
  private final ResultInterpreter resultInterpreter;

  private ScannerIterator scannerIterator;

  public ScannerImpl(List<Record> records, ResultInterpreter resultInterpreter) {
    this.records = checkNotNull(records);
    this.resultInterpreter = checkNotNull(resultInterpreter);
  }

  @Override
  @Nonnull
  public Optional<Result> one() {
    if (records.isEmpty()) {
      return Optional.empty();
    }
    Record record = records.remove(0);

    return Optional.of(resultInterpreter.interpret(record));
  }

  @Override
  @Nonnull
  public List<Result> all() {
    List<Result> results = new ArrayList<>();
    records.forEach(r -> results.add(resultInterpreter.interpret(r)));
    records.clear();
    return results;
  }

  @Override
  @Nonnull
  public Iterator<Result> iterator() {
    if (scannerIterator == null) {
      scannerIterator = new ScannerIterator(this);
    }
    return scannerIterator;
  }

  @Override
  public void close() {}
}
