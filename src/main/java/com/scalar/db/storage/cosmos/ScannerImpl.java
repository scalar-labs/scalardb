package com.scalar.db.storage.cosmos;

import static com.google.common.base.Preconditions.checkNotNull;

import com.scalar.db.api.Result;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.Selection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public final class ScannerImpl implements Scanner {
  private final List<Record> records;
  private final Selection selection;
  private final TableMetadata metadata;

  public ScannerImpl(List<Record> records, Selection selection, TableMetadata metadata) {
    this.records = checkNotNull(records);
    this.selection = selection;
    this.metadata = checkNotNull(metadata);
  }

  @Override
  @Nonnull
  public Optional<Result> one() {
    if (records.isEmpty()) {
      return Optional.empty();
    }
    Record record = records.remove(0);

    return Optional.of(new ResultImpl(record, selection, metadata));
  }

  @Override
  @Nonnull
  public List<Result> all() {
    List<Result> results = new ArrayList<>();
    records.forEach(r -> results.add(new ResultImpl(r, selection, metadata)));
    return results;
  }

  @Override
  public Iterator<Result> iterator() {
    return new ScannerIterator(records.iterator(), selection, metadata);
  }

  @Override
  public void close() {}
}
