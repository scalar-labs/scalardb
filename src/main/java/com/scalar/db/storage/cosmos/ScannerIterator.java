package com.scalar.db.storage.cosmos;

import com.scalar.db.api.Result;
import com.scalar.db.api.Selection;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public final class ScannerIterator implements Iterator<Result> {
  private final Iterator<Record> iterator;
  private final Selection selection;
  private final TableMetadata metadata;

  public ScannerIterator(List<Record> records, Selection selection, TableMetadata metadata) {
    iterator = records.iterator();
    this.selection = selection;
    this.metadata = metadata;
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  @Nullable
  public Result next() {
    Record record = iterator.next();
    if (record == null) {
      return null;
    }

    return new ResultImpl(record, selection, metadata);
  }
}
