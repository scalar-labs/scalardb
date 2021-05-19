package com.scalar.db.storage.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.scalar.db.api.Result;
import java.util.Iterator;
import java.util.NoSuchElementException;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public final class ScannerIterator implements Iterator<Result> {
  private final Iterator<Row> iterator;
  private final CassandraTableMetadata metadata;

  public ScannerIterator(ResultSet resultSet, CassandraTableMetadata metadata) {
    iterator = resultSet.iterator();
    this.metadata = metadata;
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public Result next() {
    Row row = iterator.next();
    if (row == null) {
      throw new NoSuchElementException();
    }
    return new ResultImpl(row, metadata);
  }
}
