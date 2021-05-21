package com.scalar.db.storage.cassandra;

import static com.google.common.base.Preconditions.checkNotNull;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public final class ScannerImpl implements Scanner {
  private final ResultSet resultSet;
  private final TableMetadata metadata;

  public ScannerImpl(ResultSet resultSet, TableMetadata metadata) {
    this.resultSet = checkNotNull(resultSet);
    this.metadata = checkNotNull(metadata);
  }

  @Override
  @Nonnull
  public Optional<Result> one() {
    Row row = resultSet.one();
    if (row == null) {
      return Optional.empty();
    }
    return Optional.of(new ResultImpl(row, metadata));
  }

  @Override
  @Nonnull
  public List<Result> all() {
    List<Result> results = new ArrayList<>();
    resultSet.forEach(r -> results.add(new ResultImpl(r, metadata)));
    return results;
  }

  @Override
  public Iterator<Result> iterator() {
    return new ScannerIterator(resultSet, metadata);
  }

  @Override
  public void close() {}
}
