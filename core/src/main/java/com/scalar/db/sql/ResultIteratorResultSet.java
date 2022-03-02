package com.scalar.db.sql;

import com.google.common.collect.ImmutableList;
import com.scalar.db.api.Result;
import java.util.Iterator;
import java.util.Optional;

public class ResultIteratorResultSet implements ResultSet {

  private final Iterator<Result> iterator;
  private final ImmutableList<String> projectedColumnNames;

  public ResultIteratorResultSet(
      Iterator<Result> iterator, ImmutableList<String> projectedColumnNames) {
    this.iterator = iterator;
    this.projectedColumnNames = projectedColumnNames;
  }

  @Override
  public Optional<Record> one() {
    if (iterator.hasNext()) {
      return Optional.of(new ResultRecord(iterator.next(), projectedColumnNames));
    }
    return Optional.empty();
  }

  @Override
  public Iterator<Record> iterator() {
    return new ResultIterator(iterator, projectedColumnNames);
  }

  @Override
  public void close() {}

  private static class ResultIterator implements Iterator<Record> {

    private final Iterator<Result> iterator;
    private final ImmutableList<String> projectedColumnNames;

    public ResultIterator(Iterator<Result> iterator, ImmutableList<String> projectedColumnNames) {
      this.iterator = iterator;
      this.projectedColumnNames = projectedColumnNames;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Record next() {
      return new ResultRecord(iterator.next(), projectedColumnNames);
    }
  }
}
