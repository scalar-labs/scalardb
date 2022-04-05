package com.scalar.db.sql;

import com.scalar.db.api.Result;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class ResultIteratorResultSet implements ResultSet {

  private final Iterator<Result> iterator;
  private final List<Projection> projections;

  ResultIteratorResultSet(Iterator<Result> iterator, List<Projection> projections) {
    this.iterator = Objects.requireNonNull(iterator);
    this.projections = Objects.requireNonNull(projections);
  }

  @Override
  public Optional<Record> one() {
    if (iterator.hasNext()) {
      return Optional.of(new ResultRecord(iterator.next(), projections));
    }
    return Optional.empty();
  }

  @Override
  public Iterator<Record> iterator() {
    return new ResultIterator(iterator, projections);
  }

  private static class ResultIterator implements Iterator<Record> {

    private final Iterator<Result> iterator;
    private final List<Projection> projections;

    public ResultIterator(Iterator<Result> iterator, List<Projection> projections) {
      this.iterator = iterator;
      this.projections = projections;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Record next() {
      return new ResultRecord(iterator.next(), projections);
    }
  }
}
