package com.scalar.db.sql;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class SingleRecordResultSet implements ResultSet {

  private final Iterator<Record> iterator;

  public SingleRecordResultSet(Record record) {
    iterator = Collections.singletonList(record).iterator();
  }

  @Override
  public Optional<Record> one() {
    if (iterator.hasNext()) {
      return Optional.of(iterator.next());
    }
    return Optional.empty();
  }

  @Override
  public List<Record> all() {
    if (iterator.hasNext()) {
      return Collections.singletonList(iterator.next());
    }
    return Collections.emptyList();
  }

  @Override
  public Iterator<Record> iterator() {
    return iterator;
  }

  @Override
  public void close() {}
}
