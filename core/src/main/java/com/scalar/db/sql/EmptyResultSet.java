package com.scalar.db.sql;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;

public enum EmptyResultSet implements ResultSet {
  INSTANCE;

  @Override
  public Optional<Record> one() {
    return Optional.empty();
  }

  @Override
  public List<Record> all() {
    return Collections.emptyList();
  }

  @Nonnull
  @Override
  public Iterator<Record> iterator() {
    return Collections.emptyIterator();
  }
}
