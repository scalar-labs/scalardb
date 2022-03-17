package com.scalar.db.sql;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class EmptyResultSet implements ResultSet {

  private static final EmptyResultSet INSTANCE = new EmptyResultSet();

  public static EmptyResultSet get() {
    return INSTANCE;
  }

  private EmptyResultSet() {}

  @Override
  public Optional<Record> one() {
    return Optional.empty();
  }

  @Override
  public List<Record> all() {
    return Collections.emptyList();
  }

  @Override
  public Iterator<Record> iterator() {
    return Collections.emptyIterator();
  }
}
