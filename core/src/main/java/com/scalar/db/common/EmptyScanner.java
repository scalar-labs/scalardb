package com.scalar.db.common;

import com.scalar.db.api.Result;
import com.scalar.db.api.Scanner;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class EmptyScanner implements Scanner {

  @Override
  public Optional<Result> one() {
    return Optional.empty();
  }

  @Override
  public List<Result> all() {
    return Collections.emptyList();
  }

  @Override
  public void close() {}

  @Override
  public Iterator<Result> iterator() {
    return Collections.emptyIterator();
  }
}
