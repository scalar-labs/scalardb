package com.scalar.db.storage.jdbc;

import com.scalar.db.api.Result;
import com.scalar.db.api.Scanner;
import com.scalar.db.exception.storage.ExecutionException;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

@NotThreadSafe
public class ScannerIterator implements Iterator<Result> {

  private final Scanner scanner;
  private Result next;

  public ScannerIterator(Scanner scanner) {
    this.scanner = Objects.requireNonNull(scanner);
    fetch();
  }

  private void fetch() {
    try {
      next = scanner.one().orElse(null);
    } catch (ExecutionException e) {
      throw new RuntimeException("An error occurred", e);
    }
  }

  @Override
  public boolean hasNext() {
    return next != null;
  }

  @Override
  public Result next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    Result ret = next;
    fetch();
    return ret;
  }
}
