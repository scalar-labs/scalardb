package com.scalar.db.common;

import com.google.errorprone.annotations.concurrent.LazyInit;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scanner;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

public abstract class AbstractScanner implements Scanner {

  @LazyInit private ScannerIterator scannerIterator;

  @Override
  @Nonnull
  public Iterator<Result> iterator() {
    if (scannerIterator == null) {
      scannerIterator = new ScannerIterator(this);
    }
    return scannerIterator;
  }

  @NotThreadSafe
  public static class ScannerIterator implements Iterator<Result> {

    private final Scanner scanner;
    private Result next;

    public ScannerIterator(Scanner scanner) {
      this.scanner = Objects.requireNonNull(scanner);
    }

    @Override
    public boolean hasNext() {
      if (next != null) {
        return true;
      }

      try {
        return (next = scanner.one().orElse(null)) != null;
      } catch (ExecutionException e) {
        throw new RuntimeException(e.getMessage(), e);
      }
    }

    @Override
    public Result next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      Result ret = next;
      next = null;
      return ret;
    }
  }
}
