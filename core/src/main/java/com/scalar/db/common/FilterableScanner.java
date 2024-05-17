package com.scalar.db.common;

import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.Selection.Conjunction;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.util.ScalarDbUtils;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class FilterableScanner implements Scanner {

  private final Scanner scanner;
  private final List<String> projections;
  private final Set<Conjunction> conjunctions;
  private int left;

  private ScannerIterator scannerIterator;

  public FilterableScanner(Scan scan, Scanner scanner) {
    this.scanner = scanner;
    this.projections = scan.getProjections();
    this.conjunctions = scan.getConjunctions();
    this.left = scan.getLimit() > 0 ? scan.getLimit() : -1;
  }

  @Override
  public Optional<Result> one() throws ExecutionException {
    if (left == 0) {
      return Optional.empty();
    }
    while (true) {
      Optional<Result> one = scanner.one();
      if (one.isPresent()) {
        if (ScalarDbUtils.columnsMatchAnyOfConjunctions(one.get().getColumns(), conjunctions)) {
          if (left > 0) {
            left--;
          }
          return Optional.of(new ProjectedResult(one.get(), projections));
        }
      } else {
        return Optional.empty();
      }
    }
  }

  @Override
  public List<Result> all() throws ExecutionException {
    List<Result> ret = new ArrayList<>();
    while (true) {
      Optional<Result> one = one();
      if (!one.isPresent()) {
        break;
      }
      ret.add(one.get());
    }
    return ret;
  }

  @Override
  @Nonnull
  public Iterator<Result> iterator() {
    if (scannerIterator == null) {
      scannerIterator = new ScannerIterator(this);
    }
    return scannerIterator;
  }

  @Override
  public void close() {}
}
