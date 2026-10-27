package com.scalar.db.common;

import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.Selection;
import com.scalar.db.api.Selection.Conjunction;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.util.ScalarDbUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class FilterableScanner extends AbstractScanner {

  private final Scanner scanner;
  private final List<String> projections;
  private final Set<Conjunction> conjunctions;
  @Nullable private Integer left = null;

  public FilterableScanner(Selection selection, Scanner scanner) {
    this.scanner = scanner;
    this.projections = selection.getProjections();
    this.conjunctions = selection.getConjunctions();
    if (selection instanceof Scan) {
      Scan scan = (Scan) selection;
      this.left = scan.getLimit() > 0 ? scan.getLimit() : null;
    }
  }

  @SuppressFBWarnings("DLS_DEAD_LOCAL_STORE")
  @Override
  public Optional<Result> one() throws ExecutionException {
    if (left != null && left == 0) {
      return Optional.empty();
    }
    while (true) {
      Optional<Result> one = scanner.one();
      if (one.isPresent()) {
        if (ScalarDbUtils.columnsMatchAnyOfConjunctions(one.get().getColumns(), conjunctions)) {
          if (left != null) {
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
  public void close() throws IOException {
    scanner.close();
  }
}
