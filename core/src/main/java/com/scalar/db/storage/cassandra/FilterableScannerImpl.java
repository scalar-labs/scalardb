package com.scalar.db.storage.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Conjunction;
import com.scalar.db.api.Scanner;
import com.scalar.db.util.ScalarDbUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public final class FilterableScannerImpl extends ScannerImpl implements Scanner {

  private final Set<Conjunction> conjunctions;
  private int left;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public FilterableScannerImpl(
      Scan scan, ResultSet resultSet, ResultInterpreter resultInterpreter) {
    super(resultSet, resultInterpreter);
    this.conjunctions = scan.getConjunctions();
    this.left = scan.getLimit() > 0 ? scan.getLimit() : -1;
  }

  @Override
  @Nonnull
  public Optional<Result> one() {
    while (true) {
      Row row = resultSet.one();
      if (row == null) {
        return Optional.empty();
      } else {
        if (left == 0) {
          return Optional.empty();
        }
        Optional<Result> result = getResultWithFiltering(row);
        if (result.isPresent()) {
          return result;
        }
      }
    }
  }

  @Override
  @Nonnull
  public List<Result> all() {
    List<Result> results = new ArrayList<>();
    resultSet.forEach(
        r -> {
          if (left == 0) {
            return;
          }
          Optional<Result> result = getResultWithFiltering(r);
          result.ifPresent(results::add);
        });
    return results;
  }

  private Optional<Result> getResultWithFiltering(Row row) {
    Result result = resultInterpreter.interpret(row);
    if (ScalarDbUtils.columnsMatchAnyOfConjunctions(result.getColumns(), conjunctions)) {
      if (left > 0) {
        left--;
      }
      return Optional.of(result);
    }
    return Optional.empty();
  }
}
