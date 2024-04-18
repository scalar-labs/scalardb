package com.scalar.db.storage.dynamo;

import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Conjunction;
import com.scalar.db.api.Scanner;
import com.scalar.db.storage.dynamo.request.PaginatedRequest;
import com.scalar.db.util.ScalarDbUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;

public class FilterableQueryScanner extends QueryScanner implements Scanner {

  private final Set<Conjunction> conjunctions;
  private int left;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public FilterableQueryScanner(
      Scan scan, PaginatedRequest request, ResultInterpreter resultInterpreter) {
    super(request, resultInterpreter);
    this.conjunctions = scan.getConjunctions();
    this.left = scan.getLimit() > 0 ? scan.getLimit() : -1;
  }

  @Override
  @Nonnull
  public Optional<Result> one() {
    while (hasNext()) {
      if (left == 0) {
        return Optional.empty();
      }
      Result result = resultInterpreter.interpret(itemsIterator.next());
      if (ScalarDbUtils.columnsMatchAnyOfConjunctions(result.getColumns(), conjunctions)) {
        if (left > 0) {
          left--;
        }
        return Optional.of(result);
      }
    }
    return Optional.empty();
  }
}
