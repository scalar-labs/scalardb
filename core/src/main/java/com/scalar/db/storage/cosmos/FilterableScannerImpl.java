package com.scalar.db.storage.cosmos;

import com.azure.cosmos.models.FeedResponse;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Conjunction;
import com.scalar.db.api.Scanner;
import com.scalar.db.util.ScalarDbUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
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
      Scan scan, Iterator<FeedResponse<Record>> recordsPages, ResultInterpreter resultInterpreter) {
    super(recordsPages, resultInterpreter);
    this.conjunctions = scan.getConjunctions();
    this.left = scan.getLimit() > 0 ? scan.getLimit() : -1;
  }

  @Override
  @Nonnull
  public Optional<Result> one() {
    while (currentPageRecords.hasNext() || recordsPages.hasNext()) {
      // Return the next record of the current page if there is one
      if (currentPageRecords.hasNext()) {
        if (left == 0) {
          return Optional.empty();
        }
        Optional<Result> result = getResultWithFiltering(currentPageRecords.next());
        if (result.isPresent()) {
          return result;
        }
      } else {
        // Otherwise, advance to the next page
        currentPageRecords = recordsPages.next().getResults().iterator();
      }
    }
    // There is no records left
    return Optional.empty();
  }

  @Override
  @Nonnull
  public List<Result> all() {
    List<Result> ret = new ArrayList<>();
    // Consume the remaining records of the current page
    while (currentPageRecords.hasNext()) {
      if (left == 0) {
        break;
      }
      Optional<Result> result = getResultWithFiltering(currentPageRecords.next());
      result.ifPresent(ret::add);
    }

    // Consume all the records of the remaining pages
    while (recordsPages.hasNext()) {
      if (left == 0) {
        break;
      }
      for (Record record : recordsPages.next().getResults()) {
        if (left == 0) {
          break;
        }
        Optional<Result> result = getResultWithFiltering(record);
        result.ifPresent(ret::add);
      }
    }

    // Set to empty iterator to release the records from the memory
    currentPageRecords = Collections.emptyIterator();
    recordsPages = Collections.emptyIterator();

    return ret;
  }

  private Optional<Result> getResultWithFiltering(Record record) {
    Result result = resultInterpreter.interpret(record);
    if (ScalarDbUtils.columnsMatchAnyOfConjunctions(result.getColumns(), conjunctions)) {
      if (left > 0) {
        left--;
      }
      return Optional.of(result);
    }
    return Optional.empty();
  }
}
