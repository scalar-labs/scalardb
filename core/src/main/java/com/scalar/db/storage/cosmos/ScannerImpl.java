package com.scalar.db.storage.cosmos;

import static com.google.common.base.Preconditions.checkNotNull;

import com.azure.cosmos.models.FeedResponse;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scanner;
import com.scalar.db.common.ScannerIterator;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public final class ScannerImpl implements Scanner {

  private final ResultInterpreter resultInterpreter;
  private Iterator<FeedResponse<Record>> recordsPages;
  private Iterator<Record> currentPageRecords;
  private ScannerIterator scannerIterator;

  /**
   * Create a Scanner for Cosmos DB query operations
   *
   * @param recordsPages an iterator over the pages {@code FeedResponse<Record>}, each containing
   *     records.
   * @param resultInterpreter to interpret the result
   */
  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public ScannerImpl(
      Iterator<FeedResponse<Record>> recordsPages, ResultInterpreter resultInterpreter) {
    this.recordsPages = checkNotNull(recordsPages);
    this.currentPageRecords = Collections.emptyIterator();
    this.resultInterpreter = checkNotNull(resultInterpreter);
  }

  @Override
  @Nonnull
  public Optional<Result> one() {
    while (currentPageRecords.hasNext() || recordsPages.hasNext()) {
      // Return the next record of the current page if there is one
      if (currentPageRecords.hasNext()) {
        Record currentRecord = currentPageRecords.next();
        return Optional.of(resultInterpreter.interpret(currentRecord));
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
    currentPageRecords.forEachRemaining(record -> ret.add(resultInterpreter.interpret(record)));

    // Consume all the records of the remaining pages
    recordsPages.forEachRemaining(
        page -> page.getResults().forEach(record -> ret.add(resultInterpreter.interpret(record))));

    // Set to empty iterator to release the records from the memory
    currentPageRecords = Collections.emptyIterator();
    recordsPages = Collections.emptyIterator();

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
