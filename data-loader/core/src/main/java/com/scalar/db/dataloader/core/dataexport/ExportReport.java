package com.scalar.db.dataloader.core.dataexport;

import java.util.concurrent.atomic.LongAdder;

/**
 * Represents the report of exported data from a table
 *
 * @author Jishnu J
 */
public class ExportReport {

  /**
   * The field is used to get the total number of rows exported from the table and written to the
   * exported file. LongAdder is used because it is thread-safe and optimized for high contention
   * scenarios where multiple threads are incrementing the counter.
   */
  private final LongAdder exportedRowCount = new LongAdder();

  public long getExportedRowCount() {
    return exportedRowCount.sum();
  }

  public void updateExportedRowCount(long count) {
    this.exportedRowCount.add(count);
  }
}
