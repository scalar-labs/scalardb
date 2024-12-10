package com.scalar.db.dataloader.core.dataexport;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents the report of exported data from a table
 *
 * @author Jishnu J
 */
public class ExportReport {

  /**
   * The field is used to get the total number of rows exported from the table and written to the
   * exported file. AtomicInteger is used because it is thread safe and this parameter is a counter
   * which is updated inside threads
   */
  private final AtomicInteger exportedRowCount;

  public ExportReport() {
    this.exportedRowCount = new AtomicInteger(0);
  }

  public AtomicInteger getExportedRowCount() {
    return exportedRowCount;
  }

  public void increaseExportedRowCount() {
    this.exportedRowCount.incrementAndGet();
  }
}
