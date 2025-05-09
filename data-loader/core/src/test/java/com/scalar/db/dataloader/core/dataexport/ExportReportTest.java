package com.scalar.db.dataloader.core.dataexport;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ExportReportTest {

  @Test
  void getExportedRowCount_afterInitialisation_ShouldBeZero() {
    ExportReport exportReport = new ExportReport();
    Assertions.assertEquals(0, exportReport.getExportedRowCount());
  }

  @Test
  void getExportedRowCount_afterIncrementingTwice_ShouldBeTwo() {
    ExportReport exportReport = new ExportReport();
    exportReport.updateExportedRowCount(10);
    exportReport.updateExportedRowCount(20);
    Assertions.assertEquals(30, exportReport.getExportedRowCount());
  }
}
