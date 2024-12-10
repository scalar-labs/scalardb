package com.scalar.db.dataloader.core.dataexport;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExportReportTest {

    @Test
    void getExportedRowCount_afterInitialisation_ShouldBeZero(){
        ExportReport exportReport = new ExportReport();
        Assertions.assertEquals(0, exportReport.getExportedRowCount().get());
    }

    @Test
    void getExportedRowCount_afterIncrementingTwice_ShouldBeTwo(){
        ExportReport exportReport = new ExportReport();
        exportReport.increaseExportedRowCount();
        exportReport.increaseExportedRowCount();
        Assertions.assertEquals(2, exportReport.getExportedRowCount().get());
    }
}
