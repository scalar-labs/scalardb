package com.scalar.db.dataloader.cli.command.dataexport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConsoleExportProgressReporterTest {

  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;

  @BeforeEach
  void setUpStreams() {
    System.setOut(new PrintStream(outContent));
  }

  @AfterEach
  void restoreStreams() {
    System.setOut(originalOut);
  }

  @Test
  void testStartMessageIncludesExportFilePath() {
    new ConsoleExportProgressReporter("output/test.csv");
    String output = outContent.toString();
    assertTrue(output.contains("📤 Starting export"), "Expected start message");
    assertTrue(
        output.contains("📁 Exporting data to file: output/test.csv"), "Expected file path info");
  }

  @Test
  void testCompletionMessageIncludesFilePathAndDuration() throws InterruptedException {
    ConsoleExportProgressReporter reporter = new ConsoleExportProgressReporter("target/output.csv");

    Thread.sleep(100); // Simulate work

    reporter.reportCompletion(12345);
    String output = outContent.toString();

    assertTrue(
        output.contains("✅ Export completed: 12,345 records exported to target/output.csv"),
        "Expected completion message");
    assertTrue(output.matches("(?s).*in \\d+m \\d+s.*"), "Expected duration format");
  }

  @Test
  void testCompletionOnlyPrintedOnce() {
    ConsoleExportProgressReporter reporter = new ConsoleExportProgressReporter("target/output.csv");

    reporter.reportCompletion(100);
    reporter.reportCompletion(999999); // Should be ignored

    String output = outContent.toString();
    int count = output.split("Export completed").length - 1;
    assertEquals(1, count, "Expected completion to be printed only once");
  }

  @Test
  void testReportError_shouldPrintErrorMessageWithExceptionMessage() {
    ByteArrayOutputStream errContent = new ByteArrayOutputStream();
    PrintStream originalErr = System.err;
    System.setErr(new PrintStream(errContent));

    try {
      String errorMessage = "Something went wrong";
      Throwable cause = new RuntimeException("Test exception");

      ConsoleExportProgressReporter.reportError(errorMessage, cause);

      String output = errContent.toString();
      assertTrue(
          output.contains("❌ Export failed: " + errorMessage), "Expected main error message");
      assertTrue(output.contains("Cause: " + cause.getMessage()), "Expected exception message");
    } finally {
      System.setErr(originalErr);
    }
  }

  @Test
  void testReportError_shouldPrintMessageWithoutExceptionWhenNull() {
    ByteArrayOutputStream errContent = new ByteArrayOutputStream();
    PrintStream originalErr = System.err;
    System.setErr(new PrintStream(errContent));

    try {
      String errorMessage = "Directory not found";

      ConsoleExportProgressReporter.reportError(errorMessage, null);

      String output = errContent.toString();
      assertTrue(output.contains("❌ Export failed: " + errorMessage), "Expected error message");
      assertFalse(
          output.contains("Cause:"), "Should not print exception cause when throwable is null");
    } finally {
      System.setErr(originalErr);
    }
  }
}
