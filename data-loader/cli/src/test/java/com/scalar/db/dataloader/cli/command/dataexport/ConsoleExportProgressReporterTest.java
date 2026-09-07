package com.scalar.db.dataloader.cli.command.dataexport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConsoleExportProgressReporterTest {

  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;

  @BeforeEach
  void setUpStreams() throws UnsupportedEncodingException {
    System.setOut(new PrintStream(outContent, true, "UTF-8"));
  }

  @AfterEach
  void restoreStreams() {
    System.setOut(originalOut);
  }

  @Test
  void testStartMessageIncludesExportFilePath() throws UnsupportedEncodingException {
    new ConsoleExportProgressReporter("output/test.csv");
    String output = outContent.toString("UTF-8");
    assertTrue(output.contains("📤 Starting export"), "Expected start message");
    assertTrue(
        output.contains("📁 Exporting data to file: output/test.csv"), "Expected file path info");
  }

  @Test
  void testCompletionMessageIncludesFilePathAndDuration()
      throws InterruptedException, UnsupportedEncodingException {
    ConsoleExportProgressReporter reporter = new ConsoleExportProgressReporter("target/output.csv");

    Thread.sleep(100); // Simulate work

    reporter.reportCompletion(12345);
    String output = outContent.toString("UTF-8");

    assertTrue(
        output.contains("✅ Export completed: 12,345 records exported to target/output.csv"),
        "Expected completion message");
    assertTrue(output.matches("(?s).*in \\d+m \\d+s.*"), "Expected duration format");
  }

  @Test
  void testCompletionOnlyPrintedOnce() throws UnsupportedEncodingException {
    ConsoleExportProgressReporter reporter = new ConsoleExportProgressReporter("target/output.csv");

    reporter.reportCompletion(100);
    reporter.reportCompletion(999999); // Should be ignored

    String output = outContent.toString("UTF-8");
    int count = output.split("Export completed").length - 1;
    assertEquals(1, count, "Expected completion to be printed only once");
  }

  @Test
  void testReportError_shouldPrintErrorMessageWithExceptionMessage()
      throws UnsupportedEncodingException {
    ByteArrayOutputStream errContent = new ByteArrayOutputStream();
    PrintStream originalErr = System.err;
    System.setErr(new PrintStream(errContent, true, "UTF-8"));

    try {
      String errorMessage = "Something went wrong";
      Throwable cause = new RuntimeException("Test exception");

      ConsoleExportProgressReporter.reportError(errorMessage, cause);

      String output = errContent.toString("UTF-8");
      assertTrue(
          output.contains("❌ Export failed: " + errorMessage), "Expected main error message");
      assertTrue(output.contains("Cause: " + cause.getMessage()), "Expected exception message");
    } finally {
      System.setErr(originalErr);
    }
  }

  @Test
  void testReportError_shouldPrintMessageWithoutExceptionWhenNull()
      throws UnsupportedEncodingException {
    ByteArrayOutputStream errContent = new ByteArrayOutputStream();
    PrintStream originalErr = System.err;
    System.setErr(new PrintStream(errContent, true, "UTF-8"));

    try {
      String errorMessage = "Directory not found";

      ConsoleExportProgressReporter.reportError(errorMessage, null);

      String output = errContent.toString("UTF-8");
      assertTrue(output.contains("❌ Export failed: " + errorMessage), "Expected error message");
      assertFalse(
          output.contains("Cause:"), "Should not print exception cause when throwable is null");
    } finally {
      System.setErr(originalErr);
    }
  }

  @Test
  void testReportWarning_shouldPrintFormattedWarningMessage() throws UnsupportedEncodingException {

    ByteArrayOutputStream errContent = new ByteArrayOutputStream();
    PrintStream originalErr = System.err;
    System.setErr(new PrintStream(errContent, true, "UTF-8"));

    try {
      String warningMessage = "Deprecated option detected";

      ConsoleExportProgressReporter.reportWarning(warningMessage);

      String output = errContent.toString("UTF-8");

      // Expected core formatted message
      assertTrue(
          output.contains("⚠️  Warning: " + warningMessage), "Expected formatted warning message");

      // Should start with a newline produced by %n
      assertTrue(
          output.startsWith(System.lineSeparator()), "Expected output to start with a newline");

      // Should end with a newline from the trailing %n
      assertTrue(output.endsWith(System.lineSeparator()), "Expected output to end with a newline");
    } finally {
      System.setErr(originalErr);
    }
  }

  @Test
  void testReportWarning_shouldNotIncludeErrorSpecificFields() throws UnsupportedEncodingException {

    ByteArrayOutputStream errContent = new ByteArrayOutputStream();
    PrintStream originalErr = System.err;
    System.setErr(new PrintStream(errContent, true, "UTF-8"));

    try {
      ConsoleExportProgressReporter.reportWarning("Check your input");

      String output = errContent.toString("UTF-8");

      assertFalse(output.contains("Cause:"), "Warning output must not include cause");
      assertFalse(output.contains("❌"), "Warning output must not include error symbol");
    } finally {
      System.setErr(originalErr);
    }
  }
}
