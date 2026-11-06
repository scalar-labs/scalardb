package com.scalar.db.dataloader.cli.command.dataexport;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A simple utility class to report export progress to the console.
 *
 * <p>This class is intended to be used in scenarios where there is no event-driven listener for
 * export progress, but feedback to the user is still valuable.
 *
 * <p>It displays:
 *
 * <ul>
 *   <li>A starting message when export begins
 *   <li>A completion message with total records exported and time taken
 *   <li>Error messages in case of failures (via {@link #reportError(String, Throwable)})
 * </ul>
 */
public class ConsoleExportProgressReporter {

  private final long startTime;
  private final AtomicBoolean completed = new AtomicBoolean(false);
  private final String outputFile;

  /**
   * Constructs a reporter and logs the export start.
   *
   * @param outputFile the file to which data will be exported
   */
  public ConsoleExportProgressReporter(String outputFile) {
    this.outputFile = outputFile;
    this.startTime = System.currentTimeMillis();
    System.out.println("📤 Starting export...");
    System.out.println("📁 Exporting data to file: " + outputFile);
  }

  /**
   * Reports the completion of the export process, including total records exported and time taken.
   *
   * @param totalExported the total number of records exported
   */
  public void reportCompletion(long totalExported) {
    if (completed.getAndSet(true)) {
      return;
    }
    long elapsed = System.currentTimeMillis() - startTime;
    System.out.printf(
        "%n✅ Export completed: %,d records exported to %s in %s%n",
        totalExported, outputFile, formatElapsed(elapsed));
  }

  /**
   * Prints a formatted error message to the console.
   *
   * @param message the error description
   * @param throwable the associated exception (can be null)
   */
  public static void reportError(String message, Throwable throwable) {
    System.err.println("%n❌ Export failed: " + message);
    if (throwable != null) {
      System.err.println("Cause: " + throwable.getMessage());
    }
  }

  /**
   * Prints a formatted waring message to the console.
   *
   * @param message the error description
   */
  public static void reportWarning(String message) {
    System.err.printf("%n⚠️  Warning: %s%n", message);
  }

  /**
   * Formats elapsed time in "Xm Ys" format.
   *
   * @param elapsedMillis the elapsed time in milliseconds
   * @return a human-readable string of the elapsed time
   */
  private String formatElapsed(long elapsedMillis) {
    long seconds = (elapsedMillis / 1000) % 60;
    long minutes = (elapsedMillis / 1000) / 60;
    return String.format("%dm %ds", minutes, seconds);
  }
}
