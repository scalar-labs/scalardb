package com.scalar.db.dataloader.core.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class for debugging purposes, providing methods to log runtime memory usage.
 *
 * <p>This class is typically used to log memory usage at various stages of an application's
 * execution to help diagnose memory-related issues or understand memory consumption patterns.
 */
public class DebugUtil {

  private static final Logger logger = LoggerFactory.getLogger(DebugUtil.class);

  /**
   * log memory usage
   *
   * @param stage stage of process
   */
  public static void logMemoryUsage(String stage) {
    Runtime runtime = Runtime.getRuntime();
    long usedMemory = runtime.totalMemory() - runtime.freeMemory();
    long maxMemory = runtime.maxMemory();

    logger.info(
        "Memory usage at {}: Used Memory = {} MB, Max Memory = {} MB",
        stage,
        formatMemorySize(usedMemory),
        formatMemorySize(maxMemory));
  }

  /**
   * Converts the given memory size in bytes to a human-readable string in megabytes.
   *
   * @param size the memory size in bytes
   * @return the formatted memory size in megabytes, rounded to two decimal places
   */
  private static String formatMemorySize(long size) {
    return String.format("%.2f", size / (1024.0 * 1024.0));
  }
}
