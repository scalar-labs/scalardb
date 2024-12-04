package com.scalar.db.dataloader.core.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebugUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(DebugUtil.class);

  /**
   * log memory usage
   *
   * @param stage stage of process
   */
  public static void logMemoryUsage(String stage) {
    Runtime runtime = Runtime.getRuntime();
    long usedMemory = runtime.totalMemory() - runtime.freeMemory();
    long maxMemory = runtime.maxMemory();

    LOGGER.info(
        "Memory usage at {}: Used Memory = {} MB, Max Memory = {} MB",
        stage,
        formatMemorySize(usedMemory),
        formatMemorySize(maxMemory));
  }

  private static String formatMemorySize(long size) {
    return String.format("%.2f", size / (1024.0 * 1024.0));
  }
}
