package com.scalar.db.dataloader.core.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Utility class for loading and retrieving configuration properties.
 *
 * <p>This class reads properties from a {@code config.properties} file located in the classpath.
 */
public class ConfigUtil {
  public static final String CONFIG_PROPERTIES = "config.properties";
  private static volatile Properties properties;

  /**
   * Ensures that the configuration properties are loaded. If properties are not yet loaded, it
   * loads them in a thread-safe manner.
   */
  private static void ensurePropertiesLoaded() {
    if (properties == null) {
      synchronized (ConfigUtil.class) {
        if (properties == null) {
          loadProperties();
        }
      }
    }
  }

  /**
   * Loads the configuration properties from the {@code config.properties} file in the classpath.
   *
   * <p>If the file is missing or cannot be read, a {@link RuntimeException} is thrown.
   *
   * @throws RuntimeException if the properties file is not found or cannot be loaded
   */
  private static void loadProperties() {
    try (InputStream inputStream =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(CONFIG_PROPERTIES)) {
      if (inputStream == null) {
        throw new RuntimeException("config.properties file not found in classpath.");
      }
      properties = new Properties();
      properties.load(inputStream);
    } catch (IOException e) {
      throw new RuntimeException("Failed to load config.properties", e);
    }
  }

  /**
   * Retrieves the queue size for importing data chunks.
   *
   * <p>This method reads the property {@code import.data.chunk.queue.size}. If the property is
   * missing or invalid, the default value {@code 256} is returned.
   *
   * @return the configured queue size for data chunks, or {@code 256} if not specified
   */
  public static Integer getImportDataChunkQueueSize() {
    ensurePropertiesLoaded();
    return getIntegerProperty("import.data.chunk.queue.size", 256);
  }

  /**
   * Retrieves the thread pool size for processing transaction batches.
   *
   * <p>This method reads the property {@code transaction.batch.thread.pool.size}. If the property
   * is missing or invalid, the default value {@code 16} is returned.
   *
   * @return the configured thread pool size for transaction batches, or {@code 16} if not specified
   */
  public static Integer getTransactionBatchThreadPoolSize() {
    ensurePropertiesLoaded();
    return getIntegerProperty("transaction.batch.thread.pool.size", 16);
  }

  /**
   * Retrieves an integer property value from the loaded properties.
   *
   * <p>If the property is not found, the default value is returned. If the property is present but
   * not a valid integer, an {@link IllegalArgumentException} is thrown.
   *
   * @param key the property key to look up
   * @param defaultValue the default value to return if the property is missing or empty
   * @return the integer value of the property, or the default value if not specified
   * @throws IllegalArgumentException if the property value is not a valid integer
   */
  private static Integer getIntegerProperty(String key, int defaultValue) {
    String value = properties.getProperty(key);
    if (value == null || value.trim().isEmpty()) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid integer value for property: " + key, e);
    }
  }
}
