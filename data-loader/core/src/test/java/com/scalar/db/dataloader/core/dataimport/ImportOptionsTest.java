package com.scalar.db.dataloader.core.dataimport;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/** Unit tests for ImportOptions builder and default values. */
public class ImportOptionsTest {

  @Test
  void importOptions_withoutMaxThreads_shouldUseDefaultAvailableProcessors() {
    // Create ImportOptions without explicitly setting maxThreads
    ImportOptions importOptions =
        ImportOptions.builder()
            .namespace("test_namespace")
            .tableName("test_table")
            .dataChunkSize(100)
            .transactionBatchSize(10)
            .dataChunkQueueSize(64)
            .build();

    // Verify the default was applied
    assertEquals(Runtime.getRuntime().availableProcessors(), importOptions.getThreadCount());
  }

  @Test
  void importOptions_withExplicitMaxThreads_shouldUseProvidedValue() {
    // Create ImportOptions with explicit maxThreads
    ImportOptions importOptions =
        ImportOptions.builder()
            .namespace("test_namespace")
            .tableName("test_table")
            .dataChunkSize(100)
            .transactionBatchSize(10)
            .threadCount(8)
            .dataChunkQueueSize(64)
            .build();

    // Verify the explicit value was used
    assertEquals(8, importOptions.getThreadCount());
  }
}
