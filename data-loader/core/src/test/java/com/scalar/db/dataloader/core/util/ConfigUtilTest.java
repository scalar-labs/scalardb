package com.scalar.db.dataloader.core.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Field;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConfigUtilTest {

  private Properties mockProperties;

  @BeforeEach
  void setUp() throws Exception {
    mockProperties = new Properties();
    setPropertiesField(mockProperties);
  }

  @AfterEach
  void tearDown() throws Exception {
    setPropertiesField(null);
  }

  private void setPropertiesField(Properties properties) throws Exception {
    Field field = ConfigUtil.class.getDeclaredField("properties");
    field.setAccessible(true);
    field.set(null, properties);
  }

  @Test
  void testGetImportDataChunkQueueSize_WithValidValue() {
    mockProperties.setProperty("import.data.chunk.queue.size", "512");
    assertEquals(512, ConfigUtil.getImportDataChunkQueueSize());
  }

  @Test
  void testGetImportDataChunkQueueSize_WithNoValue_UsesDefault() {
    assertEquals(256, ConfigUtil.getImportDataChunkQueueSize());
  }

  @Test
  void testGetImportDataChunkQueueSize_WithInvalidValue_ThrowsException() {
    mockProperties.setProperty("import.data.chunk.queue.size", "invalid");
    assertThrows(IllegalArgumentException.class, ConfigUtil::getImportDataChunkQueueSize);
  }

  @Test
  void testGetTransactionBatchThreadPoolSize_WithValidValue() {
    mockProperties.setProperty("transaction.batch.thread.pool.size", "32");
    assertEquals(32, ConfigUtil.getTransactionBatchThreadPoolSize());
  }

  @Test
  void testGetTransactionBatchThreadPoolSize_WithNoValue_UsesDefault() {
    assertEquals(16, ConfigUtil.getTransactionBatchThreadPoolSize());
  }

  @Test
  void testGetTransactionBatchThreadPoolSize_WithInvalidValue_ThrowsException() {
    mockProperties.setProperty("transaction.batch.thread.pool.size", "invalid");
    assertThrows(IllegalArgumentException.class, ConfigUtil::getTransactionBatchThreadPoolSize);
  }
}
