package com.scalar.db.dataloader.cli.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

import com.scalar.db.common.error.CoreError;
import com.scalar.db.config.DatabaseConfig;
import java.io.File;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

public class ScalarDbUtilTest {

  @Test
  void returnsTrueWhenClusterIsSet() throws IOException {
    File dummyFile = new File("dummy.conf");

    try (MockedConstruction<DatabaseConfig> mocked =
        mockConstruction(
            DatabaseConfig.class,
            (mock, context) -> {
              when(mock.getTransactionManager()).thenReturn("cluster");
            })) {
      boolean result = ScalarDbUtil.isScalarDBClusterEnabled(dummyFile);
      assertTrue(result);
    }
  }

  @Test
  void returnsFalseWhenTransactionManagerIsNotCluster() throws IOException {
    File dummyFile = new File("dummy.conf");

    try (MockedConstruction<DatabaseConfig> mocked =
        mockConstruction(
            DatabaseConfig.class,
            (mock, context) -> {
              when(mock.getTransactionManager()).thenReturn("jdbc");
            })) {
      boolean result = ScalarDbUtil.isScalarDBClusterEnabled(dummyFile);
      assertFalse(result);
    }
  }

  @Test
  void returnsFalseWhenTransactionManagerIsNull() throws IOException {
    File dummyFile = new File("dummy.conf");

    try (MockedConstruction<DatabaseConfig> mocked =
        mockConstruction(
            DatabaseConfig.class,
            (mock, context) -> {
              when(mock.getTransactionManager()).thenReturn(null);
            })) {
      boolean result = ScalarDbUtil.isScalarDBClusterEnabled(dummyFile);
      assertFalse(result);
    }
  }

  @Test
  void throwsIllegalArgumentExceptionWhenFileIsNull() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class, () -> ScalarDbUtil.isScalarDBClusterEnabled(null));
    assertEquals(CoreError.DATA_LOADER_INVALID_CONFIG_FILE.buildMessage(), ex.getMessage());
  }
}
