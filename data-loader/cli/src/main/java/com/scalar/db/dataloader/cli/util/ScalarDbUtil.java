package com.scalar.db.dataloader.cli.util;

import com.scalar.db.common.error.CoreError;
import com.scalar.db.config.DatabaseConfig;
import java.io.File;
import java.io.IOException;

public class ScalarDbUtil {

  /**
   * Checks whether ScalarDB is configured to use a cluster-based transaction manager.
   *
   * <p>This method reads the provided ScalarDB configuration file and determines if the transaction
   * manager is set to "cluster". It performs a null-safe comparison to avoid potential {@code
   * NullPointerException}s if the configuration is incomplete.
   *
   * @param configFile the ScalarDB configuration file to read
   * @return {@code true} if the transaction manager is configured as "cluster"; {@code false}
   *     otherwise
   * @throws IllegalArgumentException if the provided config file is {@code null}
   * @throws IOException if an I/O error occurs while reading the configuration file
   */
  public static boolean isScalarDBClusterEnabled(File configFile) throws IOException {
    if (configFile == null) {
      throw new IllegalArgumentException(CoreError.DATA_LOADER_INVALID_CONFIG_FILE.buildMessage());
    }
    DatabaseConfig config = new DatabaseConfig(configFile);
    return "cluster".equals(config.getTransactionManager());
  }
}
