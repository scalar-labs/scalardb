package com.scalar.db.dataloader.cli.util;

import com.scalar.db.config.DatabaseConfig;

public class ScalarDbModeCheckUtil {

  public static boolean checkCluster(DatabaseConfig config) {
    return config.getTransactionManager().equals("cluster");
  }
}
