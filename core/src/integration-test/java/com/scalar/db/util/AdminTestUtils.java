package com.scalar.db.util;

import com.scalar.db.config.DatabaseConfig;
import java.util.Properties;

/** Utility class used to delete or truncate the metadata table */
public abstract class AdminTestUtils {

  String metadataNamespace;
  String metadataTable;

  AdminTestUtils() {}

  public static AdminTestUtils create(Properties properties) {
    String storage = properties.getProperty(DatabaseConfig.STORAGE, "");
    switch (storage) {
      case "cosmos":
        return new CosmosAdminTestUtils(properties);
      case "dynamo":
        return new DynamoAdminTestUtils(properties);
      case "jdbc":
        return new JdbcAdminTestUtils(properties);
      default:
        return null;
    }
  }

  /**
   * Delete the metadata table
   *
   * @throws Exception if an error occurs
   */
  public abstract void dropMetadataTable() throws Exception;

  /**
   * Truncate the metadata table
   *
   * @throws Exception if an error occurs
   */
  public abstract void truncateMetadataTable() throws Exception;
}
