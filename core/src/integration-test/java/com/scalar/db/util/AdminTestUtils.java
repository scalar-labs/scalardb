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
        return new CassandraAdminTestUtils();
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

  /**
   * Modify the metadata the table in a way that it will fail if the metadata are parsed
   *
   * @param namespace a namespace
   * @param table a table
   * @throws Exception if an error occurs
   */
  public abstract void corruptMetadata(String namespace, String table) throws Exception;
}
