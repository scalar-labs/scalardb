package com.scalar.db.schemaloader.core;

import com.scalar.db.config.DatabaseConfig;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

public class SchemaOperatorFactory {
  /**
   * Creates a SchemaOperator instance.
   *
   * @param properties the Scalar DB properties configuration.
   * @param separateCoordinatorTable separate creating/deleting coordinator table or not. If this
   *     param is `true` then for example when creating a schema which contains a transactional
   *     table, the coordinator will be created as well.
   * @return {@link SchemaOperator} instance.
   */
  public static SchemaOperator getSchemaOperator(
      Properties properties, boolean separateCoordinatorTable) {
    DatabaseConfig databaseConfig = new DatabaseConfig(properties);
    return new SchemaOperator(databaseConfig, separateCoordinatorTable);
  }

  /**
   * Creates a SchemaOperator instance.
   *
   * @param configPath the file path to Scalar DB configuration file.
   * @param separateCoordinatorTable separate creating/deleting coordinator table or not. If this
   *     param is `true` then for example when creating a schema which contains a transactional
   *     table, the coordinator will be created as well.
   * @return {@link SchemaOperator} instance.
   * @throws SchemaOperatorException thrown when getting {@link SchemaOperator} instance failed.
   */
  public static SchemaOperator getSchemaOperator(Path configPath, boolean separateCoordinatorTable)
      throws SchemaOperatorException {
    DatabaseConfig dbConfig;
    try {
      dbConfig = new DatabaseConfig(new FileInputStream(configPath.toString()));
    } catch (IOException e) {
      throw new SchemaOperatorException("Reading config file failed", e);
    }
    return new SchemaOperator(dbConfig, separateCoordinatorTable);
  }
}
