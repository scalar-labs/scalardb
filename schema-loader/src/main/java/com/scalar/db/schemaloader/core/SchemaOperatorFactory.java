package com.scalar.db.schemaloader.core;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdmin;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class SchemaOperatorFactory {
  /**
   * Creates a SchemaOperator instance.
   *
   * @param properties the Scalar DB properties configuration
   * @param separateCoordinatorTable separate creating/deleting coordinator table or not. If this
   *     param is `true` then for example when creating a schema which contains a transactional
   *     table, the coordinator will be created as well.
   */
  public static SchemaOperator getSchemaOperator(
      Properties properties, boolean separateCoordinatorTable) {
    DatabaseConfig databaseConfig = new DatabaseConfig(properties);
    return new SchemaOperator(databaseConfig, separateCoordinatorTable);
  }

  /**
   * Creates a SchemaOperator instance.
   *
   * @param configPath the file path to Scalar DB configuration file
   * @param separateCoordinatorTable separate creating/deleting coordinator table or not. If this
   *     param is `true` then for example when creating a schema which contains a transactional
   *     table, the coordinator will be created as well.
   */
  public static SchemaOperator getSchemaOperator(
      String configPath, boolean separateCoordinatorTable) throws IOException {
    DatabaseConfig dbConfig = new DatabaseConfig(new FileInputStream(configPath));
    return new SchemaOperator(dbConfig, separateCoordinatorTable);
  }

  /**
   * Creates a SchemaOperator instance.
   *
   * @param dbConfig the Scalar DB configuration, instance of {@link DatabaseConfig}
   * @param separateCoordinatorTable separate creating/deleting coordinator table or not. If this
   *     param is `true` then for example when creating a schema which contains a transactional
   *     table, the coordinator will be created as well.
   */
  public static SchemaOperator getSchemaOperator(
      DatabaseConfig dbConfig, boolean separateCoordinatorTable) {
    return new SchemaOperator(dbConfig, separateCoordinatorTable);
  }

  /**
   * Creates a SchemaOperator instance.
   *
   * @param admin the instance of {@link DistributedStorageAdmin}
   * @param consensusCommitAdmin the instance of {@link ConsensusCommitAdmin}
   * @param separateCoordinatorTable separate creating/deleting coordinator table or not. If this
   *     param is `true` then for example when creating a schema which contains a transactional
   *     table, the coordinator will be created as well.
   */
  public static SchemaOperator getSchemaOperator(
      DistributedStorageAdmin admin,
      ConsensusCommitAdmin consensusCommitAdmin,
      boolean separateCoordinatorTable) {
    return new SchemaOperator(admin, consensusCommitAdmin, separateCoordinatorTable);
  }
}
