package com.scalar.db.schemaloader.cosmos;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import com.scalar.db.schemaloader.core.SchemaOperator;
import com.scalar.db.schemaloader.schema.SchemaParser;
import com.scalar.db.storage.cosmos.CosmosConfig;
import com.scalar.db.storage.cosmos.CosmosEnv;
import java.io.FileOutputStream;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

public class CosmosSchemaLoaderIntegrationTest extends SchemaLoaderIntegrationTestBase {
  private static final CosmosConfig config = CosmosEnv.getCosmosConfig();

  @Override
  protected void initialize() throws Exception {
    Properties properties = config.getProperties();
    try (final FileOutputStream fileOutputStream = new FileOutputStream(CONFIG_FILE)) {
      properties.store(fileOutputStream, null);
    }
  }

  @Override
  protected void parsingTables() throws Exception {
    tables =
        SchemaParser.parse(
            Paths.get(SCHEMA_FILE),
            ImmutableMap.of(SchemaOperator.NAMESPACE_PREFIX, CosmosEnv.getDatabasePrefix().get()));
  }

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return config;
  }

  @Override
  protected List<String> getStorageSpecificCreationCommandArgs() {
    return ImmutableList.of(
        "java",
        "-jar",
        "scalardb-schema-loader.jar",
        "--cosmos",
        "-h",
        config.getContactPoints().get(0),
        "--schema-file",
        SCHEMA_FILE,
        "-p",
        config.getPassword().get(),
        "--prefix",
        CosmosEnv.getDatabasePrefix().get());
  }

  @Override
  protected List<String> getSchemaLoaderCreationCommandArgs() {
    return ImmutableList.of(
        "java",
        "-jar",
        "scalardb-schema-loader.jar",
        "--config",
        CONFIG_FILE,
        "--schema-file",
        SCHEMA_FILE,
        "--coordinator",
        "--prefix",
        CosmosEnv.getDatabasePrefix().get());
  }
}
