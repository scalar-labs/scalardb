package com.scalar.db.schemaloader.cosmos;

import com.google.common.collect.ImmutableList;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import com.scalar.db.storage.cosmos.CosmosConfig;
import com.scalar.db.storage.cosmos.CosmosEnv;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Properties;
import org.junit.BeforeClass;

public class SchemaLoaderIntegrationTestCosmos extends SchemaLoaderIntegrationTestBase {
  private static final CosmosConfig config = CosmosEnv.getCosmosConfig();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Properties properties = config.getProperties();
    properties.store(new FileOutputStream(CONFIG_FILE), null);
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
        config.getPassword().get());
  }
}
