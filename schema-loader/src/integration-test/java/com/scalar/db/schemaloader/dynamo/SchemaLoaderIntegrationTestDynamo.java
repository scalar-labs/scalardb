package com.scalar.db.schemaloader.dynamo;

import com.google.common.collect.ImmutableList;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import com.scalar.db.storage.dynamo.DynamoConfig;
import com.scalar.db.storage.dynamo.DynamoEnv;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Properties;
import org.junit.BeforeClass;

public class SchemaLoaderIntegrationTestDynamo extends SchemaLoaderIntegrationTestBase {
  private static final DynamoConfig config = DynamoEnv.getDynamoConfig();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Properties properties = config.getProperties();
    try (final FileOutputStream fileOutputStream = new FileOutputStream(CONFIG_FILE)) {
      properties.store(fileOutputStream, null);
    }
  }

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return config;
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
        "--no-scaling",
        "--no-backup");
  }

  @Override
  protected List<String> getStorageSpecificCreationCommandArgs() {
    return ImmutableList.of(
        "java",
        "-jar",
        "scalardb-schema-loader.jar",
        "--dynamo",
        "--region",
        config.getContactPoints().get(0),
        "--schema-file",
        SCHEMA_FILE,
        "-u",
        config.getUsername().get(),
        "-p",
        config.getPassword().get(),
        "--endpoint-override",
        config.getEndpointOverride().get(),
        "--no-scaling",
        "--no-backup");
  }
}
