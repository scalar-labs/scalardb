package com.scalar.db.schemaloader.dynamo;

import com.google.common.collect.ImmutableList;
import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import com.scalar.db.storage.dynamo.DynamoConfig;
import com.scalar.db.storage.dynamo.DynamoEnv;
import java.util.List;

public class DynamoSchemaLoaderIntegrationTest extends SchemaLoaderIntegrationTestBase {
  private static final DynamoConfig dynamoConfig = DynamoEnv.getDynamoConfig();

  @Override
  protected void initialize() {
    config = dynamoConfig;
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
        dynamoConfig.getEndpointOverride().get(),
        "--no-scaling",
        "--no-backup");
  }
}
