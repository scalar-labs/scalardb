package com.scalar.db.storage.dynamo;

import com.google.common.collect.ImmutableList;
import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

public class DynamoSchemaLoaderIntegrationTest extends SchemaLoaderIntegrationTestBase {

  @Override
  protected Properties getProperties() {
    return DynamoEnv.getProperties();
  }

  @Override
  protected List<String> getCommandArgsForCreation(Path configFilePath, Path schemaFilePath)
      throws Exception {
    return ImmutableList.<String>builder()
        .addAll(super.getCommandArgsForCreation(configFilePath, schemaFilePath))
        .add("--no-scaling")
        .add("--no-backup")
        .build();
  }

  @Override
  protected List<String> getCommandArgsForTableReparation(
      Path configFilePath, Path schemaFilePath) {
    return ImmutableList.<String>builder()
        .addAll(super.getCommandArgsForTableReparation(configFilePath, schemaFilePath))
        .add("--no-backup")
        .build();
  }
}
