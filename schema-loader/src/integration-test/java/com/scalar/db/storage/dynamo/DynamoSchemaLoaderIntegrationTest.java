package com.scalar.db.storage.dynamo;

import com.google.common.collect.ImmutableList;
import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import java.util.List;
import java.util.Properties;

public class DynamoSchemaLoaderIntegrationTest extends SchemaLoaderIntegrationTestBase {

  @Override
  protected Properties getProperties() {
    return DynamoEnv.getProperties();
  }

  @Override
  protected List<String> getCommandArgsForCreation(String configFile, String schemaFile)
      throws Exception {
    return ImmutableList.<String>builder()
        .addAll(super.getCommandArgsForCreation(configFile, schemaFile))
        .add("--no-scaling")
        .add("--no-backup")
        .build();
  }

  @Override
  protected List<String> getCommandArgsForTableReparation(String configFile, String schemaFile) {
    return ImmutableList.<String>builder()
        .addAll(super.getCommandArgsForTableReparation(configFile, schemaFile))
        .add("--no-backup")
        .build();
  }
}
