package com.scalar.db.schemaloader.dynamo;

import com.google.common.collect.ImmutableList;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import com.scalar.db.storage.dynamo.DynamoEnv;
import java.util.List;

public class DynamoSchemaLoaderIntegrationTest extends SchemaLoaderIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return DynamoEnv.getDynamoConfig();
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
}
