package com.scalar.db.storage.dynamo;

import com.google.common.collect.ImmutableList;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;

public class DynamoSchemaLoaderWithStorageSpecificArgsIntegrationTest
    extends SchemaLoaderIntegrationTestBase {

  @Override
  protected Properties getProperties() {
    return DynamoEnv.getProperties();
  }

  @Override
  protected List<String> getCommandArgsForCreationWithCoordinator(
      String configFile, String schemaFile) throws IOException {
    DynamoConfig config = new DynamoConfig(new DatabaseConfig(new File(configFile)));
    return ImmutableList.of(
        "--dynamo",
        "--region",
        config.getRegion(),
        "--schema-file",
        schemaFile,
        "-u",
        config.getAccessKeyId(),
        "-p",
        config.getSecretAccessKey(),
        "--endpoint-override",
        config.getEndpointOverride().get(),
        "--no-scaling",
        "--no-backup");
  }

  @Disabled
  @Override
  public void createTablesThenDeleteTables_ShouldExecuteProperly() {}
}
