package com.scalar.db.schemaloader.dynamo;

import com.google.common.collect.ImmutableList;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import com.scalar.db.storage.dynamo.DynamoConfig;
import com.scalar.db.storage.dynamo.DynamoEnv;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.junit.Ignore;

public class DynamoSchemaLoaderWithStorageSpecificArgsIntegrationTest
    extends SchemaLoaderIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return DynamoEnv.getDynamoConfig();
  }

  @Override
  protected List<String> getCommandArgsForCreationWithCoordinatorTable(
      String configFile, String schemaFile) throws IOException {
    DynamoConfig config = new DynamoConfig(new File(configFile));
    return ImmutableList.of(
        "--dynamo",
        "--region",
        config.getContactPoints().get(0),
        "--schema-file",
        schemaFile,
        "-u",
        config.getUsername().get(),
        "-p",
        config.getPassword().get(),
        "--endpoint-override",
        config.getEndpointOverride().get(),
        "--no-scaling",
        "--no-backup");
  }

  @Ignore
  @Override
  public void createTablesThenDeleteTables_ShouldExecuteProperly() {}
}
