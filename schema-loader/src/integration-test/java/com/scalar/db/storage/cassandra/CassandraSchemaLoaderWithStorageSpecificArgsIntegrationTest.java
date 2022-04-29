package com.scalar.db.storage.cassandra;

import com.google.common.collect.ImmutableList;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Disabled;

public class CassandraSchemaLoaderWithStorageSpecificArgsIntegrationTest
    extends SchemaLoaderIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return CassandraEnv.getDatabaseConfig();
  }

  @Override
  protected List<String> getCommandArgsForCreationWithCoordinator(
      String configFile, String schemaFile) throws IOException {
    DatabaseConfig config = new DatabaseConfig(new File(configFile));
    return ImmutableList.of(
        "--cassandra",
        "-h",
        String.valueOf(config.getContactPoints().get(0)),
        "--schema-file",
        schemaFile,
        "-u",
        config.getUsername().get(),
        "-p",
        config.getPassword().get());
  }

  @Disabled
  @Override
  public void createTablesThenDeleteTables_ShouldExecuteProperly() {}
}
