package com.scalar.db.storage.jdbc;

import com.google.common.collect.ImmutableList;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;

public class JdbcSchemaLoaderWithStorageSpecificArgsIntegrationTest
    extends SchemaLoaderIntegrationTestBase {

  @Override
  protected Properties getProperties() {
    return JdbcEnv.getProperties();
  }

  @Override
  protected List<String> getCommandArgsForCreationWithCoordinator(
      String configFile, String schemaFile) throws IOException {
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(new File(configFile)));
    return ImmutableList.of(
        "--jdbc",
        "-j",
        config.getJdbcUrl(),
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
