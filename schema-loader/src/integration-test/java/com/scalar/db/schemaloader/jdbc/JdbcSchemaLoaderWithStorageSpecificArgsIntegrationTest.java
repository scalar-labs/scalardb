package com.scalar.db.schemaloader.jdbc;

import com.google.common.collect.ImmutableList;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import com.scalar.db.storage.jdbc.JdbcEnv;
import java.io.File;
import java.io.IOException;
import java.util.List;

public class JdbcSchemaLoaderWithStorageSpecificArgsIntegrationTest
    extends SchemaLoaderIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return JdbcEnv.getJdbcConfig();
  }

  @Override
  protected List<String> getCommandArgsForCreation(String configFile, String schemaFile)
      throws IOException {
    DatabaseConfig config = new DatabaseConfig(new File(configFile));
    return ImmutableList.of(
        "--jdbc",
        "-j",
        config.getContactPoints().get(0),
        "--schema-file",
        schemaFile,
        "-u",
        config.getUsername().get(),
        "-p",
        config.getPassword().get());
  }
}
