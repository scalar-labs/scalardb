package com.scalar.db.schemaloader.jdbc;

import com.google.common.collect.ImmutableList;
import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import com.scalar.db.storage.jdbc.JdbcEnv;
import java.util.List;

public class JdbcSchemaLoaderIntegrationTest extends SchemaLoaderIntegrationTestBase {

  @Override
  protected void initialize() {
    config = JdbcEnv.getJdbcConfig();
  }

  @Override
  protected List<String> getStorageSpecificCreationCommandArgs() {
    return ImmutableList.of(
        "java",
        "-jar",
        "scalardb-schema-loader.jar",
        "--jdbc",
        "-j",
        config.getContactPoints().get(0),
        "--schema-file",
        SCHEMA_FILE,
        "-u",
        config.getUsername().get(),
        "-p",
        config.getPassword().get());
  }
}
