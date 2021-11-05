package com.scalar.db.schemaloader.cassandra;

import com.google.common.collect.ImmutableList;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import com.scalar.db.storage.cassandra.CassandraEnv;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Properties;

public class CassandraSchemaLoaderIntegrationTest extends SchemaLoaderIntegrationTestBase {
  private static final DatabaseConfig config = CassandraEnv.getDatabaseConfig();

  @Override
  protected void initialize() throws Exception {
    Properties properties = config.getProperties();
    try (final FileOutputStream fileOutputStream = new FileOutputStream(CONFIG_FILE)) {
      properties.store(fileOutputStream, null);
    }
  }

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return config;
  }

  @Override
  protected List<String> getStorageSpecificCreationCommandArgs() {
    return ImmutableList.of(
        "java",
        "-jar",
        "scalardb-schema-loader.jar",
        "--cassandra",
        "-h",
        String.valueOf(config.getContactPort()),
        "--schema-file",
        SCHEMA_FILE,
        "-u",
        config.getUsername().get(),
        "-p",
        config.getPassword().get());
  }
}
