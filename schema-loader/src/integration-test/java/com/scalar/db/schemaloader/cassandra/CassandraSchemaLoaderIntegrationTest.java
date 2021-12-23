package com.scalar.db.schemaloader.cassandra;

import com.google.common.collect.ImmutableList;
import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import com.scalar.db.storage.cassandra.CassandraEnv;
import java.util.List;

public class CassandraSchemaLoaderIntegrationTest extends SchemaLoaderIntegrationTestBase {

  @Override
  protected void initialize() {
    config = CassandraEnv.getDatabaseConfig();
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
