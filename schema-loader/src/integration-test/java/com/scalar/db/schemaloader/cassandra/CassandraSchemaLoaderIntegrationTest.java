package com.scalar.db.schemaloader.cassandra;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import com.scalar.db.storage.cassandra.CassandraEnv;

public class CassandraSchemaLoaderIntegrationTest extends SchemaLoaderIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return CassandraEnv.getDatabaseConfig();
  }
}
