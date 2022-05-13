package com.scalar.db.storage.cassandra;

import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import java.util.Properties;

public class CassandraSchemaLoaderIntegrationTest extends SchemaLoaderIntegrationTestBase {

  @Override
  protected Properties getProperties() {
    return CassandraEnv.getProperties();
  }
}
