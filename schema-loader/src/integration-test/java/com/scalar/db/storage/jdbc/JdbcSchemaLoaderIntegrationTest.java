package com.scalar.db.storage.jdbc;

import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import java.util.Properties;

public class JdbcSchemaLoaderIntegrationTest extends SchemaLoaderIntegrationTestBase {

  @Override
  protected Properties getProperties() {
    return JdbcEnv.getProperties();
  }
}
