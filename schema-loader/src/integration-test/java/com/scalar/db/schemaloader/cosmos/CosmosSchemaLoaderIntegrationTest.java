package com.scalar.db.schemaloader.cosmos;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import com.scalar.db.storage.cosmos.CosmosEnv;
import java.util.Optional;

public class CosmosSchemaLoaderIntegrationTest extends SchemaLoaderIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return CosmosEnv.getCosmosConfig();
  }

  @Override
  protected String getNamespace1() {
    return getNamespace(super.getNamespace1());
  }

  @Override
  protected String getNamespace2() {
    return getNamespace(super.getNamespace2());
  }

  private String getNamespace(String namespace) {
    Optional<String> databasePrefix = CosmosEnv.getDatabasePrefix();
    return databasePrefix.map(prefix -> prefix + namespace).orElse(namespace);
  }
}
