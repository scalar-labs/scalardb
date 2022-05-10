package com.scalar.db.storage.cosmos;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.sql.SqlStatementWithTransactionSessionIntegrationTestBase;
import java.util.Map;
import java.util.Optional;

public class SqlStatementWithTransactionSessionIntegrationTestWithCosmos
    extends SqlStatementWithTransactionSessionIntegrationTestBase {
  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return CosmosEnv.getCosmosConfig();
  }

  @Override
  protected String getNamespace() {
    String namespace = super.getNamespace();
    Optional<String> databasePrefix = CosmosEnv.getDatabasePrefix();
    return databasePrefix.map(prefix -> prefix + namespace).orElse(namespace);
  }

  @Override
  protected Map<String, String> getCreateOptions() {
    return CosmosEnv.getCreateOptions();
  }
}
