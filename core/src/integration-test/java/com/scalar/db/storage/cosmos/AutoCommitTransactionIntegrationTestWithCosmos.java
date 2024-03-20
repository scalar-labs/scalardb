package com.scalar.db.storage.cosmos;

import com.scalar.db.transaction.autocommit.AutoCommitTransactionIntegrationTestBase;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class AutoCommitTransactionIntegrationTestWithCosmos
    extends AutoCommitTransactionIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    return CosmosEnv.getProperties(testName);
  }

  @Override
  protected String getNamespaceBaseName() {
    String namespaceBaseName = super.getNamespaceBaseName();
    Optional<String> databasePrefix = CosmosEnv.getDatabasePrefix();
    return databasePrefix.map(prefix -> prefix + namespaceBaseName).orElse(namespaceBaseName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return CosmosEnv.getCreationOptions();
  }
}
