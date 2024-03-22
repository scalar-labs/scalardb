package com.scalar.db.storage.cosmos;

import com.scalar.db.transaction.autocommit.AutoCommitTransactionAdminIntegrationTestBase;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class AutoCommitTransactionAdminIntegrationTestWithCosmos
    extends AutoCommitTransactionAdminIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    return CosmosEnv.getProperties(testName);
  }

  @Override
  protected String getNamespace1() {
    return getNamespace(super.getNamespace1());
  }

  @Override
  protected String getNamespace2() {
    return getNamespace(super.getNamespace2());
  }

  @Override
  protected String getNamespace3() {
    return getNamespace(super.getNamespace3());
  }

  private String getNamespace(String namespace) {
    Optional<String> databasePrefix = CosmosEnv.getDatabasePrefix();
    return databasePrefix.map(prefix -> prefix + namespace).orElse(namespace);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return CosmosEnv.getCreationOptions();
  }
}
