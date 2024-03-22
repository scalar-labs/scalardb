package com.scalar.db.storage.dynamo;

import com.scalar.db.transaction.autocommit.AutoCommitTransactionAdminIntegrationTestBase;
import java.util.Map;
import java.util.Properties;

public class AutoCommitTransactionAdminIntegrationTestWithDynamo
    extends AutoCommitTransactionAdminIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    return DynamoEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return DynamoEnv.getCreationOptions();
  }

  @Override
  protected boolean isIndexOnBooleanColumnSupported() {
    return false;
  }
}
