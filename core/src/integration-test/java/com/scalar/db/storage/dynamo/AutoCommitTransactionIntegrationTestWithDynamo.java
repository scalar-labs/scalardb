package com.scalar.db.storage.dynamo;

import com.scalar.db.transaction.autocommit.AutoCommitTransactionIntegrationTestBase;
import java.util.Map;
import java.util.Properties;

public class AutoCommitTransactionIntegrationTestWithDynamo
    extends AutoCommitTransactionIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    return DynamoEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return DynamoEnv.getCreationOptions();
  }
}
