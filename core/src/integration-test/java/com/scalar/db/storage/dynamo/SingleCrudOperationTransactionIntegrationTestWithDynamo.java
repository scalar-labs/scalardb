package com.scalar.db.storage.dynamo;

import com.scalar.db.transaction.singlecrudoperation.SingleCrudOperationTransactionIntegrationTestBase;
import java.util.Map;
import java.util.Properties;

public class SingleCrudOperationTransactionIntegrationTestWithDynamo
    extends SingleCrudOperationTransactionIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    return DynamoEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return DynamoEnv.getCreationOptions();
  }
}
