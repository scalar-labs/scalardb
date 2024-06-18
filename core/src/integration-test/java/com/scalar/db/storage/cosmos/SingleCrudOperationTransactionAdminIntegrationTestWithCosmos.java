package com.scalar.db.storage.cosmos;

import com.scalar.db.transaction.singlecrudoperation.SingleCrudOperationTransactionAdminIntegrationTestBase;
import java.util.Map;
import java.util.Properties;

public class SingleCrudOperationTransactionAdminIntegrationTestWithCosmos
    extends SingleCrudOperationTransactionAdminIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    return CosmosEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return CosmosEnv.getCreationOptions();
  }
}
