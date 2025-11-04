package com.scalar.db.storage.objectstorage;

import com.scalar.db.transaction.singlecrudoperation.SingleCrudOperationTransactionIntegrationTestBase;
import java.util.Properties;

public class SingleCrudOperationTransactionIntegrationTestWithObjectStorage
    extends SingleCrudOperationTransactionIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    return ObjectStorageEnv.getProperties(testName);
  }
}
