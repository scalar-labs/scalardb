package com.scalar.db.storage.jdbc;

import com.scalar.db.transaction.singlecrudoperation.SingleCrudOperationTransactionIntegrationTestBase;
import java.util.Properties;

public class SingleCrudOperationTransactionIntegrationTestWithJdbcDatabase
    extends SingleCrudOperationTransactionIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    return JdbcEnv.getProperties(testName);
  }
}
