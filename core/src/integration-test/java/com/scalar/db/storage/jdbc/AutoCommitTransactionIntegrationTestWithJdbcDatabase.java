package com.scalar.db.storage.jdbc;

import com.scalar.db.transaction.autocommit.AutoCommitTransactionIntegrationTestBase;
import java.util.Properties;

public class AutoCommitTransactionIntegrationTestWithJdbcDatabase
    extends AutoCommitTransactionIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    return JdbcEnv.getProperties(testName);
  }
}
