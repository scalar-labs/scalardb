package com.scalar.db.storage.jdbc;

import com.scalar.db.transaction.autocommit.AutoCommitTransactionAdminIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import java.util.Properties;

public class AutoCommitTransactionAdminIntegrationTestWithJdbcDatabase
    extends AutoCommitTransactionAdminIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    return JdbcEnv.getProperties(testName);
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new JdbcAdminTestUtils(getProperties(testName));
  }
}
