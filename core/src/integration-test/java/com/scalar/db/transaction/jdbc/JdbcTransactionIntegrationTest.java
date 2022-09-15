package com.scalar.db.transaction.jdbc;

import com.scalar.db.api.DistributedTransactionIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.jdbc.JdbcEnv;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;

public class JdbcTransactionIntegrationTest extends DistributedTransactionIntegrationTestBase {

  @Override
  protected String getTestName() {
    return "tx_jdbc";
  }

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = new Properties();
    properties.putAll(JdbcEnv.getProperties(testName));
    properties.setProperty(DatabaseConfig.TRANSACTION_MANAGER, "jdbc");
    return properties;
  }

  @Disabled("JDBC transaction doesn't support getState()")
  @Override
  public void getState_forSuccessfulTransaction_ShouldReturnCommittedState() {}

  @Disabled("JDBC transaction doesn't support getState()")
  @Override
  public void getState_forFailedTransaction_ShouldReturnAbortedState() {}

  @Disabled("JDBC transaction doesn't support abort()")
  @Override
  public void abort_forOngoingTransaction_ShouldAbortCorrectly() {}

  @Disabled("JDBC transaction doesn't support rollback()")
  @Override
  public void rollback_forOngoingTransaction_ShouldRollbackCorrectly() {}
}
