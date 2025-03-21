package com.scalar.db.transaction.jdbc;

import com.scalar.db.api.DistributedTransactionIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.jdbc.JdbcConfig;
import com.scalar.db.storage.jdbc.JdbcEnv;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class JdbcTransactionIntegrationTest extends DistributedTransactionIntegrationTestBase {

  @Override
  protected String getTestName() {
    return "tx_jdbc";
  }

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = new Properties();
    properties.putAll(JdbcEnv.getProperties(testName));
    properties.setProperty(DatabaseConfig.TRANSACTION_MANAGER, JdbcConfig.TRANSACTION_MANAGER_NAME);
    return properties;
  }

  @Disabled("JDBC transactions don't support getState()")
  @Override
  @Test
  public void getState_forSuccessfulTransaction_ShouldReturnCommittedState() {}

  @Disabled("JDBC transactions don't support getState()")
  @Override
  @Test
  public void getState_forFailedTransaction_ShouldReturnAbortedState() {}

  @Disabled("JDBC transactions don't support abort()")
  @Override
  @Test
  public void abort_forOngoingTransaction_ShouldAbortCorrectly() {}

  @Disabled("JDBC transactions don't support rollback()")
  @Override
  @Test
  public void rollback_forOngoingTransaction_ShouldRollbackCorrectly() {}

  @Disabled("Implement later")
  @Override
  @Test
  public void getScanner_ScanGivenForCommittedRecord_ShouldReturnRecords() {}

  @Disabled("Implement later")
  @Override
  @Test
  public void manager_getScanner_ScanGivenForCommittedRecord_ShouldReturnRecords() {}
}
