package com.scalar.db.transaction.jdbc;

import com.scalar.db.api.DistributedTransactionIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.jdbc.JdbcConfig;
import com.scalar.db.storage.jdbc.JdbcEnv;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

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
  public void get_GetGivenForCommittedRecord_InReadOnlyMode_ShouldReturnRecord() {}

  @Disabled("Implement later")
  @Override
  @ParameterizedTest
  @EnumSource(ScanType.class)
  public void scanOrGetScanner_ScanGivenForCommittedRecord_InReadOnlyMode_ShouldReturnRecords(
      ScanType scanType) {}
}
