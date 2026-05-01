package com.scalar.db.transaction.jdbc;

import com.scalar.db.api.DistributedTransactionCrossPartitionScanIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.storage.jdbc.JdbcConfig;
import com.scalar.db.storage.jdbc.JdbcEnv;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

public class JdbcTransactionCrossPartitionScanIntegrationTest
    extends DistributedTransactionCrossPartitionScanIntegrationTestBase {

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

  @Test
  @Override
  @DisabledIf("isSpanner")
  public void
      scan_CrossPartitionScanWithLikeWithCustomEscapeCharacterGivenForCommittedRecord_ShouldReturnRecord()
          throws TransactionException {
    super
        .scan_CrossPartitionScanWithLikeWithCustomEscapeCharacterGivenForCommittedRecord_ShouldReturnRecord();
  }

  @SuppressWarnings("unused")
  private boolean isSpanner() {
    return JdbcEnv.isSpanner();
  }
}
