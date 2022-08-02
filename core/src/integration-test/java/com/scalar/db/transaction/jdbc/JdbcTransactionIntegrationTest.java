package com.scalar.db.transaction.jdbc;

import com.scalar.db.api.DistributedTransactionIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.TransactionException;
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
  protected Properties getProperties() {
    Properties properties = new Properties();
    properties.putAll(JdbcEnv.getProperties());
    properties.setProperty(DatabaseConfig.TRANSACTION_MANAGER, "jdbc");
    return properties;
  }

  // JDBC transaction doesn't support getState() and abort()

  @Disabled
  @Override
  public void getState_forSuccessfulTransaction_ShouldReturnCommittedState() {}

  @Disabled
  @Override
  public void getState_forFailedTransaction_ShouldReturnAbortedState() {}

  @Disabled
  @Override
  public void abort_forOngoingTransaction_ShouldAbortCorrectly() {}

  @Disabled("There is no metadata columns when using the jdbc transaction manager")
  @Override
  @Test
  public void scan_UsingDebugMode_ShouldReturnTransactionMetadataColumns() {}

  @Disabled("There is no metadata columns when using the jdbc transaction manager")
  @Override
  @Test
  public void scan_UsingDebugModeWithProjections_ShouldReturnProjectedColumns()
      throws TransactionException {
    super.scan_UsingDebugModeWithProjections_ShouldReturnProjectedColumns();
  }

  @Disabled("There is no metadata columns when using the jdbc transaction manager")
  @Override
  @Test
  public void get_UsingDebugMode_ShouldReturnTransactionMetadataColumns()
      throws TransactionException {
    super.get_UsingDebugMode_ShouldReturnTransactionMetadataColumns();
  }

  @Disabled("There is no metadata columns when using the jdbc transaction manager")
  @Override
  @Test
  public void get_UsingDebugModeWithProjections_ShouldReturnProjectedColumns()
      throws TransactionException {
    super.get_UsingDebugModeWithProjections_ShouldReturnProjectedColumns();
  }
}
