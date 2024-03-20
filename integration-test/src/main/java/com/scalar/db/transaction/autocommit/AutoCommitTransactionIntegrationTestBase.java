package com.scalar.db.transaction.autocommit;

import com.scalar.db.api.DistributedTransactionIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;

public abstract class AutoCommitTransactionIntegrationTestBase
    extends DistributedTransactionIntegrationTestBase {

  @Override
  protected String getTestName() {
    return "tx_at";
  }

  @Override
  protected final Properties getProperties(String testName) {
    Properties properties = new Properties();
    properties.putAll(getProps(testName));
    properties.setProperty(
        DatabaseConfig.TRANSACTION_MANAGER, AutoCommitTransactionConfig.TRANSACTION_MANAGER_NAME);
    return properties;
  }

  protected abstract Properties getProps(String testName);

  @Disabled("auto-commit transaction doesn't support getState()")
  @Override
  public void getState_forSuccessfulTransaction_ShouldReturnCommittedState() {}

  @Disabled("auto-commit transaction doesn't support getState()")
  @Override
  public void getState_forFailedTransaction_ShouldReturnAbortedState() {}

  @Disabled("auto-commit transaction doesn't support abort()")
  @Override
  public void abort_forOngoingTransaction_ShouldAbortCorrectly() {}

  @Disabled("auto-commit transaction doesn't support rollback()")
  @Override
  public void rollback_forOngoingTransaction_ShouldRollbackCorrectly() {}

  @Disabled("auto-commit transaction doesn't support implicit pre-read")
  @Override
  public void
      putAndCommit_PutWithImplicitPreReadDisabledGivenForExisting_ShouldThrowCommitConflictException() {}

  @Disabled("auto-commit transaction doesn't support transaction abort")
  @Override
  public void putAndAbort_ShouldNotCreateRecord() {}

  @Disabled("auto-commit transaction doesn't support transaction rollback")
  @Override
  public void putAndRollback_ShouldNotCreateRecord() {}

  @Disabled("auto-commit transaction doesn't support transaction abort")
  @Override
  public void deleteAndAbort_ShouldNotDeleteRecord() {}

  @Disabled("auto-commit transaction doesn't support transaction rollback")
  @Override
  public void deleteAndRollback_ShouldNotDeleteRecord() {}
}
