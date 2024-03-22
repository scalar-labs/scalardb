package com.scalar.db.transaction.autocommit;

import com.scalar.db.api.DistributedTransactionAdminIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.util.AdminTestUtils;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public abstract class AutoCommitTransactionAdminIntegrationTestBase
    extends DistributedTransactionAdminIntegrationTestBase {

  @Override
  protected final Properties getProperties(String testName) {
    Properties properties = new Properties();
    properties.putAll(getProps(testName));
    properties.setProperty(
        DatabaseConfig.TRANSACTION_MANAGER, AutoCommitTransactionConfig.TRANSACTION_MANAGER_NAME);
    return properties;
  }

  protected abstract Properties getProps(String testName);

  // Disable several tests for the coordinator tables since auto-commit transaction doesn't have
  // coordinator tables
  @Disabled("auto-commit transaction doesn't have coordinator tables")
  @Test
  @Override
  public void createCoordinatorTables_ShouldCreateCoordinatorTablesCorrectly()
      throws ExecutionException {
    super.createCoordinatorTables_ShouldCreateCoordinatorTablesCorrectly();
  }

  @Disabled("auto-commit transaction doesn't have coordinator tables")
  @Test
  @Override
  public void
      createCoordinatorTables_CoordinatorTablesAlreadyExist_ShouldThrowIllegalArgumentException() {
    super
        .createCoordinatorTables_CoordinatorTablesAlreadyExist_ShouldThrowIllegalArgumentException();
  }

  @Disabled("auto-commit transaction doesn't have coordinator tables")
  @Test
  @Override
  public void
      createCoordinatorTables_IfNotExist_CoordinatorTablesAlreadyExist_ShouldNotThrowAnyException() {
    super
        .createCoordinatorTables_IfNotExist_CoordinatorTablesAlreadyExist_ShouldNotThrowAnyException();
  }

  @Disabled("auto-commit transaction doesn't have coordinator tables")
  @Test
  @Override
  public void dropCoordinatorTables_ShouldDropCoordinatorTablesCorrectly()
      throws ExecutionException {
    super.dropCoordinatorTables_ShouldDropCoordinatorTablesCorrectly();
  }

  @Disabled("auto-commit transaction doesn't have coordinator tables")
  @Test
  @Override
  public void
      dropCoordinatorTables_CoordinatorTablesDoNotExist_ShouldThrowIllegalArgumentException()
          throws ExecutionException {
    super.dropCoordinatorTables_CoordinatorTablesDoNotExist_ShouldThrowIllegalArgumentException();
  }

  @Disabled("auto-commit transaction doesn't have coordinator tables")
  @Test
  @Override
  public void dropCoordinatorTables_IfExist_CoordinatorTablesDoNotExist_ShouldNotThrowAnyException()
      throws ExecutionException {
    super.dropCoordinatorTables_IfExist_CoordinatorTablesDoNotExist_ShouldNotThrowAnyException();
  }

  @Test
  @Disabled("We don't test this case for auto-commit transaction")
  @Override
  public void
      upgrade_WhenMetadataTableExistsButNotNamespacesTable_ShouldCreateNamespacesTableAndImportExistingNamespaces() {}

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    throw new UnsupportedOperationException();
  }
}
