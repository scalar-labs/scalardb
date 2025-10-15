package com.scalar.db.transaction.singlecrudoperation;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.DistributedTransactionAdminIntegrationTestBase;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.util.AdminTestUtils;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public abstract class SingleCrudOperationTransactionAdminIntegrationTestBase
    extends DistributedTransactionAdminIntegrationTestBase {

  @Override
  protected String getTestName() {
    return "tx_admin_sco";
  }

  @Override
  protected final Properties getProperties(String testName) {
    Properties properties = new Properties();
    properties.putAll(getProps(testName));
    properties.putIfAbsent(
        DatabaseConfig.TRANSACTION_MANAGER,
        SingleCrudOperationTransactionConfig.TRANSACTION_MANAGER_NAME);
    return properties;
  }

  protected abstract Properties getProps(String testName);

  // Disable several tests for the coordinator tables since single CRUD transactions don't have
  // coordinator tables
  @Disabled("Single CRUD operation transactions don't have Coordinator tables")
  @Test
  @Override
  public void createCoordinatorTables_ShouldCreateCoordinatorTablesCorrectly()
      throws ExecutionException {
    super.createCoordinatorTables_ShouldCreateCoordinatorTablesCorrectly();
  }

  @Disabled("Single CRUD operation transactions don't have Coordinator tables")
  @Test
  @Override
  public void
      createCoordinatorTables_CoordinatorTablesAlreadyExist_ShouldThrowIllegalArgumentException() {
    super
        .createCoordinatorTables_CoordinatorTablesAlreadyExist_ShouldThrowIllegalArgumentException();
  }

  @Disabled("Single CRUD operation transactions don't have Coordinator tables")
  @Test
  @Override
  public void
      createCoordinatorTables_IfNotExist_CoordinatorTablesAlreadyExist_ShouldNotThrowAnyException() {
    super
        .createCoordinatorTables_IfNotExist_CoordinatorTablesAlreadyExist_ShouldNotThrowAnyException();
  }

  @Disabled("Single CRUD operation transactions don't have Coordinator tables")
  @Test
  @Override
  public void dropCoordinatorTables_ShouldDropCoordinatorTablesCorrectly()
      throws ExecutionException {
    super.dropCoordinatorTables_ShouldDropCoordinatorTablesCorrectly();
  }

  @Disabled("Single CRUD operation transactions don't have Coordinator tables")
  @Test
  @Override
  public void
      dropCoordinatorTables_CoordinatorTablesDoNotExist_ShouldThrowIllegalArgumentException()
          throws ExecutionException {
    super.dropCoordinatorTables_CoordinatorTablesDoNotExist_ShouldThrowIllegalArgumentException();
  }

  @Disabled("Single CRUD operation transactions don't have Coordinator tables")
  @Test
  @Override
  public void dropCoordinatorTables_IfExist_CoordinatorTablesDoNotExist_ShouldNotThrowAnyException()
      throws ExecutionException {
    super.dropCoordinatorTables_IfExist_CoordinatorTablesDoNotExist_ShouldNotThrowAnyException();
  }

  @Test
  @Disabled("This case is not tested for single CRUD operation transactions")
  @Override
  public void
      upgrade_WhenMetadataTableExistsButNotNamespacesTable_ShouldCreateNamespacesTableAndImportExistingNamespaces() {}

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void transactionalInsert(Insert insert) throws TransactionException {
    // Wait for cache expiry
    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

    try (DistributedTransactionManager manager = transactionFactory.getTransactionManager()) {
      manager.insert(insert);
    }
  }

  @Override
  protected List<Result> transactionalScan(Scan scan) throws TransactionException {
    // Wait for cache expiry
    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

    try (DistributedTransactionManager manager = transactionFactory.getTransactionManager()) {
      return manager.scan(scan);
    }
  }
}
