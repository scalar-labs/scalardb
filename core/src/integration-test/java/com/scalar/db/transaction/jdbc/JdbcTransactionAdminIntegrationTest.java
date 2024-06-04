package com.scalar.db.transaction.jdbc;

import com.scalar.db.api.DistributedTransactionAdminIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.storage.jdbc.JdbcConfig;
import com.scalar.db.storage.jdbc.JdbcEnv;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

public class JdbcTransactionAdminIntegrationTest
    extends DistributedTransactionAdminIntegrationTestBase {

  @Override
  protected String getTestName() {
    return "tx_admin_jdbc";
  }

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = new Properties();
    properties.putAll(JdbcEnv.getProperties(testName));
    properties.setProperty(DatabaseConfig.TRANSACTION_MANAGER, JdbcConfig.TRANSACTION_MANAGER_NAME);
    return properties;
  }

  // Since SQLite doesn't have persistent namespaces, some behaviors around the namespace are
  // different from the other adapters. So disable several tests that check such behaviors.

  @SuppressWarnings("unused")
  private boolean isSqlite() {
    return JdbcEnv.isSqlite();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void createNamespace_ForNonExistingNamespace_ShouldCreateNamespaceProperly()
      throws ExecutionException {
    super.createNamespace_ForNonExistingNamespace_ShouldCreateNamespaceProperly();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void createNamespace_ForExistingNamespace_ShouldThrowIllegalArgumentException() {
    super.createNamespace_ForExistingNamespace_ShouldThrowIllegalArgumentException();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void createNamespace_IfNotExists_ForExistingNamespace_ShouldNotThrowAnyException() {
    super.createNamespace_IfNotExists_ForExistingNamespace_ShouldNotThrowAnyException();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void dropNamespace_ForNonExistingNamespace_ShouldDropNamespaceProperly()
      throws ExecutionException {
    super.dropNamespace_ForNonExistingNamespace_ShouldDropNamespaceProperly();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void dropNamespace_ForNonExistingNamespace_ShouldThrowIllegalArgumentException() {
    super.dropNamespace_ForNonExistingNamespace_ShouldThrowIllegalArgumentException();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void dropNamespace_ForNonEmptyNamespace_ShouldThrowIllegalArgumentException()
      throws ExecutionException {
    super.dropNamespace_ForNonEmptyNamespace_ShouldThrowIllegalArgumentException();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void dropNamespace_IfExists_ForNonExistingNamespace_ShouldNotThrowAnyException() {
    super.dropNamespace_IfExists_ForNonExistingNamespace_ShouldNotThrowAnyException();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void namespaceExists_ShouldReturnCorrectResults() throws ExecutionException {
    super.namespaceExists_ShouldReturnCorrectResults();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void createTable_ForNonExistingNamespace_ShouldThrowIllegalArgumentException() {
    super.createTable_ForNonExistingNamespace_ShouldThrowIllegalArgumentException();
  }

  // Disable several tests for the coordinator tables since JDBC transaction doesn't have
  // coordinator tables

  @Disabled("JDBC transactions don't have Coordinator tables")
  @Test
  @Override
  public void createCoordinatorTables_ShouldCreateCoordinatorTablesCorrectly()
      throws ExecutionException {
    super.createCoordinatorTables_ShouldCreateCoordinatorTablesCorrectly();
  }

  @Disabled("JDBC transactions don't have Coordinator tables")
  @Test
  @Override
  public void
      createCoordinatorTables_CoordinatorTablesAlreadyExist_ShouldThrowIllegalArgumentException() {
    super
        .createCoordinatorTables_CoordinatorTablesAlreadyExist_ShouldThrowIllegalArgumentException();
  }

  @Disabled("JDBC transactions don't have Coordinator tables")
  @Test
  @Override
  public void
      createCoordinatorTables_IfNotExist_CoordinatorTablesAlreadyExist_ShouldNotThrowAnyException() {
    super
        .createCoordinatorTables_IfNotExist_CoordinatorTablesAlreadyExist_ShouldNotThrowAnyException();
  }

  @Disabled("JDBC transactions don't have Coordinator tables")
  @Test
  @Override
  public void dropCoordinatorTables_ShouldDropCoordinatorTablesCorrectly()
      throws ExecutionException {
    super.dropCoordinatorTables_ShouldDropCoordinatorTablesCorrectly();
  }

  @Disabled("JDBC transactions don't have Coordinator tables")
  @Test
  @Override
  public void
      dropCoordinatorTables_CoordinatorTablesDoNotExist_ShouldThrowIllegalArgumentException()
          throws ExecutionException {
    super.dropCoordinatorTables_CoordinatorTablesDoNotExist_ShouldThrowIllegalArgumentException();
  }

  @Disabled("JDBC transactions don't have Coordinator tables")
  @Test
  @Override
  public void dropCoordinatorTables_IfExist_CoordinatorTablesDoNotExist_ShouldNotThrowAnyException()
      throws ExecutionException {
    super.dropCoordinatorTables_IfExist_CoordinatorTablesDoNotExist_ShouldNotThrowAnyException();
  }
}
