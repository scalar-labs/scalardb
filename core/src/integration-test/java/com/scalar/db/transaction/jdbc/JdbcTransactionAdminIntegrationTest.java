package com.scalar.db.transaction.jdbc;

import com.scalar.db.api.DistributedTransactionAdminIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.storage.jdbc.JdbcEnv;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class JdbcTransactionAdminIntegrationTest
    extends DistributedTransactionAdminIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = new Properties();
    properties.putAll(JdbcEnv.getProperties(testName));
    properties.setProperty(DatabaseConfig.TRANSACTION_MANAGER, "jdbc");
    return properties;
  }

  // Disable several tests for the coordinator tables since JDBC transaction doesn't have
  // coordinator tables
  @Disabled("JDBC transaction doesn't have coordinator tables")
  @Test
  @Override
  public void createCoordinatorTables_ShouldCreateCoordinatorTablesCorrectly()
      throws ExecutionException {
    super.createCoordinatorTables_ShouldCreateCoordinatorTablesCorrectly();
  }

  @Disabled("JDBC transaction doesn't have coordinator tables")
  @Test
  @Override
  public void
      createCoordinatorTables_CoordinatorTablesAlreadyExist_ShouldThrowIllegalArgumentException() {
    super
        .createCoordinatorTables_CoordinatorTablesAlreadyExist_ShouldThrowIllegalArgumentException();
  }

  @Disabled("JDBC transaction doesn't have coordinator tables")
  @Test
  @Override
  public void
      createCoordinatorTables_IfNotExist_CoordinatorTablesAlreadyExist_ShouldNotThrowAnyException() {
    super
        .createCoordinatorTables_IfNotExist_CoordinatorTablesAlreadyExist_ShouldNotThrowAnyException();
  }

  @Disabled("JDBC transaction doesn't have coordinator tables")
  @Test
  @Override
  public void dropCoordinatorTables_ShouldDropCoordinatorTablesCorrectly()
      throws ExecutionException {
    super.dropCoordinatorTables_ShouldDropCoordinatorTablesCorrectly();
  }

  @Disabled("JDBC transaction doesn't have coordinator tables")
  @Test
  @Override
  public void
      dropCoordinatorTables_CoordinatorTablesDoNotExist_ShouldThrowIllegalArgumentException()
          throws ExecutionException {
    super.dropCoordinatorTables_CoordinatorTablesDoNotExist_ShouldThrowIllegalArgumentException();
  }

  @Disabled("JDBC transaction doesn't have coordinator tables")
  @Test
  @Override
  public void dropCoordinatorTables_IfExist_CoordinatorTablesDoNotExist_ShouldNotThrowAnyException()
      throws ExecutionException {
    super.dropCoordinatorTables_IfExist_CoordinatorTablesDoNotExist_ShouldNotThrowAnyException();
  }
}
