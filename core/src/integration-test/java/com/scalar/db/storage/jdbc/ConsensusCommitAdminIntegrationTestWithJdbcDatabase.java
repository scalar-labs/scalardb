package com.scalar.db.storage.jdbc;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminIntegrationTestBase;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

public class ConsensusCommitAdminIntegrationTestWithJdbcDatabase
    extends ConsensusCommitAdminIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    return JdbcEnv.getProperties(testName);
  }

  // Since SQLite doesn't have persistent namespaces, some behaviors around the namespace are
  // different from the other adapters. So disable several tests that check such behaviors.

  private boolean isSqlite() {
    return JdbcEnv.getRdbEngineFromProperty() == RdbEngine.SQLITE;
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
  public void createNamespace_ForExistingNamespace_ShouldThrowExecutionException() {
    super.createNamespace_ForExistingNamespace_ShouldThrowExecutionException();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void dropNamespace_ForNonExistingNamespace_ShouldThrowExecutionException() {
    super.dropNamespace_ForNonExistingNamespace_ShouldThrowExecutionException();
  }
}
