package com.scalar.db.storage.jdbc;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminIntegrationTestBase;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class ConsensusCommitAdminIntegrationTestWithJdbcDatabase
    extends ConsensusCommitAdminIntegrationTestBase {

  private static final String PROP_JDBC_URL = "scalardb.jdbc.url";
  private static final String DEFAULT_JDBC_URL = "jdbc:mysql://localhost:3306/";

  @Override
  protected Properties getProps(String testName) {
    return JdbcEnv.getProperties(testName);
  }

  // Since SQLite doesn't have persistent namespaces, some behaviors around the namespace are
  // different from the other adapters. So disable several tests that check such behaviors.

  private boolean isSqlite() {
    String jdbcUrl = System.getProperty(PROP_JDBC_URL, DEFAULT_JDBC_URL);
    return jdbcUrl.startsWith("jdbc:sqlite:");
  }

  @Test
  @Override
  public void createNamespace_ForNonExistingNamespace_ShouldCreateNamespaceProperly()
      throws ExecutionException {
    if (!isSqlite()) {
      super.createNamespace_ForNonExistingNamespace_ShouldCreateNamespaceProperly();
    }
  }

  @Test
  @Override
  public void createNamespace_ForExistingNamespace_ShouldThrowExecutionException() {
    if (!isSqlite()) {
      super.createNamespace_ForExistingNamespace_ShouldThrowExecutionException();
    }
  }

  @Test
  @Override
  public void dropNamespace_ForNonExistingNamespace_ShouldThrowExecutionException() {
    if (!isSqlite()) {
      super.dropNamespace_ForNonExistingNamespace_ShouldThrowExecutionException();
    }
  }
}
