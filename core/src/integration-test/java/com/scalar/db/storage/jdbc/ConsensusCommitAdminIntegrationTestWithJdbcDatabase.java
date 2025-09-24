package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThatCode;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.condition.EnabledIf;

public class ConsensusCommitAdminIntegrationTestWithJdbcDatabase
    extends ConsensusCommitAdminIntegrationTestBase {
  private RdbEngineStrategy rdbEngine;

  @Override
  protected Properties getProps(String testName) {
    Properties properties = JdbcEnv.getProperties(testName);
    rdbEngine = RdbEngineFactory.create(new JdbcConfig(new DatabaseConfig(properties)));
    return properties;
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new JdbcAdminTestUtils(getProperties(testName));
  }

  @Override
  protected boolean isCreateIndexOnTextColumnEnabled() {
    // "admin.createIndex()" for TEXT column fails (the "create index" query runs
    // indefinitely) on the Db2 community edition docker version which we use for the CI.
    // However, the index creation is successful on Db2 hosted on IBM Cloud.
    // So we disable these tests until the issue with the Db2 community edition is resolved.
    return !JdbcTestUtils.isDb2(rdbEngine);
  }

  @SuppressWarnings("unused")
  private boolean isDb2() {
    return JdbcEnv.isDb2();
  }

  @Test
  @Override
  @DisabledIf("isDb2")
  public void renameColumn_ForPrimaryKeyColumn_ShouldRenameColumnCorrectly()
      throws ExecutionException {
    super.renameColumn_ForPrimaryKeyColumn_ShouldRenameColumnCorrectly();
  }

  @Test
  @Override
  @DisabledIf("isDb2")
  public void renameColumn_ForIndexKeyColumn_ShouldRenameColumnAndIndexCorrectly()
      throws ExecutionException {
    super.renameColumn_ForIndexKeyColumn_ShouldRenameColumnAndIndexCorrectly();
  }

  @Test
  @EnabledIf("isDb2")
  public void renameColumn_Db2_ForPrimaryOrIndexKeyColumn_ShouldThrowUnsupportedOperationException()
      throws ExecutionException {
    try {
      // Arrange
      Map<String, String> options = getCreationOptions();
      TableMetadata currentTableMetadata =
          TableMetadata.newBuilder()
              .addColumn(COL_NAME1, DataType.INT)
              .addColumn(COL_NAME2, DataType.INT)
              .addColumn(COL_NAME3, DataType.TEXT)
              .addPartitionKey(COL_NAME1)
              .addClusteringKey(COL_NAME2)
              .addSecondaryIndex(COL_NAME3)
              .build();
      admin.createTable(namespace1, TABLE4, currentTableMetadata, options);

      // Act Assert
      assertThatCode(() -> admin.renameColumn(namespace1, TABLE4, COL_NAME1, COL_NAME4))
          .isInstanceOf(UnsupportedOperationException.class);
      assertThatCode(() -> admin.renameColumn(namespace1, TABLE4, COL_NAME2, COL_NAME4))
          .isInstanceOf(UnsupportedOperationException.class);
      assertThatCode(() -> admin.renameColumn(namespace1, TABLE4, COL_NAME3, COL_NAME4))
          .isInstanceOf(UnsupportedOperationException.class);
    } finally {
      admin.dropTable(namespace1, TABLE4, true);
    }
  }

  @Override
  protected boolean isIndexOnBlobColumnSupported() {
    return !(JdbcTestUtils.isDb2(rdbEngine) || JdbcTestUtils.isOracle(rdbEngine));
  }
}
