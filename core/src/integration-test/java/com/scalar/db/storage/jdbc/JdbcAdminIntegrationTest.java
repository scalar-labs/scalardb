package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThatCode;

import com.scalar.db.api.DistributedStorageAdminIntegrationTestBase;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.util.AdminTestUtils;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.condition.EnabledIf;

public class JdbcAdminIntegrationTest extends DistributedStorageAdminIntegrationTestBase {
  private RdbEngineStrategy rdbEngine;

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = JdbcEnv.getProperties(testName);
    rdbEngine = RdbEngineFactory.create(new JdbcConfig(new DatabaseConfig(properties)));
    return properties;
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new JdbcAdminTestUtils(getProperties(testName));
  }

  @Override
  protected boolean isCreateIndexOnTextAndBlobColumnsEnabled() {
    // "admin.createIndex()" for TEXT and BLOB columns fails (the "create index" query runs
    // indefinitely) on Db2 community edition version but works on Db2 hosted on IBM Cloud.
    // So we disable these tests until the issue is resolved.
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
              .addColumn(getColumnName1(), DataType.INT)
              .addColumn(getColumnName2(), DataType.INT)
              .addColumn(getColumnName3(), DataType.TEXT)
              .addPartitionKey(getColumnName1())
              .addClusteringKey(getColumnName2())
              .addSecondaryIndex(getColumnName3())
              .build();
      admin.createTable(getNamespace1(), getTable4(), currentTableMetadata, options);

      // Act Assert
      assertThatCode(
              () ->
                  admin.renameColumn(
                      getNamespace1(), getTable4(), getColumnName1(), getColumnName4()))
          .isInstanceOf(UnsupportedOperationException.class);
      assertThatCode(
              () ->
                  admin.renameColumn(
                      getNamespace1(), getTable4(), getColumnName2(), getColumnName4()))
          .isInstanceOf(UnsupportedOperationException.class);
      assertThatCode(
              () ->
                  admin.renameColumn(
                      getNamespace1(), getTable4(), getColumnName3(), getColumnName4()))
          .isInstanceOf(UnsupportedOperationException.class);
    } finally {
      admin.dropTable(getNamespace1(), getTable4(), true);
    }
  }
}
