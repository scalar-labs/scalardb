package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.DistributedStorageAdminImportTableIntegrationTestBase;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.condition.EnabledIf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcAdminImportTableIntegrationTest
    extends DistributedStorageAdminImportTableIntegrationTestBase {
  private static final Logger logger =
      LoggerFactory.getLogger(JdbcAdminImportTableIntegrationTest.class);

  private JdbcAdminImportTestUtils testUtils;

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = JdbcEnv.getProperties(testName);
    testUtils = new JdbcAdminImportTestUtils(properties);
    return JdbcEnv.getProperties(testName);
  }

  @Override
  public void afterAll() {
    try {
      super.afterAll();
    } catch (Exception e) {
      logger.warn("Failed to call super.afterAll", e);
    }

    try {
      if (testUtils != null) {
        testUtils.close();
      }
    } catch (Exception e) {
      logger.warn("Failed to close test utils", e);
    }
  }

  @Override
  protected List<TestData> createExistingDatabaseWithAllDataTypes() throws SQLException {
    return testUtils.createExistingDatabaseWithAllDataTypes(getNamespace());
  }

  @Override
  protected List<String> getIntCompatibleColumnNamesOnExistingDatabase(String table) {
    return testUtils.getIntCompatibleColumnNamesOnExistingDatabase(table);
  }

  @Override
  protected List<String> getFloatCompatibleColumnNamesOnExistingDatabase(String table) {
    return testUtils.getFloatCompatibleColumnNamesOnExistingDatabase(table);
  }

  @Override
  protected void dropNonImportableTable(String table) throws SQLException {
    testUtils.dropTable(getNamespace(), table);
  }

  @SuppressWarnings("unused")
  private boolean isOracle() {
    return JdbcEnv.isOracle();
  }

  @SuppressWarnings("unused")
  private boolean isSqlServer() {
    return JdbcEnv.isSqlServer();
  }

  @SuppressWarnings("unused")
  private boolean isDb2() {
    return JdbcEnv.isDb2();
  }

  @SuppressWarnings("unused")
  private boolean isSqlite() {
    return JdbcEnv.isSqlite();
  }

  @SuppressWarnings("unused")
  private boolean isTidb() {
    return testUtils.isTidb();
  }

  @SuppressWarnings("unused")
  private boolean isColumnTypeConversionToTextNotFullySupported() {
    return JdbcEnv.isDb2()
        || JdbcEnv.isSqlServer()
        || JdbcEnv.isOracle()
        || JdbcEnv.isSqlite()
        || isTidb();
  }

  @SuppressWarnings("unused")
  private boolean isWideningColumnTypeConversionNotFullySupported() {
    return JdbcEnv.isOracle() || JdbcEnv.isSqlite();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void importTable_ShouldWorkProperly() throws Exception {
    super.importTable_ShouldWorkProperly();
  }

  @Test
  @Override
  @EnabledIf("isSqlite")
  public void importTable_ForUnsupportedDatabase_ShouldThrowUnsupportedOperationException()
      throws ExecutionException {
    super.importTable_ForUnsupportedDatabase_ShouldThrowUnsupportedOperationException();
  }

  @Test
  @Override
  @DisabledIf("isColumnTypeConversionToTextNotFullySupported")
  public void
      alterColumnType_AlterColumnTypeFromEachExistingDataTypeToText_ForImportedTable_ShouldAlterColumnTypesCorrectly()
          throws Exception {
    super
        .alterColumnType_AlterColumnTypeFromEachExistingDataTypeToText_ForImportedTable_ShouldAlterColumnTypesCorrectly();
  }

  @Test
  @EnabledIf("isSqlServer")
  public void
      alterColumnType_SqlServer_AlterColumnTypeFromEachExistingDataTypeToText_ForImportedTable_ShouldAlterColumnTypesCorrectly()
          throws Exception {
    // Arrange
    testDataList.addAll(createExistingDatabaseWithAllDataTypes());
    for (TestData testData : testDataList) {
      if (testData.isImportableTable()) {
        admin.importTable(
            getNamespace(),
            testData.getTableName(),
            Collections.emptyMap(),
            testData.getOverrideColumnsType());
      }
    }

    for (TestData testData : testDataList) {
      if (testData.isImportableTable()) {
        // Act
        TableMetadata metadata = testData.getTableMetadata();
        for (String column : metadata.getColumnNames()) {
          if (!metadata.getPartitionKeyNames().contains(column)
              && !metadata.getClusteringKeyNames().contains(column)) {
            if (Objects.equals(column, "col16")) {
              // Conversion from IMAGE to VARCHAR(8000) is not supported in SQL Server engine
              continue;
            }
            admin.alterColumnType(getNamespace(), testData.getTableName(), column, DataType.TEXT);
          }
        }

        // Assert
        TableMetadata newMetadata = admin.getTableMetadata(getNamespace(), testData.getTableName());
        assertThat(newMetadata).isNotNull();
        for (String column : metadata.getColumnNames()) {
          if (!metadata.getPartitionKeyNames().contains(column)
              && !metadata.getClusteringKeyNames().contains(column)) {
            if (Objects.equals(column, "col16")) {
              continue;
            }
            assertThat(newMetadata.getColumnDataType(column)).isEqualTo(DataType.TEXT);
          }
        }
      }
    }
  }

  @Test
  @EnabledIf("isDb2")
  public void
      alterColumnType_Db2_AlterColumnTypeFromEachExistingDataTypeToText_ForImportedTable_ShouldAlterColumnTypesCorrectly()
          throws Exception {
    // Arrange
    testDataList.addAll(createExistingDatabaseWithAllDataTypes());
    for (TestData testData : testDataList) {
      if (testData.isImportableTable()) {
        admin.importTable(
            getNamespace(),
            testData.getTableName(),
            Collections.emptyMap(),
            testData.getOverrideColumnsType());
      }
    }

    for (TestData testData : testDataList) {
      if (testData.isImportableTable()) {
        // Act
        TableMetadata metadata = testData.getTableMetadata();
        for (String column : metadata.getColumnNames()) {
          if (!metadata.getPartitionKeyNames().contains(column)
              && !metadata.getClusteringKeyNames().contains(column)) {
            if (metadata.getColumnDataType(column).equals(DataType.BLOB)) {
              // Conversion from BLOB to TEXT is not supported in Db2 engine
              continue;
            }
            admin.alterColumnType(getNamespace(), testData.getTableName(), column, DataType.TEXT);
          }
        }

        // Assert
        TableMetadata newMetadata = admin.getTableMetadata(getNamespace(), testData.getTableName());
        assertThat(newMetadata).isNotNull();
        for (String column : metadata.getColumnNames()) {
          if (!metadata.getPartitionKeyNames().contains(column)
              && !metadata.getClusteringKeyNames().contains(column)) {
            if (metadata.getColumnDataType(column).equals(DataType.BLOB)) {
              continue;
            }
            assertThat(newMetadata.getColumnDataType(column)).isEqualTo(DataType.TEXT);
          }
        }
      }
    }
  }

  @Test
  @EnabledIf("isTidb")
  public void
      alterColumnType_Tidb_AlterColumnTypeFromEachExistingDataTypeToText_ForImportedTable_ShouldAlterColumnTypesCorrectly()
          throws Exception {
    // Arrange
    testDataList.addAll(createExistingDatabaseWithAllDataTypes());
    for (TestData testData : testDataList) {
      if (testData.isImportableTable()) {
        admin.importTable(
            getNamespace(),
            testData.getTableName(),
            Collections.emptyMap(),
            testData.getOverrideColumnsType());
      }
    }

    for (TestData testData : testDataList) {
      if (testData.isImportableTable()) {
        // Act
        TableMetadata metadata = testData.getTableMetadata();
        for (String column : metadata.getColumnNames()) {
          if (!metadata.getPartitionKeyNames().contains(column)
              && !metadata.getClusteringKeyNames().contains(column)) {
            if (metadata.getColumnDataType(column).equals(DataType.BLOB)) {
              // Conversion from BLOB to TEXT is not supported in TiDB
              continue;
            }
            admin.alterColumnType(getNamespace(), testData.getTableName(), column, DataType.TEXT);
          }
        }

        // Assert
        TableMetadata newMetadata = admin.getTableMetadata(getNamespace(), testData.getTableName());
        assertThat(newMetadata).isNotNull();
        for (String column : metadata.getColumnNames()) {
          if (!metadata.getPartitionKeyNames().contains(column)
              && !metadata.getClusteringKeyNames().contains(column)) {
            if (metadata.getColumnDataType(column).equals(DataType.BLOB)) {
              continue;
            }
            assertThat(newMetadata.getColumnDataType(column)).isEqualTo(DataType.TEXT);
          }
        }
      }
    }
  }

  @Test
  @Override
  @DisabledIf("isWideningColumnTypeConversionNotFullySupported")
  public void alterColumnType_WideningConversion_ForImportedTable_ShouldAlterProperly()
      throws Exception {
    super.alterColumnType_WideningConversion_ForImportedTable_ShouldAlterProperly();
  }

  @Test
  @EnabledIf("isOracle")
  public void alterColumnType_Oracle_WideningConversion_ForImportedTable_ShouldAlterProperly()
      throws Exception {
    // Arrange
    testDataList.addAll(createExistingDatabaseWithAllDataTypes());
    for (TestData testData : testDataList) {
      if (testData.isImportableTable()) {
        admin.importTable(
            getNamespace(),
            testData.getTableName(),
            Collections.emptyMap(),
            testData.getOverrideColumnsType());
      }
    }

    for (TestData testData : testDataList) {
      if (testData.isImportableTable()) {
        // Act
        for (String intCompatibleColumn :
            getIntCompatibleColumnNamesOnExistingDatabase(testData.getTableName())) {
          admin.alterColumnType(
              getNamespace(), testData.getTableName(), intCompatibleColumn, DataType.BIGINT);
        }
        // Conversion from FLOAT TO DOUBLE is not supported in Oracle engine

        // Assert
        TableMetadata metadata = admin.getTableMetadata(getNamespace(), testData.getTableName());
        assertThat(metadata).isNotNull();
        for (String intCompatibleColumn :
            getIntCompatibleColumnNamesOnExistingDatabase(testData.getTableName())) {
          assertThat(metadata.getColumnDataType(intCompatibleColumn)).isEqualTo(DataType.BIGINT);
        }
      }
    }
  }
}
