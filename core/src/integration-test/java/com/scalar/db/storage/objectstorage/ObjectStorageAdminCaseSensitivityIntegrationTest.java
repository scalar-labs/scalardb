package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.DistributedStorageAdminCaseSensitivityIntegrationTestBase;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.util.AdminTestUtils;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;

public class ObjectStorageAdminCaseSensitivityIntegrationTest
    extends DistributedStorageAdminCaseSensitivityIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return ObjectStorageEnv.getProperties(testName);
  }

  @Override
  protected String getSystemNamespaceName(Properties properties) {
    return ObjectStorageUtils.getObjectStorageConfig(new DatabaseConfig(properties))
        .getMetadataNamespace();
  }

  @Override
  protected TableMetadata getTableMetadata() {
    return TableMetadata.newBuilder()
        .addColumn(getColumnName1(), DataType.INT)
        .addColumn(getColumnName2(), DataType.TEXT)
        .addColumn(getColumnName3(), DataType.TEXT)
        .addColumn(getColumnName4(), DataType.INT)
        .addColumn(getColumnName5(), DataType.INT)
        .addColumn(getColumnName6(), DataType.TEXT)
        .addColumn(getColumnName7(), DataType.BIGINT)
        .addColumn(getColumnName8(), DataType.FLOAT)
        .addColumn(getColumnName9(), DataType.DOUBLE)
        .addColumn(getColumnName10(), DataType.BOOLEAN)
        .addColumn(getColumnName11(), DataType.BLOB)
        .addColumn(getColumnName12(), DataType.DATE)
        .addColumn(getColumnName13(), DataType.TIME)
        .addColumn(getColumnName14(), DataType.TIMESTAMPTZ)
        .addPartitionKey(getColumnName2())
        .addPartitionKey(getColumnName1())
        .addClusteringKey(getColumnName4(), Scan.Ordering.Order.ASC)
        .addClusteringKey(getColumnName3(), Scan.Ordering.Order.DESC)
        .build();
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return ObjectStorageEnv.getCreationOptions();
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new ObjectStorageAdminTestUtils(getProperties(testName));
  }

  @Override
  @Disabled("Object Storage does not have a concept of namespaces")
  public void
      dropNamespace_ForNamespaceWithNonScalarDBManagedTables_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void createIndex_ForAllDataTypesWithExistingData_ShouldCreateIndexesCorrectly() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void createIndex_ForNonExistingTable_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void createIndex_ForNonExistingColumn_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void createIndex_ForAlreadyExistingIndex_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void createIndex_IfNotExists_ForAlreadyExistingIndex_ShouldNotThrowAnyException() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void dropIndex_ForAllDataTypesWithExistingData_ShouldDropIndexCorrectly() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void dropIndex_ForNonExistingTable_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void dropIndex_ForNonExistingIndex_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void dropIndex_IfExists_ForNonExistingIndex_ShouldNotThrowAnyException() {}

  @Override
  @Disabled("Object Storage does not support dropping columns")
  public void dropColumnFromTable_DropColumnForEachExistingDataType_ShouldDropColumnsCorrectly() {}

  @Override
  @Disabled("Object Storage does not support dropping columns")
  public void dropColumnFromTable_ForNonExistingTable_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("Object Storage does not support dropping columns")
  public void dropColumnFromTable_ForNonExistingColumn_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("Object Storage does not support dropping columns")
  public void dropColumnFromTable_ForPrimaryKeyColumn_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("Object Storage does not support dropping columns")
  public void dropColumnFromTable_ForIndexedColumn_ShouldDropColumnAndIndexCorrectly() {}

  @Override
  @Disabled("Object Storage does not support dropping columns")
  public void dropColumnFromTable_IfExists_ForNonExistingColumn_ShouldNotThrowAnyException() {}

  @Override
  @Disabled("Object Storage does not support renaming columns")
  public void renameColumn_ShouldRenameColumnCorrectly() {}

  @Override
  @Disabled("Object Storage does not support renaming columns")
  public void renameColumn_ForNonExistingTable_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("Object Storage does not support renaming columns")
  public void renameColumn_ForNonExistingColumn_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("Object Storage does not support renaming columns")
  public void renameColumn_ForPrimaryKeyColumn_ShouldRenameColumnCorrectly() {}

  @Override
  @Disabled("Object Storage does not support renaming columns")
  public void renameColumn_ForIndexKeyColumn_ShouldRenameColumnAndIndexCorrectly() {}

  @Override
  @Disabled("Object Storage does not support altering column types")
  public void
      alterColumnType_AlterColumnTypeFromEachExistingDataTypeToText_ShouldAlterColumnTypesCorrectly() {}

  @Override
  @Disabled("Object Storage does not support altering column types")
  public void alterColumnType_WideningConversion_ShouldAlterColumnTypesCorrectly() {}

  @Override
  @Disabled("Object Storage does not support altering column types")
  public void alterColumnType_ForPrimaryKeyOrIndexKeyColumn_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("Object Storage does not support renaming tables")
  public void renameTable_ForExistingTable_ShouldRenameTableCorrectly() {}

  @Override
  @Disabled("Object Storage does not support renaming tables")
  public void renameTable_ForNonExistingTable_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("Object Storage does not support renaming tables")
  public void renameTable_IfNewTableNameAlreadyExists_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("Object Storage does not support renaming tables")
  public void renameTable_ForExistingTableWithIndexes_ShouldRenameTableAndIndexesCorrectly() {}

  @Override
  @Disabled("Object Storage does not support renaming tables")
  public void renameTable_IfOnlyOneTableExists_ShouldRenameTableCorrectly() {}
}
