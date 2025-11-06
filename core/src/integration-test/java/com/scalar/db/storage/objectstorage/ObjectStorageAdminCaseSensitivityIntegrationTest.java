package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.DistributedStorageAdminCaseSensitivityIntegrationTestBase;
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
  protected Map<String, String> getCreationOptions() {
    return ObjectStorageEnv.getCreationOptions();
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new ObjectStorageAdminTestUtils(getProperties(testName));
  }

  @Override
  @Disabled("Temporarily disabled because it includes DML operations")
  public void truncateTable_ShouldTruncateProperly() {}

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

  @Override
  @Disabled("The ScalarDB environment does not need to be upgraded with Object Storage")
  public void
      upgrade_WhenMetadataTableExistsButNotNamespacesTable_ShouldCreateNamespacesTableAndImportExistingNamespaces() {}
}
