package com.scalar.db.storage.cassandra;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.DistributedStorageAdminCaseSensitivityIntegrationTestBase;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;

public class CassandraAdminCaseSensitivityIntegrationTest
    extends DistributedStorageAdminCaseSensitivityIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return CassandraEnv.getProperties(testName);
  }

  @Override
  protected String getSystemNamespaceName(Properties properties) {
    return new CassandraConfig(new DatabaseConfig(properties))
        .getSystemNamespaceName()
        .orElse(DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME);
  }

  @Override
  protected boolean isTimestampTypeSupported() {
    return false;
  }

  @Override
  @Disabled("Renaming non-primary key columns is not supported in Cassandra")
  public void renameColumn_ShouldRenameColumnCorrectly() {}

  @Override
  public void renameColumn_ForIndexKeyColumn_ShouldRenameColumnAndIndexCorrectly()
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
              .addSecondaryIndex(getColumnName1())
              .build();
      admin.createTable(getNamespace1(), getTable4(), currentTableMetadata, options);

      // Act
      admin.renameColumn(getNamespace1(), getTable4(), getColumnName1(), getColumnName4());

      // Assert
      TableMetadata expectedTableMetadata =
          TableMetadata.newBuilder()
              .addColumn(getColumnName4(), DataType.INT)
              .addColumn(getColumnName2(), DataType.INT)
              .addColumn(getColumnName3(), DataType.TEXT)
              .addPartitionKey(getColumnName4())
              .addClusteringKey(getColumnName2())
              .addSecondaryIndex(getColumnName4())
              .build();
      assertThat(admin.getTableMetadata(getNamespace1(), getTable4()))
          .isEqualTo(expectedTableMetadata);
      assertThat(admin.indexExists(getNamespace1(), getTable4(), getColumnName1())).isFalse();
      assertThat(admin.indexExists(getNamespace1(), getTable4(), getColumnName4())).isTrue();
    } finally {
      admin.dropTable(getNamespace1(), getTable4(), true);
    }
  }

  @Override
  @Disabled("Cassandra does not support altering column types")
  public void
      alterColumnType_AlterColumnTypeFromEachExistingDataTypeToText_ShouldAlterColumnTypesCorrectly() {}

  @Override
  @Disabled("Cassandra does not support altering column types")
  public void alterColumnType_WideningConversion_ShouldAlterColumnTypesCorrectly() {}

  @Override
  @Disabled("Cassandra does not support altering column types")
  public void alterColumnType_ForPrimaryKeyOrIndexKeyColumn_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("Cassandra does not support renaming tables")
  public void renameTable_ForExistingTable_ShouldRenameTableCorrectly() {}

  @Override
  @Disabled("Cassandra does not support renaming tables")
  public void renameTable_ForNonExistingTable_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("Cassandra does not support renaming tables")
  public void renameTable_IfNewTableNameAlreadyExists_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("Cassandra does not support renaming tables")
  public void renameTable_ForExistingTableWithIndexes_ShouldRenameTableAndIndexesCorrectly() {}
}
