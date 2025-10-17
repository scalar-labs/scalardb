package com.scalar.db.storage.cassandra;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;

public class ConsensusCommitAdminIntegrationTestWithCassandra
    extends ConsensusCommitAdminIntegrationTestBase {
  @Override
  protected Properties getProps(String testName) {
    return CassandraEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return Collections.singletonMap(CassandraAdmin.REPLICATION_FACTOR, "1");
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new CassandraAdminTestUtils(getProperties(testName));
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
              .addColumn("c1", DataType.INT)
              .addColumn("c2", DataType.INT)
              .addColumn("c3", DataType.TEXT)
              .addPartitionKey("c1")
              .addClusteringKey("c2")
              .addSecondaryIndex("c1")
              .build();
      admin.createTable(namespace1, TABLE4, currentTableMetadata, options);

      // Act
      admin.renameColumn(namespace1, TABLE4, "c1", "c4");

      // Assert
      TableMetadata expectedTableMetadata =
          TableMetadata.newBuilder()
              .addColumn("c4", DataType.INT)
              .addColumn("c2", DataType.INT)
              .addColumn("c3", DataType.TEXT)
              .addPartitionKey("c4")
              .addClusteringKey("c2")
              .addSecondaryIndex("c4")
              .build();
      assertThat(admin.getTableMetadata(namespace1, TABLE4)).isEqualTo(expectedTableMetadata);
      assertThat(admin.indexExists(namespace1, TABLE4, "c1")).isFalse();
      assertThat(admin.indexExists(namespace1, TABLE4, "c4")).isTrue();
    } finally {
      admin.dropTable(namespace1, TABLE4, true);
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

  @Override
  @Disabled("Cassandra does not support renaming tables")
  public void renameTable_IfOnlyOneTableExists_ShouldRenameTableCorrectly() {}
}
