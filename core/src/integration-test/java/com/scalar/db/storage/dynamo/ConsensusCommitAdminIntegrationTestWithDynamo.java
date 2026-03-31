package com.scalar.db.storage.dynamo;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.Insert;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminIntegrationTestBase;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class ConsensusCommitAdminIntegrationTestWithDynamo
    extends ConsensusCommitAdminIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    return ConsensusCommitDynamoEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return ConsensusCommitDynamoEnv.getCreationOptions();
  }

  @Override
  protected boolean isIndexOnBooleanColumnSupported() {
    return false;
  }

  @Override
  protected String getSystemNamespaceName(Properties properties) {
    return new DynamoConfig(new DatabaseConfig(properties))
        .getTableMetadataNamespace()
        .orElse(DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME);
  }

  @Override
  protected String getCoordinatorNamespaceName(String testName) {
    return new ConsensusCommitConfig(new DatabaseConfig(getProperties(testName)))
        .getCoordinatorNamespace()
        .orElse(Coordinator.NAMESPACE);
  }

  @Override
  protected boolean isGroupCommitEnabled(String testName) {
    return new ConsensusCommitConfig(new DatabaseConfig(getProperties(testName)))
        .isCoordinatorGroupCommitEnabled();
  }

  // Since DynamoDB doesn't have the namespace concept, some behaviors around the namespace are
  // different from the other adapters. So disable several tests that check such behaviors

  @Disabled
  @Test
  @Override
  public void createNamespace_ForNonExistingNamespace_ShouldCreateNamespaceProperly() {}

  @Disabled
  @Test
  @Override
  public void createNamespace_ForExistingNamespace_ShouldThrowIllegalArgumentException() {}

  @Disabled
  @Test
  @Override
  public void createNamespace_IfNotExists_ForExistingNamespace_ShouldNotThrowAnyException() {}

  @Disabled
  @Test
  @Override
  public void dropNamespace_ForNonExistingNamespace_ShouldDropNamespaceProperly() {}

  @Disabled
  @Test
  @Override
  public void dropNamespace_ForNonExistingNamespace_ShouldThrowIllegalArgumentException() {}

  @Disabled
  @Test
  @Override
  public void dropNamespace_ForNonEmptyNamespace_ShouldThrowIllegalArgumentException() {}

  @Disabled
  @Test
  @Override
  public void dropNamespace_IfExists_ForNonExistingNamespace_ShouldNotThrowAnyException() {}

  @Disabled
  @Test
  @Override
  public void namespaceExists_ShouldReturnCorrectResults() {}

  @Disabled
  @Test
  @Override
  public void createTable_ForNonExistingNamespace_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("Replaced by createIndexAndDropIndex test to avoid DynamoDB GSI per-table limit")
  public void createIndex_ForAllDataTypesWithExistingData_ShouldCreateIndexesCorrectly() {}

  @Override
  @Disabled("Replaced by createIndexAndDropIndex test to avoid DynamoDB GSI per-table limit")
  public void dropIndex_ForAllDataTypesWithExistingData_ShouldDropIndexCorrectly() {}

  // This test merges createIndex and dropIndex tests into one to avoid exceeding DynamoDB's
  // 20 GSI per-table limit. With before-image secondary indexes, each user index creates two
  // GSIs, so creating indexes on all columns at once would exceed the limit.
  @Test
  public void
      createIndexAndDropIndex_ForAllDataTypesWithExistingData_ShouldCreateAndDropIndexCorrectly()
          throws Exception {
    String table = "tbl_for_create_drop_idx";

    try {
      // Arrange
      Map<String, String> options = getCreationOptions();
      TableMetadata metadata =
          TableMetadata.newBuilder()
              .addColumn(COL_NAME1, DataType.INT)
              .addColumn(COL_NAME2, DataType.INT)
              .addColumn(COL_NAME3, DataType.TEXT)
              .addColumn(COL_NAME4, DataType.BIGINT)
              .addColumn(COL_NAME5, DataType.FLOAT)
              .addColumn(COL_NAME6, DataType.DOUBLE)
              .addColumn(COL_NAME7, DataType.BOOLEAN)
              .addColumn(COL_NAME8, DataType.BLOB)
              .addColumn(COL_NAME9, DataType.TEXT)
              .addColumn(COL_NAME10, DataType.DATE)
              .addColumn(COL_NAME11, DataType.TIME)
              .addColumn(COL_NAME12, DataType.TIMESTAMPTZ)
              .addColumn(COL_NAME13, DataType.TIMESTAMP)
              .addPartitionKey(COL_NAME1)
              .addSecondaryIndex(COL_NAME9)
              .build();
      admin.createTable(namespace1, table, metadata, options);

      transactionalInsert(
          Insert.newBuilder()
              .namespace(namespace1)
              .table(table)
              .partitionKey(Key.ofInt(COL_NAME1, 1))
              .intValue(COL_NAME2, 2)
              .textValue(COL_NAME3, "3")
              .bigIntValue(COL_NAME4, 4)
              .floatValue(COL_NAME5, 5)
              .doubleValue(COL_NAME6, 6)
              .booleanValue(COL_NAME7, true)
              .blobValue(COL_NAME8, "8".getBytes(StandardCharsets.UTF_8))
              .textValue(COL_NAME9, "9")
              .dateValue(COL_NAME10, LocalDate.of(2020, 6, 2))
              .timeValue(COL_NAME11, LocalTime.of(12, 2, 6, 123_456_000))
              .timestampTZValue(
                  COL_NAME12,
                  LocalDateTime.of(LocalDate.of(2020, 6, 2), LocalTime.of(12, 2, 6, 123_000_000))
                      .toInstant(ZoneOffset.UTC))
              .timestampValue(
                  COL_NAME13,
                  LocalDateTime.of(LocalDate.of(2020, 6, 2), LocalTime.of(12, 2, 6, 123_000_000)))
              .build());

      // Act Assert: create and drop each index one by one
      createIndexAssertAndDrop(namespace1, table, COL_NAME2, options);
      createIndexAssertAndDrop(namespace1, table, COL_NAME3, options);
      createIndexAssertAndDrop(namespace1, table, COL_NAME4, options);
      createIndexAssertAndDrop(namespace1, table, COL_NAME5, options);
      createIndexAssertAndDrop(namespace1, table, COL_NAME6, options);
      // DynamoDB does not support index on BOOLEAN column
      createIndexAssertAndDrop(namespace1, table, COL_NAME10, options);
      createIndexAssertAndDrop(namespace1, table, COL_NAME11, options);
      createIndexAssertAndDrop(namespace1, table, COL_NAME12, options);
      createIndexAssertAndDrop(namespace1, table, COL_NAME13, options);

      // Verify COL_NAME9 index still exists
      Set<String> finalIndexNames =
          admin.getTableMetadata(namespace1, table).getSecondaryIndexNames();
      assertThat(finalIndexNames).containsOnly(COL_NAME9);
    } finally {
      admin.dropTable(namespace1, table, true);
    }
  }

  private void createIndexAssertAndDrop(
      String namespace, String table, String column, Map<String, String> options) throws Exception {
    admin.createIndex(namespace, table, column, options);
    assertThat(admin.indexExists(namespace, table, column)).isTrue();
    assertThat(admin.getTableMetadata(namespace, table).getSecondaryIndexNames()).contains(column);
    admin.dropIndex(namespace, table, column);
    assertThat(admin.indexExists(namespace, table, column)).isFalse();
  }
}
