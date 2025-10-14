package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Insert;
import com.scalar.db.api.InsertBuilder;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminIntegrationTestBase;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
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
  protected String getSystemNamespaceName(Properties properties) {
    return new JdbcConfig(new DatabaseConfig(properties))
        .getTableMetadataSchema()
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

  // Since SQLite doesn't have persistent namespaces, some behaviors around the namespace are
  // different from the other adapters. So disable several tests that check such behaviors.

  @SuppressWarnings("unused")
  private boolean isSqlite() {
    return JdbcEnv.isSqlite();
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
  public void createNamespace_ForExistingNamespace_ShouldThrowIllegalArgumentException() {
    super.createNamespace_ForExistingNamespace_ShouldThrowIllegalArgumentException();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void createNamespace_IfNotExists_ForExistingNamespace_ShouldNotThrowAnyException() {
    super.createNamespace_IfNotExists_ForExistingNamespace_ShouldNotThrowAnyException();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void dropNamespace_ForNonExistingNamespace_ShouldDropNamespaceProperly()
      throws ExecutionException {
    super.dropNamespace_ForNonExistingNamespace_ShouldDropNamespaceProperly();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void dropNamespace_ForNonExistingNamespace_ShouldThrowIllegalArgumentException() {
    super.dropNamespace_ForNonExistingNamespace_ShouldThrowIllegalArgumentException();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void dropNamespace_ForNonEmptyNamespace_ShouldThrowIllegalArgumentException()
      throws ExecutionException {
    super.dropNamespace_ForNonEmptyNamespace_ShouldThrowIllegalArgumentException();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void dropNamespace_IfExists_ForNonExistingNamespace_ShouldNotThrowAnyException() {
    super.dropNamespace_IfExists_ForNonExistingNamespace_ShouldNotThrowAnyException();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void namespaceExists_ShouldReturnCorrectResults() throws ExecutionException {
    super.namespaceExists_ShouldReturnCorrectResults();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void createTable_ForNonExistingNamespace_ShouldThrowIllegalArgumentException() {
    super.createTable_ForNonExistingNamespace_ShouldThrowIllegalArgumentException();
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
  private boolean isOracle() {
    return JdbcEnv.isOracle();
  }

  @SuppressWarnings("unused")
  private boolean isDb2() {
    return JdbcEnv.isDb2();
  }

  @SuppressWarnings("unused")
  private boolean isColumnTypeConversionToTextNotFullySupported() {
    return JdbcTestUtils.isDb2(rdbEngine)
        || JdbcTestUtils.isOracle(rdbEngine)
        || JdbcTestUtils.isSqlite(rdbEngine);
  }

  @SuppressWarnings("unused")
  private boolean isWideningColumnTypeConversionNotFullySupported() {
    return JdbcTestUtils.isOracle(rdbEngine) || JdbcTestUtils.isSqlite(rdbEngine);
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

  @Test
  @Override
  @DisabledIf("isColumnTypeConversionToTextNotFullySupported")
  public void
      alterColumnType_AlterColumnTypeFromEachExistingDataTypeToText_ShouldAlterColumnTypesCorrectly()
          throws ExecutionException, IOException, TransactionException {
    super
        .alterColumnType_AlterColumnTypeFromEachExistingDataTypeToText_ShouldAlterColumnTypesCorrectly();
  }

  @Test
  @EnabledIf("isOracle")
  public void
      alterColumnType_Oracle_AlterColumnTypeFromEachExistingDataTypeToText_ShouldThrowUnsupportedOperationException()
          throws ExecutionException, TransactionException {
    try (DistributedTransactionManager transactionManager =
        transactionFactory.getTransactionManager()) {
      // Arrange
      Map<String, String> options = getCreationOptions();
      TableMetadata.Builder currentTableMetadataBuilder =
          TableMetadata.newBuilder()
              .addColumn("c1", DataType.INT)
              .addColumn("c2", DataType.INT)
              .addColumn("c3", DataType.INT)
              .addColumn("c4", DataType.BIGINT)
              .addColumn("c5", DataType.FLOAT)
              .addColumn("c6", DataType.DOUBLE)
              .addColumn("c7", DataType.TEXT)
              .addColumn("c8", DataType.BLOB)
              .addColumn("c9", DataType.DATE)
              .addColumn("c10", DataType.TIME)
              .addPartitionKey("c1")
              .addClusteringKey("c2", Scan.Ordering.Order.ASC);
      if (isTimestampTypeSupported()) {
        currentTableMetadataBuilder
            .addColumn("c11", DataType.TIMESTAMP)
            .addColumn("c12", DataType.TIMESTAMPTZ);
      }
      TableMetadata currentTableMetadata = currentTableMetadataBuilder.build();
      admin.createTable(namespace1, TABLE4, currentTableMetadata, options);
      InsertBuilder.Buildable insert =
          Insert.newBuilder()
              .namespace(namespace1)
              .table(TABLE4)
              .partitionKey(Key.ofInt("c1", 1))
              .clusteringKey(Key.ofInt("c2", 2))
              .intValue("c3", 1)
              .bigIntValue("c4", 2L)
              .floatValue("c5", 3.0f)
              .doubleValue("c6", 4.0d)
              .textValue("c7", "5")
              .blobValue("c8", "6".getBytes(StandardCharsets.UTF_8))
              .dateValue("c9", LocalDate.now(ZoneId.of("UTC")))
              .timeValue("c10", LocalTime.now(ZoneId.of("UTC")));
      if (isTimestampTypeSupported()) {
        insert.timestampValue("c11", LocalDateTime.now(ZoneOffset.UTC));
        insert.timestampTZValue("c12", Instant.now());
      }
      transactionalInsert(transactionManager, insert.build());

      // Act Assert
      assertThatCode(() -> admin.alterColumnType(namespace1, TABLE4, "c3", DataType.TEXT))
          .isInstanceOf(UnsupportedOperationException.class);
      assertThatCode(() -> admin.alterColumnType(namespace1, TABLE4, "c4", DataType.TEXT))
          .isInstanceOf(UnsupportedOperationException.class);
      assertThatCode(() -> admin.alterColumnType(namespace1, TABLE4, "c5", DataType.TEXT))
          .isInstanceOf(UnsupportedOperationException.class);
      assertThatCode(() -> admin.alterColumnType(namespace1, TABLE4, "c6", DataType.TEXT))
          .isInstanceOf(UnsupportedOperationException.class);
      assertThatCode(() -> admin.alterColumnType(namespace1, TABLE4, "c7", DataType.TEXT))
          .doesNotThrowAnyException();
      assertThatCode(() -> admin.alterColumnType(namespace1, TABLE4, "c8", DataType.TEXT))
          .isInstanceOf(UnsupportedOperationException.class);
      assertThatCode(() -> admin.alterColumnType(namespace1, TABLE4, "c9", DataType.TEXT))
          .isInstanceOf(UnsupportedOperationException.class);
      assertThatCode(() -> admin.alterColumnType(namespace1, TABLE4, "c10", DataType.TEXT))
          .isInstanceOf(UnsupportedOperationException.class);
      if (isTimestampTypeSupported()) {
        assertThatCode(() -> admin.alterColumnType(namespace1, TABLE4, "c11", DataType.TEXT))
            .isInstanceOf(UnsupportedOperationException.class);
        assertThatCode(() -> admin.alterColumnType(namespace1, TABLE4, "c12", DataType.TEXT))
            .isInstanceOf(UnsupportedOperationException.class);
      }
    } finally {
      admin.dropTable(namespace1, TABLE4, true);
    }
  }

  @Test
  @EnabledIf("isDb2")
  public void
      alterColumnType_Db2_AlterColumnTypeFromEachExistingDataTypeToText_ShouldAlterColumnTypesCorrectlyIfSupported()
          throws ExecutionException, TransactionException {
    try (DistributedTransactionManager transactionManager =
        transactionFactory.getTransactionManager()) {
      // Arrange
      Map<String, String> options = getCreationOptions();
      TableMetadata.Builder currentTableMetadataBuilder =
          TableMetadata.newBuilder()
              .addColumn("c1", DataType.INT)
              .addColumn("c2", DataType.INT)
              .addColumn("c3", DataType.INT)
              .addColumn("c4", DataType.BIGINT)
              .addColumn("c5", DataType.FLOAT)
              .addColumn("c6", DataType.DOUBLE)
              .addColumn("c7", DataType.TEXT)
              .addColumn("c8", DataType.BLOB)
              .addColumn("c9", DataType.DATE)
              .addColumn("c10", DataType.TIME)
              .addPartitionKey("c1")
              .addClusteringKey("c2", Scan.Ordering.Order.ASC);
      if (isTimestampTypeSupported()) {
        currentTableMetadataBuilder
            .addColumn("c11", DataType.TIMESTAMP)
            .addColumn("c12", DataType.TIMESTAMPTZ);
      }
      TableMetadata currentTableMetadata = currentTableMetadataBuilder.build();
      admin.createTable(namespace1, TABLE4, currentTableMetadata, options);
      InsertBuilder.Buildable insert =
          Insert.newBuilder()
              .namespace(namespace1)
              .table(TABLE4)
              .partitionKey(Key.ofInt("c1", 1))
              .clusteringKey(Key.ofInt("c2", 2))
              .intValue("c3", 1)
              .bigIntValue("c4", 2L)
              .floatValue("c5", 3.0f)
              .doubleValue("c6", 4.0d)
              .textValue("c7", "5")
              .blobValue("c8", "6".getBytes(StandardCharsets.UTF_8))
              .dateValue("c9", LocalDate.now(ZoneId.of("UTC")))
              .timeValue("c10", LocalTime.now(ZoneId.of("UTC")));
      if (isTimestampTypeSupported()) {
        insert.timestampValue("c11", LocalDateTime.now(ZoneOffset.UTC));
        insert.timestampTZValue("c12", Instant.now());
      }
      transactionalInsert(transactionManager, insert.build());

      // Act Assert
      assertThatCode(() -> admin.alterColumnType(namespace1, TABLE4, "c3", DataType.TEXT))
          .doesNotThrowAnyException();
      assertThatCode(() -> admin.alterColumnType(namespace1, TABLE4, "c4", DataType.TEXT))
          .doesNotThrowAnyException();
      assertThatCode(() -> admin.alterColumnType(namespace1, TABLE4, "c5", DataType.TEXT))
          .doesNotThrowAnyException();
      assertThatCode(() -> admin.alterColumnType(namespace1, TABLE4, "c6", DataType.TEXT))
          .doesNotThrowAnyException();
      assertThatCode(() -> admin.alterColumnType(namespace1, TABLE4, "c7", DataType.TEXT))
          .doesNotThrowAnyException();
      assertThatCode(() -> admin.alterColumnType(namespace1, TABLE4, "c8", DataType.TEXT))
          .isInstanceOf(UnsupportedOperationException.class);
      assertThatCode(() -> admin.alterColumnType(namespace1, TABLE4, "c9", DataType.TEXT))
          .doesNotThrowAnyException();
      assertThatCode(() -> admin.alterColumnType(namespace1, TABLE4, "c10", DataType.TEXT))
          .doesNotThrowAnyException();
      if (isTimestampTypeSupported()) {
        assertThatCode(() -> admin.alterColumnType(namespace1, TABLE4, "c11", DataType.TEXT))
            .doesNotThrowAnyException();
        assertThatCode(() -> admin.alterColumnType(namespace1, TABLE4, "c12", DataType.TEXT))
            .doesNotThrowAnyException();
      }

      TableMetadata.Builder expectedTableMetadataBuilder =
          TableMetadata.newBuilder()
              .addColumn("c1", DataType.INT)
              .addColumn("c2", DataType.INT)
              .addColumn("c3", DataType.TEXT)
              .addColumn("c4", DataType.TEXT)
              .addColumn("c5", DataType.TEXT)
              .addColumn("c6", DataType.TEXT)
              .addColumn("c7", DataType.TEXT)
              .addColumn("c8", DataType.BLOB)
              .addColumn("c9", DataType.TEXT)
              .addColumn("c10", DataType.TEXT)
              .addPartitionKey("c1")
              .addClusteringKey("c2", Scan.Ordering.Order.ASC);
      if (isTimestampTypeSupported()) {
        expectedTableMetadataBuilder
            .addColumn("c11", DataType.TEXT)
            .addColumn("c12", DataType.TEXT);
      }
      TableMetadata expectedTableMetadata = expectedTableMetadataBuilder.build();
      assertThat(admin.getTableMetadata(namespace1, TABLE4)).isEqualTo(expectedTableMetadata);
    } finally {
      admin.dropTable(namespace1, TABLE4, true);
    }
  }

  @Test
  @Override
  @DisabledIf("isWideningColumnTypeConversionNotFullySupported")
  public void alterColumnType_WideningConversion_ShouldAlterColumnTypesCorrectly()
      throws ExecutionException, IOException, TransactionException {
    super.alterColumnType_WideningConversion_ShouldAlterColumnTypesCorrectly();
  }

  @Test
  @EnabledIf("isOracle")
  public void alterColumnType_Oracle_WideningConversion_ShouldAlterColumnTypesCorrectly()
      throws ExecutionException, TransactionException {
    try (DistributedTransactionManager manager = transactionFactory.getTransactionManager()) {
      // Arrange
      Map<String, String> options = getCreationOptions();
      TableMetadata.Builder currentTableMetadataBuilder =
          TableMetadata.newBuilder()
              .addColumn("c1", DataType.INT)
              .addColumn("c2", DataType.INT)
              .addColumn("c3", DataType.INT)
              .addColumn("c4", DataType.FLOAT)
              .addPartitionKey("c1")
              .addClusteringKey("c2", Scan.Ordering.Order.ASC);
      TableMetadata currentTableMetadata = currentTableMetadataBuilder.build();
      admin.createTable(namespace1, TABLE4, currentTableMetadata, options);

      int expectedColumn3Value = 1;
      float expectedColumn4Value = 4.0f;
      InsertBuilder.Buildable insert =
          Insert.newBuilder()
              .namespace(namespace1)
              .table(TABLE4)
              .partitionKey(Key.ofInt("c1", 1))
              .clusteringKey(Key.ofInt("c2", 2))
              .intValue("c3", expectedColumn3Value)
              .floatValue("c4", expectedColumn4Value);
      transactionalInsert(manager, insert.build());

      // Act
      admin.alterColumnType(namespace1, TABLE4, "c3", DataType.BIGINT);
      Throwable exception =
          catchThrowable(() -> admin.alterColumnType(namespace1, TABLE4, "c4", DataType.DOUBLE));

      // Wait for cache expiry
      Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

      // Assert
      assertThat(exception).isInstanceOf(UnsupportedOperationException.class);
      TableMetadata.Builder expectedTableMetadataBuilder =
          TableMetadata.newBuilder()
              .addColumn("c1", DataType.INT)
              .addColumn("c2", DataType.INT)
              .addColumn("c3", DataType.BIGINT)
              .addColumn("c4", DataType.FLOAT)
              .addPartitionKey("c1")
              .addClusteringKey("c2", Scan.Ordering.Order.ASC);
      TableMetadata expectedTableMetadata = expectedTableMetadataBuilder.build();
      assertThat(admin.getTableMetadata(namespace1, TABLE4)).isEqualTo(expectedTableMetadata);

      Scan scan =
          Scan.newBuilder()
              .namespace(namespace1)
              .table(TABLE4)
              .partitionKey(Key.ofInt("c1", 1))
              .build();
      List<Result> results = transactionalScan(manager, scan);
      assertThat(results).hasSize(1);
      Result result = results.get(0);
      assertThat(result.getBigInt("c3")).isEqualTo(expectedColumn3Value);
    } finally {
      admin.dropTable(namespace1, TABLE4, true);
    }
  }

  @Test
  @EnabledIf("isSqlite")
  public void alterColumnType_Sqlite_AlterColumnType_ShouldThrowUnsupportedOperationException()
      throws ExecutionException {
    try {
      // Arrange
      Map<String, String> options = getCreationOptions();
      TableMetadata.Builder currentTableMetadataBuilder =
          TableMetadata.newBuilder()
              .addColumn("c1", DataType.INT)
              .addColumn("c2", DataType.INT)
              .addColumn("c3", DataType.INT)
              .addPartitionKey("c1")
              .addClusteringKey("c2", Scan.Ordering.Order.ASC);
      TableMetadata currentTableMetadata = currentTableMetadataBuilder.build();
      admin.createTable(namespace1, TABLE4, currentTableMetadata, options);

      // Act Assert
      assertThatCode(() -> admin.alterColumnType(namespace1, TABLE4, "c3", DataType.TEXT))
          .isInstanceOf(UnsupportedOperationException.class);
    } finally {
      admin.dropTable(namespace1, TABLE4, true);
    }
  }

  @Override
  protected boolean isIndexOnBlobColumnSupported() {
    return !JdbcTestUtils.isDb2(rdbEngine);
  }
}
